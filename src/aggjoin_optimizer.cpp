/*
 * AggJoin Optimizer Extension v5
 *
 * Post-optimize approach: walks through Projection chain between
 * Aggregate and Join to remap column indices. Two-child pipeline:
 * build side sinks, probe side feeds ExecuteInternal.
 */

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/function/scalar_function.hpp"

#if __has_include("duckdb/main/extension_callback_manager.hpp")
#include "duckdb/main/extension_callback_manager.hpp"
#define HAS_CALLBACK_MANAGER 1
#else
#include "duckdb/main/config.hpp"
#define HAS_CALLBACK_MANAGER 0
#endif

// v1.5.1 uses GetDataInternal (virtual in CachingPhysicalOperator);
// v1.4.3 uses GetData (virtual in PhysicalOperator base).
// Detect via presence of the callback manager (v1.5.1 feature).
#if HAS_CALLBACK_MANAGER
#define AGGJOIN_GETDATA GetDataInternal
#else
#define AGGJOIN_GETDATA GetData
#endif

#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstdlib>
#include <atomic>
#include <cstring>

namespace duckdb {

static bool AggJoinTraceEnabled() {
    static int enabled = []() {
        auto *env = std::getenv("AGGJOIN_TRACE");
        if (!env || !env[0]) return 0;
        if (env[0] == '0') return 0;
        return 1;
    }();
    return enabled != 0;
}

static bool AggJoinTraceStatsEnabled() {
    static int enabled = []() {
        auto *env = std::getenv("AGGJOIN_TRACE_STATS");
        if (!env || !env[0]) return 0;
        if (env[0] == '0') return 0;
        return 1;
    }();
    return enabled != 0;
}

static bool AggJoinStaticDisabledByEnv() {
    static int disabled = []() {
        auto *env = std::getenv("AGGJOIN_DISABLE_STATIC");
        if (!env || !env[0]) return 0;
        if (env[0] == '0') return 0;
        return 1;
    }();
    return disabled != 0;
}

static std::atomic<int64_t> aggjoin_test_hash_bits {-1};

int64_t GetAggJoinTestHashBits() {
    return aggjoin_test_hash_bits.load();
}

void SetAggJoinTestHashBits(int64_t bits) {
    aggjoin_test_hash_bits.store(bits);
}

static inline hash_t ApplyAggJoinTestHashBits(hash_t h) {
    auto bits = aggjoin_test_hash_bits.load();
    if (bits < 0 || bits >= 64) return h;
    if (bits == 0) return 0;
    return h & ((((hash_t)1) << bits) - 1);
}

static inline void ApplyAggJoinTestHashBits(hash_t *hashes, idx_t count) {
    auto bits = aggjoin_test_hash_bits.load();
    if (bits < 0 || bits >= 64) return;
    hash_t mask = bits == 0 ? 0 : ((((hash_t)1) << bits) - 1);
    for (idx_t i = 0; i < count; i++) hashes[i] &= mask;
}

struct AggJoinOptimizerInfo : public OptimizerExtensionInfo {
    explicit AggJoinOptimizerInfo(bool ignore_disable_static_p) : ignore_disable_static(ignore_disable_static_p) {
    }
    bool ignore_disable_static;
};

struct AggJoinRewriteState {
    vector<ReplacementBinding> replacement_bindings;
};

// ============================================================
// Open-addressing hash table
// ============================================================

struct ResultEntry {
    hash_t key = 0;
    vector<Value> group_vals;
    vector<double> sum_vals;
    vector<Value> min_vals, max_vals;
    vector<bool> has_value;
    bool occupied = false;
};

struct BuildAggValues {
    vector<double> agg_sum;
    vector<double> agg_min;
    vector<double> agg_max;
    vector<double> agg_count;
    vector<uint8_t> agg_init;

    void EnsureSize(idx_t n) {
        if (agg_sum.size() >= n) return;
        auto old_size = agg_sum.size();
        agg_sum.resize(n, 0.0);
        agg_min.resize(n, std::numeric_limits<double>::max());
        agg_max.resize(n, std::numeric_limits<double>::lowest());
        agg_count.resize(n, 0.0);
        agg_init.resize(n, 0);
        for (idx_t i = old_size; i < n; i++) {
            agg_min[i] = std::numeric_limits<double>::max();
            agg_max[i] = std::numeric_limits<double>::lowest();
        }
    }
};

struct BuildEntry {
    hash_t key = 0;
    idx_t count = 0;
    bool occupied = false;
    int64_t int_key = 0;  // Actual integer key value (for direct mode)
    double dbl_key = 0.0; // Actual floating key value (for non-integral same-key hash mode)
    bool uses_string_key = false;
    string str_key;       // Exact single-key VARCHAR payload for narrow varlen fast path
    bool uses_value_keys = false;
    vector<Value> key_vals; // Exact key payloads for collision checks on composite/unsupported keys
    // Inlined build-side aggregate values (eliminates separate build_agg_map lookup)
    BuildAggValues bav;
};

template <typename E>
struct OpenHT {
    vector<E> buckets;
    idx_t mask = 0;
    idx_t count = 0;
    void Init(idx_t n) { idx_t c=16; while(c<n*2)c<<=1; mask=c-1; buckets.resize(c); count=0; }
    void Grow() {
        // Double capacity and rehash
        auto old = std::move(buckets);
        idx_t new_cap = (mask + 1) * 2;
        mask = new_cap - 1;
        buckets.resize(new_cap);
        count = 0;
        for (auto &b : old) {
            if (b.occupied) {
                idx_t p = b.key & mask;
                while (buckets[p].occupied) {
                    p = (p + 1) & mask;
                }
                buckets[p] = std::move(b);
                count++;
            }
        }
    }
    E &FindOrCreate(hash_t k) {
        if(!mask)Init(1024); idx_t p=k&mask;
        while(true){auto&b=buckets[p];if(!b.occupied){b.key=k;b.occupied=true;count++;
            if(count > (mask+1)*3/4) { /* defer grow to after return */ }
            return b;}if(b.key==k)return b;p=(p+1)&mask;}
    }
    // Insert with auto-grow when load factor > 75%
    E &Insert(hash_t k) {
        if (!mask) Init(1024);
        if (count >= (mask + 1) * 3 / 4) Grow();
        return FindOrCreate(k);
    }
    E *Find(hash_t k) {
        if(__builtin_expect(!mask, 0)) return nullptr;
        idx_t p = k & mask;
        // Hot path: first probe hits (no collision). Unrolled first check.
        auto &first = buckets[p];
        if (__builtin_expect(first.key == k, 1)) return &first;
        if (__builtin_expect(!first.occupied, 0)) return nullptr;
        // Collision: linear probe
        p = (p + 1) & mask;
        while (true) {
            auto &b = buckets[p];
            if (!b.occupied) return nullptr;
            if (b.key == k) return &b;
            p = (p + 1) & mask;
        }
    }

    template <typename MatchFn>
    E *Find(hash_t k, MatchFn &&match) {
        if (__builtin_expect(!mask, 0)) return nullptr;
        idx_t p = k & mask;
        while (true) {
            auto &b = buckets[p];
            if (!b.occupied) return nullptr;
            if (b.key == k && match(b)) return &b;
            p = (p + 1) & mask;
        }
    }

    template <typename MatchFn, typename InitFn>
    E &Insert(hash_t k, MatchFn &&match, InitFn &&init) {
        if (!mask) Init(1024);
        if (count >= (mask + 1) * 3 / 4) Grow();
        idx_t p = k & mask;
        while (true) {
            auto &b = buckets[p];
            if (!b.occupied) {
                b.key = k;
                b.occupied = true;
                init(b);
                count++;
                return b;
            }
            if (b.key == k && match(b)) return b;
            p = (p + 1) & mask;
        }
    }

    // Prefetch the bucket for a hash (call before Find for next batch)
    void Prefetch(hash_t k) {
        if(mask) __builtin_prefetch(&buckets[k & mask], 0, 1);
    }
    template<typename F> void ForEach(F&&fn){for(auto&b:buckets)if(b.occupied)fn(b);}
};

// ============================================================
// Column info (resolved during optimizer, not CreatePlan)
// ============================================================

struct CompressInfo {
    bool has_compress = false;       // Whether this column has a compress Projection
    bool is_string_compress = false; // String compress/decompress pair stripped above AggJoin
    int64_t offset = 0;             // The min_val offset used by compress(value, min_val)
    LogicalType compressed_type;     // The output type after compression (e.g. UINT8)
    LogicalType original_type;       // The input type before compression (e.g. INT32)
};

struct AggJoinColInfo {
    // Remapped to probe/build scan column indices (after Projection tracing)
    vector<idx_t> probe_key_cols;   // Join key cols in probe scan
    vector<idx_t> build_key_cols;   // Join key cols in build scan
    vector<idx_t> group_cols;       // GROUP BY cols as join-output indices
    vector<idx_t> agg_input_cols;   // Aggregate input as join-output indices
    vector<string> agg_funcs;
    vector<bool> agg_on_build;       // True if aggregate input is on build side
    vector<bool> agg_is_numeric;     // True if aggregate input is numeric (double path), false for VARCHAR etc.
    vector<idx_t> build_agg_cols;    // Build-side column indices for build-side aggs (parallel to agg_funcs)
    idx_t probe_col_count = 0;      // Columns from probe in join output
    idx_t probe_estimate = 0;
    idx_t build_estimate = 0;
    idx_t group_estimate = 0;
    // Compression info per group column (parallel to group_cols)
    // If the Projection chain applies compress() to a group column,
    // we need to output compressed values so the decompress Projection above works.
    vector<CompressInfo> group_compress;
};

static void InitBuildEntryKeys(BuildEntry &entry, DataChunk &chunk, const vector<idx_t> &key_cols, idx_t r) {
    entry.uses_string_key = false;
    entry.str_key.clear();
    entry.uses_value_keys = false;
    entry.key_vals.clear();
    if (key_cols.size() != 1) {
        entry.uses_value_keys = true;
        entry.key_vals.reserve(key_cols.size());
        for (auto ki : key_cols) {
            entry.key_vals.push_back(chunk.data[ki].GetValue(r));
        }
        return;
    }
    auto ki = key_cols[0];
    auto ptype = chunk.data[ki].GetType().InternalType();
    switch (ptype) {
    case PhysicalType::INT32:  entry.int_key = FlatVector::GetData<int32_t>(chunk.data[ki])[r]; break;
    case PhysicalType::INT64:  entry.int_key = FlatVector::GetData<int64_t>(chunk.data[ki])[r]; break;
    case PhysicalType::INT16:  entry.int_key = FlatVector::GetData<int16_t>(chunk.data[ki])[r]; break;
    case PhysicalType::INT8:   entry.int_key = FlatVector::GetData<int8_t>(chunk.data[ki])[r]; break;
    case PhysicalType::UINT32: entry.int_key = FlatVector::GetData<uint32_t>(chunk.data[ki])[r]; break;
    case PhysicalType::UINT16: entry.int_key = FlatVector::GetData<uint16_t>(chunk.data[ki])[r]; break;
    case PhysicalType::UINT8:  entry.int_key = FlatVector::GetData<uint8_t>(chunk.data[ki])[r]; break;
    case PhysicalType::DOUBLE: entry.dbl_key = FlatVector::GetData<double>(chunk.data[ki])[r]; entry.int_key = INT64_MIN; break;
    case PhysicalType::FLOAT:  entry.dbl_key = FlatVector::GetData<float>(chunk.data[ki])[r]; entry.int_key = INT64_MIN; break;
    case PhysicalType::VARCHAR: {
        auto str = FlatVector::GetData<string_t>(chunk.data[ki])[r];
        entry.uses_string_key = true;
        entry.str_key.assign(str.GetData(), str.GetSize());
        entry.int_key = INT64_MIN;
        break;
    }
    default:
        entry.uses_value_keys = true;
        entry.key_vals.push_back(chunk.data[ki].GetValue(r));
        entry.int_key = INT64_MIN;
        break;
    }
}

static bool BuildEntryMatches(const BuildEntry &entry, DataChunk &chunk, const vector<idx_t> &key_cols, idx_t r) {
    if (entry.uses_string_key) {
        if (key_cols.size() != 1) return false;
        auto ki = key_cols[0];
        if (chunk.data[ki].GetType().InternalType() != PhysicalType::VARCHAR) return false;
        auto str = FlatVector::GetData<string_t>(chunk.data[ki])[r];
        return str.GetSize() == entry.str_key.size() &&
               std::memcmp(str.GetData(), entry.str_key.data(), str.GetSize()) == 0;
    }
    if (entry.uses_value_keys) {
        if (entry.key_vals.size() != key_cols.size()) return false;
        for (idx_t i = 0; i < key_cols.size(); i++) {
            if (!Value::NotDistinctFrom(entry.key_vals[i], chunk.data[key_cols[i]].GetValue(r))) return false;
        }
        return true;
    }
    if (key_cols.size() != 1) return false;
    auto ki = key_cols[0];
    auto ptype = chunk.data[ki].GetType().InternalType();
    switch (ptype) {
    case PhysicalType::INT32:  return entry.int_key == FlatVector::GetData<int32_t>(chunk.data[ki])[r];
    case PhysicalType::INT64:  return entry.int_key == FlatVector::GetData<int64_t>(chunk.data[ki])[r];
    case PhysicalType::INT16:  return entry.int_key == FlatVector::GetData<int16_t>(chunk.data[ki])[r];
    case PhysicalType::INT8:   return entry.int_key == FlatVector::GetData<int8_t>(chunk.data[ki])[r];
    case PhysicalType::UINT32: return entry.int_key == (int64_t)FlatVector::GetData<uint32_t>(chunk.data[ki])[r];
    case PhysicalType::UINT16: return entry.int_key == (int64_t)FlatVector::GetData<uint16_t>(chunk.data[ki])[r];
    case PhysicalType::UINT8:  return entry.int_key == (int64_t)FlatVector::GetData<uint8_t>(chunk.data[ki])[r];
    case PhysicalType::DOUBLE: return entry.dbl_key == FlatVector::GetData<double>(chunk.data[ki])[r];
    case PhysicalType::FLOAT:  return entry.dbl_key == FlatVector::GetData<float>(chunk.data[ki])[r];
    default:
        return false;
    }
}

static bool NeedsValueGroupStorage(PhysicalType ptype) {
    switch (ptype) {
    case PhysicalType::INT8:
    case PhysicalType::INT16:
    case PhysicalType::INT32:
    case PhysicalType::INT64:
    case PhysicalType::UINT8:
    case PhysicalType::UINT16:
    case PhysicalType::UINT32:
    case PhysicalType::FLOAT:
    case PhysicalType::DOUBLE:
        return false;
    default:
        return true;
    }
}

struct FlatResultHT;
static bool ResultSlotGroupMatches(FlatResultHT &rht, idx_t slot, DataChunk &input, const AggJoinColInfo &col, idx_t r);

// ============================================================
// Extract compress info from a Projection expression
// ============================================================

// Check if a Projection expression at `idx` is a compress function.
// If so, extract the offset (second constant argument) and compressed type.
static CompressInfo ExtractCompressInfo(LogicalOperator &op, idx_t idx) {
    CompressInfo info;
    if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) return info;
    auto &proj = op.Cast<LogicalProjection>();
    if (idx >= proj.expressions.size()) return info;
    auto &expr = proj.expressions[idx];
    if (expr->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) return info;
    auto &func = expr->Cast<BoundFunctionExpression>();
    if (func.function.name.find("__internal_compress_integral") == string::npos) {
        if (func.function.name.find("compress_string") == string::npos) return info;
        info.has_compress = true;
        info.is_string_compress = true;
        info.compressed_type = func.return_type;
        if (!func.children.empty()) info.original_type = func.children[0]->return_type;
        return info;
    }
    // It's a compress function. Extract offset from second child (constant).
    if (func.children.size() < 2) return info;
    if (func.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) return info;
    auto &constant = func.children[1]->Cast<BoundConstantExpression>();
    // The offset is the min_val constant — extract as int64
    info.has_compress = true;
    info.compressed_type = func.return_type;
    info.original_type = func.children[0]->return_type;
    // Extract offset value as int64
    auto &val = constant.value;
    switch (val.type().InternalType()) {
    case PhysicalType::INT16: info.offset = val.GetValue<int16_t>(); break;
    case PhysicalType::INT32: info.offset = val.GetValue<int32_t>(); break;
    case PhysicalType::INT64: info.offset = val.GetValue<int64_t>(); break;
    case PhysicalType::INT8:  info.offset = val.GetValue<int8_t>(); break;
    case PhysicalType::UINT8: info.offset = val.GetValue<uint8_t>(); break;
    case PhysicalType::UINT16: info.offset = val.GetValue<uint16_t>(); break;
    case PhysicalType::UINT32: info.offset = val.GetValue<uint32_t>(); break;
    case PhysicalType::UINT64: info.offset = (int64_t)val.GetValue<uint64_t>(); break;
    default: info.has_compress = false; break; // Can't extract offset
    }
    return info;
}

// Walk the Projection chain from agg_child down to find compress info
// for a column at position `agg_idx` in the agg_child's output.
static CompressInfo FindCompressInChain(LogicalOperator &op, idx_t idx) {
    if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
        auto &proj = op.Cast<LogicalProjection>();
        if (idx < proj.expressions.size()) {
            // Check if THIS Projection applies compress at this index
            auto ci = ExtractCompressInfo(op, idx);
            if (ci.has_compress) return ci;
            // Not a compress — trace through to child and check deeper
            auto &expr = proj.expressions[idx];
            if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
                return FindCompressInChain(*op.children[0],
                    expr->Cast<BoundReferenceExpression>().index);
            }
        }
    }
    return CompressInfo{}; // No compress found
}

// ============================================================
// Projection chain tracer: remap aggregate index → join output index
// ============================================================

// Given an index into a Projection's output, trace through the Projection
// chain down to the Join's output, returning the Join output column index.
static idx_t TraceProjectionChain(LogicalOperator &op, idx_t idx, int depth = 0) {
    if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
        auto &proj = op.Cast<LogicalProjection>();
        if (idx >= proj.expressions.size()) return DConstants::INVALID_INDEX;
        auto &expr = proj.expressions[idx];
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
            auto child_idx = expr->Cast<BoundReferenceExpression>().index;
            return TraceProjectionChain(*op.children[0], child_idx, depth+1);
        }
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
            // Resolve via child's column bindings
            auto &binding = expr->Cast<BoundColumnRefExpression>().binding;
            auto child_bindings = op.children[0]->GetColumnBindings();
            for (idx_t i = 0; i < child_bindings.size(); i++) {
                if (child_bindings[i] == binding)
                    return TraceProjectionChain(*op.children[0], i, depth+1);
            }
            // Fallback: use column_index directly
            return TraceProjectionChain(*op.children[0], binding.column_index, depth+1);
        }
        // Non-reference expression (e.g. compress function) — trace into its child ref
        // compress(#0, 1) has children[0] = BoundReferenceExpression(#0)
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            // compress/decompress wraps a column ref in BoundFunctionExpression.children
            auto &func_expr = expr->Cast<BoundFunctionExpression>();
            for (auto &child : func_expr.children) {
                if (child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
                    auto child_idx = child->Cast<BoundReferenceExpression>().index;
                    return TraceProjectionChain(*op.children[0], child_idx, depth+1);
                }
                if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                    // Resolve BoundColumnRefExpression against child's column bindings
                    auto &binding = child->Cast<BoundColumnRefExpression>().binding;
                    auto child_bindings = op.children[0]->GetColumnBindings();
                    for (idx_t i = 0; i < child_bindings.size(); i++) {
                        if (child_bindings[i] == binding)
                            return TraceProjectionChain(*op.children[0], i, depth+1);
                    }
                    return DConstants::INVALID_INDEX;
                }
            }
            // Recurse into function children (nested functions)
            for (auto &child : func_expr.children) {
                if (child->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
                    // Create a temporary Projection-like wrapper to recurse
                    // Actually, just find the deepest BoundRef
                    std::function<idx_t(Expression &)> findRef = [&](Expression &e) -> idx_t {
                        if (e.GetExpressionClass() == ExpressionClass::BOUND_REF)
                            return e.Cast<BoundReferenceExpression>().index;
                        if (e.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
                            for (auto &c : e.Cast<BoundFunctionExpression>().children) {
                                auto r = findRef(*c);
                                if (r != DConstants::INVALID_INDEX) return r;
                            }
                        }
                        return DConstants::INVALID_INDEX;
                    };
                    auto found = findRef(*child);
                    if (found != DConstants::INVALID_INDEX)
                        return TraceProjectionChain(*op.children[0], found, depth+1);
                }
            }
        }
        // Handle CAST expressions (e.g. CAST(dim_id AS BIGINT))
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
            auto &cast = expr->Cast<BoundCastExpression>();
            // Recurse: create a temporary single-expression to trace
            if (cast.child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
                return TraceProjectionChain(*op.children[0],
                    cast.child->Cast<BoundReferenceExpression>().index, depth+1);
            }
            if (cast.child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                auto &binding = cast.child->Cast<BoundColumnRefExpression>().binding;
                auto cbindings = op.children[0]->GetColumnBindings();
                for (idx_t i = 0; i < cbindings.size(); i++) {
                    if (cbindings[i] == binding)
                        return TraceProjectionChain(*op.children[0], i, depth+1);
                }
                return TraceProjectionChain(*op.children[0], binding.column_index, depth+1);
            }
        }
        return DConstants::INVALID_INDEX;
    }
    // At the Join or Scan level — return the index directly
    return idx;
}

// ============================================================
// States
// ============================================================

// Flat result storage: contiguous arrays indexed by slot position.
// Flat-array hash table for hash mode accumulation.
// Column-major layout: each aggregate has a contiguous double array indexed by slot.
// Open-addressing with linear probing. No heap allocation per group.
struct FlatResultHT {
    // Slot metadata
    vector<hash_t> slot_keys;      // hash key per slot
    vector<uint8_t> slot_occupied;  // 0 or 1
    vector<uint8_t> slot_group_init;
    // Per-group values: column-major typed arrays (no Value boxing)
    // group_int_data[g * capacity + slot] for integer group columns
    // group_dbl_data[g * capacity + slot] for double group columns
    // group_is_int[g] = true if column g is stored as int64
    vector<int64_t> group_int_data;
    vector<double> group_dbl_data;
    vector<bool> group_is_int;  // per group column: true=int64, false=double
    vector<bool> group_uses_value; // per group column: true when exact Value storage is needed
    vector<vector<Value>> group_val_data; // [group][slot] for non-typed group equality/emit
    idx_t num_groups = 0;

    // Column-major accumulators: [agg * capacity + slot]
    vector<double> sum_data;
    vector<double> count_data;  // For AVG
    vector<double> min_data;    // Numeric MIN
    vector<double> max_data;    // Numeric MAX
    vector<uint8_t> has_val;
    // Value-based MIN/MAX for non-numeric types (VARCHAR, DATE, etc.)
    vector<vector<Value>> val_min_data;  // [agg][slot]
    vector<vector<Value>> val_max_data;  // [agg][slot]
    vector<bool> agg_uses_val_minmax;    // [agg] true if this agg uses Value path
    idx_t mask = 0, num_aggs = 0, count = 0, capacity = 0;

    void Init(idx_t expected, idx_t na, idx_t ng = 0, const vector<bool> *g_is_int = nullptr,
              const vector<bool> *val_minmax = nullptr, const vector<bool> *g_use_value = nullptr) {
        num_aggs = na; num_groups = ng;
        idx_t cap = 16;
        while (cap < expected * 2) cap <<= 1;
        mask = cap - 1;
        capacity = cap;
        slot_keys.resize(cap, 0);
        slot_occupied.resize(cap, 0);
        slot_group_init.resize(cap, 0);
        if (ng > 0) {
            group_int_data.resize(ng * cap, 0);
            group_dbl_data.resize(ng * cap, 0.0);
            if (g_is_int) group_is_int = *g_is_int;
            else group_is_int.assign(ng, true);
            if (g_use_value) group_uses_value = *g_use_value;
            else group_uses_value.assign(ng, false);
            group_val_data.resize(ng);
            for (idx_t g = 0; g < ng; g++) {
                if (g < group_uses_value.size() && group_uses_value[g]) {
                    group_val_data[g].resize(cap);
                }
            }
        }
        if (val_minmax) {
            agg_uses_val_minmax = *val_minmax;
            val_min_data.resize(na);
            val_max_data.resize(na);
            for (idx_t a = 0; a < na; a++) {
                if (agg_uses_val_minmax[a]) {
                    val_min_data[a].resize(cap);
                    val_max_data[a].resize(cap);
                }
            }
        }
        sum_data.resize(cap * na, 0.0);
        count_data.resize(cap * na, 0.0);
        min_data.resize(cap * na, std::numeric_limits<double>::max());
        max_data.resize(cap * na, std::numeric_limits<double>::lowest());
        has_val.resize(cap * na, 0);
    }

    void Prefetch(hash_t key) {
        if (mask) __builtin_prefetch(&slot_occupied[key & mask], 1, 1);
    }

    idx_t FindOrCreate(hash_t key) {
        if (!mask) Init(1024, num_aggs);
        // Grow at 70% load
        if (count >= capacity * 7 / 10) Grow();
        idx_t pos = key & mask;
        while (true) {
            if (!slot_occupied[pos]) {
                slot_keys[pos] = key;
                slot_occupied[pos] = 1;
                count++;
                return pos;
            }
            if (slot_keys[pos] == key) return pos;
            pos = (pos + 1) & mask;
        }
    }

    template <typename MatchFn, typename InitFn>
    idx_t FindOrCreate(hash_t key, MatchFn &&match, InitFn &&init) {
        if (!mask) Init(1024, num_aggs);
        if (count >= capacity * 7 / 10) Grow();
        idx_t pos = key & mask;
        while (true) {
            if (!slot_occupied[pos]) {
                slot_keys[pos] = key;
                slot_occupied[pos] = 1;
                count++;
                init(pos);
                return pos;
            }
            if (slot_keys[pos] == key && match(pos)) return pos;
            pos = (pos + 1) & mask;
        }
    }

    void Grow() {
        auto old_cap = capacity;
        auto old_keys = std::move(slot_keys);
        auto old_occ = std::move(slot_occupied);
        auto old_ginit = std::move(slot_group_init);
        auto old_gint = std::move(group_int_data);
        auto old_gdbl = std::move(group_dbl_data);
        auto old_gval = std::move(group_val_data);
        auto old_sum = std::move(sum_data);
        auto old_count = std::move(count_data);
        auto old_min = std::move(min_data);
        auto old_max = std::move(max_data);
        auto old_has = std::move(has_val);
        auto na = num_aggs;
        auto ng = num_groups;

        idx_t new_cap = old_cap * 2;
        mask = new_cap - 1;
        capacity = new_cap;
        count = 0;
        slot_keys.assign(new_cap, 0);
        slot_occupied.assign(new_cap, 0);
        slot_group_init.assign(new_cap, 0);
        if (ng > 0) {
            group_int_data.assign(ng * new_cap, 0);
            group_dbl_data.assign(ng * new_cap, 0.0);
            group_val_data.resize(ng);
            for (idx_t g = 0; g < ng; g++) {
                if (g < group_uses_value.size() && group_uses_value[g]) {
                    group_val_data[g].resize(new_cap);
                }
            }
        }
        sum_data.assign(new_cap * na, 0.0);
        count_data.assign(new_cap * na, 0.0);
        min_data.assign(new_cap * na, std::numeric_limits<double>::max());
        max_data.assign(new_cap * na, std::numeric_limits<double>::lowest());
        has_val.assign(new_cap * na, 0);

        // Save and resize Value-based MIN/MAX
        auto old_val_min = std::move(val_min_data);
        auto old_val_max = std::move(val_max_data);
        val_min_data.resize(na);
        val_max_data.resize(na);
        for (idx_t a = 0; a < na; a++) {
            if (a < agg_uses_val_minmax.size() && agg_uses_val_minmax[a]) {
                val_min_data[a].resize(new_cap);
                val_max_data[a].resize(new_cap);
            }
        }

        for (idx_t i = 0; i < old_cap; i++) {
            if (!old_occ[i]) continue;
            idx_t new_slot = old_keys[i] & mask;
            while (slot_occupied[new_slot]) {
                new_slot = (new_slot + 1) & mask;
            }
            slot_keys[new_slot] = old_keys[i];
            slot_occupied[new_slot] = 1;
            count++;
            slot_group_init[new_slot] = old_ginit[i];
            for (idx_t g = 0; g < ng; g++) {
                group_int_data[g * capacity + new_slot] = old_gint[g * old_cap + i];
                group_dbl_data[g * capacity + new_slot] = old_gdbl[g * old_cap + i];
                if (g < group_uses_value.size() && group_uses_value[g]) {
                    group_val_data[g][new_slot] = std::move(old_gval[g][i]);
                }
            }
            for (idx_t a = 0; a < na; a++) {
                sum_data[a * capacity + new_slot] = old_sum[a * old_cap + i];
                count_data[a * capacity + new_slot] = old_count[a * old_cap + i];
                if (old_has[a * old_cap + i]) {
                    min_data[a * capacity + new_slot] = old_min[a * old_cap + i];
                    max_data[a * capacity + new_slot] = old_max[a * old_cap + i];
                    has_val[a * capacity + new_slot] = 1;
                }
                // Move Value-based MIN/MAX
                if (a < agg_uses_val_minmax.size() && agg_uses_val_minmax[a] && old_has[a * old_cap + i]) {
                    val_min_data[a][new_slot] = std::move(old_val_min[a][i]);
                    val_max_data[a][new_slot] = std::move(old_val_max[a][i]);
                }
            }
        }
    }

    // Column-major accessors: [agg * capacity + slot]
    double &Sum(idx_t slot, idx_t agg) { return sum_data[agg * capacity + slot]; }
    double &Count(idx_t slot, idx_t agg) { return count_data[agg * capacity + slot]; }
    double &Min(idx_t slot, idx_t agg) { return min_data[agg * capacity + slot]; }
    double &Max(idx_t slot, idx_t agg) { return max_data[agg * capacity + slot]; }
    bool GetHas(idx_t slot, idx_t agg) { return has_val[agg * capacity + slot]; }
    void SetHas(idx_t slot, idx_t agg, bool v) { has_val[agg * capacity + slot] = v ? 1 : 0; }
    bool UsesValMinMax(idx_t agg) { return agg < agg_uses_val_minmax.size() && agg_uses_val_minmax[agg]; }
    Value &ValMin(idx_t slot, idx_t agg) { return val_min_data[agg][slot]; }
    Value &ValMax(idx_t slot, idx_t agg) { return val_max_data[agg][slot]; }
    bool GroupInit(idx_t slot) { return slot_group_init[slot]; }
    void SetGroupInit(idx_t slot) { slot_group_init[slot] = 1; }
    int64_t &GroupInt(idx_t g, idx_t slot) { return group_int_data[g * capacity + slot]; }
    double &GroupDbl(idx_t g, idx_t slot) { return group_dbl_data[g * capacity + slot]; }
    bool GroupUsesValue(idx_t g) const { return g < group_uses_value.size() && group_uses_value[g]; }
    Value &GroupVal(idx_t g, idx_t slot) { return group_val_data[g][slot]; }
};

static bool ResultSlotGroupMatches(FlatResultHT &rht, idx_t slot, DataChunk &input, const AggJoinColInfo &col, idx_t r) {
    for (idx_t g = 0; g < col.group_cols.size(); g++) {
        auto gi = col.group_cols[g];
        if (rht.GroupUsesValue(g)) {
            if (!Value::NotDistinctFrom(rht.GroupVal(g, slot), input.data[gi].GetValue(r))) return false;
            continue;
        }
        auto gt = input.data[gi].GetType().InternalType();
        if (g < col.group_compress.size() && col.group_compress[g].has_compress &&
            !col.group_compress[g].is_string_compress) {
            int64_t raw;
            switch (gt) {
            case PhysicalType::INT32: raw = FlatVector::GetData<int32_t>(input.data[gi])[r]; break;
            case PhysicalType::INT64: raw = FlatVector::GetData<int64_t>(input.data[gi])[r]; break;
            case PhysicalType::INT16: raw = FlatVector::GetData<int16_t>(input.data[gi])[r]; break;
            case PhysicalType::INT8:  raw = FlatVector::GetData<int8_t>(input.data[gi])[r]; break;
            case PhysicalType::UINT32: raw = (int64_t)FlatVector::GetData<uint32_t>(input.data[gi])[r]; break;
            case PhysicalType::UINT16: raw = (int64_t)FlatVector::GetData<uint16_t>(input.data[gi])[r]; break;
            case PhysicalType::UINT8:  raw = (int64_t)FlatVector::GetData<uint8_t>(input.data[gi])[r]; break;
            default: raw = input.data[gi].GetValue(r).GetValue<int64_t>(); break;
            }
            if (rht.GroupInt(g, slot) != raw) return false;
            continue;
        }
        switch (gt) {
        case PhysicalType::INT32:  if (rht.GroupInt(g, slot) != FlatVector::GetData<int32_t>(input.data[gi])[r]) return false; break;
        case PhysicalType::INT64:  if (rht.GroupInt(g, slot) != FlatVector::GetData<int64_t>(input.data[gi])[r]) return false; break;
        case PhysicalType::INT16:  if (rht.GroupInt(g, slot) != FlatVector::GetData<int16_t>(input.data[gi])[r]) return false; break;
        case PhysicalType::INT8:   if (rht.GroupInt(g, slot) != FlatVector::GetData<int8_t>(input.data[gi])[r]) return false; break;
        case PhysicalType::UINT32: if (rht.GroupInt(g, slot) != (int64_t)FlatVector::GetData<uint32_t>(input.data[gi])[r]) return false; break;
        case PhysicalType::UINT16: if (rht.GroupInt(g, slot) != (int64_t)FlatVector::GetData<uint16_t>(input.data[gi])[r]) return false; break;
        case PhysicalType::UINT8:  if (rht.GroupInt(g, slot) != (int64_t)FlatVector::GetData<uint8_t>(input.data[gi])[r]) return false; break;
        case PhysicalType::DOUBLE: if (rht.GroupDbl(g, slot) != FlatVector::GetData<double>(input.data[gi])[r]) return false; break;
        case PhysicalType::FLOAT:  if (rht.GroupDbl(g, slot) != FlatVector::GetData<float>(input.data[gi])[r]) return false; break;
        default: return false;
        }
    }
    return true;
}

struct AggJoinSinkState : public GlobalSinkState {
    OpenHT<BuildEntry> build_ht;
    unique_ptr<GroupedAggregateHashTable> agg_ht;
    FlatResultHT result_ht;
    bool use_native_ht = false;
    // Pre-allocated per-chunk buffers (avoid allocation in hot path)
    bool finalized = false;
    // Pre-allocated per-chunk scratch buffers (avoid per-chunk allocation)
    mutable unsafe_vector<double> probe_bc;       // build_count per row (hash mode)
    mutable unsafe_vector<idx_t> probe_slots;     // result HT slot per row (hash mode)
    mutable unsafe_vector<BuildEntry*> probe_build_ptrs; // cached build entry pointers (hash mode)
    mutable unsafe_vector<int64_t> prefilter_keys_buf; // typed key scratch for range prefilter
    mutable unsafe_vector<idx_t> direct_key_buf;  // key offsets per row (direct mode)
    mutable unsafe_vector<double> direct_bc_buf;  // build_count per row (direct mode, non-PK/FK)
    idx_t build_agg_slots = 0;                    // number of build-side aggregates

    // ── Build-side aggregate values for direct mode (transferred in Finalize) ──
    vector<double> direct_build_sums;    // [ba * range + key_offset]
    vector<double> direct_build_mins;
    vector<double> direct_build_maxs;
    vector<double> direct_build_counts;
    vector<uint8_t> direct_build_has;    // validity: was MIN/MAX initialized for this key?

    // ── Direct mode: flat arrays indexed by integer key ──
    // Eliminates ALL hash table lookups when keys are dense integers.
    bool direct_mode = false;
    int64_t key_min = 0;        // Min key value (offset for array indexing)
    idx_t key_range = 0;        // key_max - key_min + 1
    vector<idx_t> build_counts; // [key - key_min] → frequency count (0 = no match)
    vector<double> direct_sums; // [key_offset * num_aggs + agg] → accumulated sum
    vector<double> direct_counts; // [key_offset * num_aggs + agg] → count for AVG
    bool has_avg = false;         // Whether any aggregate is AVG
    // Direct mode MIN/MAX: flat arrays indexed by key offset
    vector<double> direct_mins; // [key_offset * num_aggs + agg]
    vector<double> direct_maxs;
    vector<uint8_t> direct_has; // [key_offset * num_aggs + agg] — whether min/max initialized
    bool has_min_max = false;   // Whether any aggregate is MIN or MAX
    idx_t num_aggs = 0;
    bool track_active_keys = false;          // Track matched key offsets for grouped direct emit
    vector<uint8_t> direct_key_seen;         // [key_offset] → whether key was seen on probe side
    vector<idx_t> direct_active_keys;        // matched key offsets for grouped direct emit
    bool segmented_direct_mode = false;      // Segment-backed direct path for simple grouped shapes
    bool segmented_multi_direct_mode = false; // Segment-backed fused path for narrow grouped multi-agg shapes
    bool build_slot_hash_mode = false;       // Non-integral/varlen GROUP BY join_key path keyed by build bucket slot
    idx_t segmented_shift = 16;              // segment size = 1 << shift
    idx_t segmented_size = 1 << 16;
    idx_t segmented_mask = (1 << 16) - 1;
    vector<unsafe_vector<uint32_t>> segmented_build_counts; // [segment][local_key]
    vector<unsafe_vector<double>> segmented_sums;           // [segment][local_key]
    vector<unsafe_vector<double>> segmented_avg_counts;     // [segment][local_key] for AVG
    vector<unsafe_vector<double>> segmented_multi_accums;   // [segment][local_key * accum_slots + slot]
    vector<unsafe_vector<double>> segmented_multi_avg_counts; // [segment][local_key * avg_slots + slot]
    vector<unsafe_vector<double>> segmented_multi_mins;     // [segment][local_key * min_slots + slot]
    vector<unsafe_vector<double>> segmented_multi_maxs;     // [segment][local_key * max_slots + slot]
    vector<unsafe_vector<uint8_t>> segmented_multi_min_has; // [segment][local_key * min_slots + slot]
    vector<unsafe_vector<uint8_t>> segmented_multi_max_has; // [segment][local_key * max_slots + slot]
    vector<idx_t> segmented_accum_index;                    // [agg] -> accum slot or INVALID_INDEX
    vector<idx_t> segmented_avg_index;                      // [agg] -> avg count slot or INVALID_INDEX
    vector<idx_t> segmented_min_index;                      // [agg] -> min slot or INVALID_INDEX
    vector<idx_t> segmented_max_index;                      // [agg] -> max slot or INVALID_INDEX
    idx_t segmented_accum_slots = 0;
    idx_t segmented_avg_slots = 0;
    idx_t segmented_min_slots = 0;
    idx_t segmented_max_slots = 0;
    vector<uint64_t> segmented_active_bits;                // 1 bit per key offset
    vector<idx_t> segmented_active_keys;                   // matched key offsets for emit
    // Group values stored as typed flat arrays (one per group column).
    // For the common case of GROUP BY key, group_is_key=true and values
    // are reconstructed from the array index (k + key_min) — no storage needed.
    bool group_is_key = false;  // True when single group col == probe key col
    vector<vector<int64_t>> direct_group_ints;   // [group_col][key_offset] for integer groups
    vector<vector<double>>  direct_group_doubles; // [group_col][key_offset] for double groups
    vector<vector<Value>>   direct_group_vals;   // [key_offset] → fallback for non-int/double groups
    vector<bool> direct_group_init;              // [key_offset] → whether group values stored
    bool all_bc_one = false;                     // True when all build counts == 1 (PK join)

    // ── Hash-mode prefilters: skip build HT probe for non-matching keys ──
    // 1. Min/max range check (single integer keys only)
    bool has_range_prefilter = false;
    int64_t build_key_min = 0;
    int64_t build_key_max = 0;
    // 2. Bloom filter over build hashes (works for all key types)
    vector<uint64_t> bloom_filter;  // bit array, size = bloom_bits / 64
    idx_t bloom_bits = 0;           // total bits (power of 2)
    idx_t bloom_mask = 0;           // bloom_bits - 1

    // ── Ungrouped direct mode: running scalar accumulators ──
    // Maintained during probe to avoid O(krange) emit loop.
    mutable vector<double> ungrouped_sum;   // [agg] running sum (SUM, COUNT, AVG numerator)
    mutable vector<double> ungrouped_count; // [agg] running count (AVG denominator)
    mutable vector<double> ungrouped_min;   // [agg] running MIN
    mutable vector<double> ungrouped_max;   // [agg] running MAX
    mutable vector<uint8_t> ungrouped_has;  // [agg] whether MIN/MAX initialized

    // ── Build-slot hash mode: accumulators keyed directly by build_ht bucket ──
    vector<double> build_slot_sums;
    vector<double> build_slot_counts;
    vector<double> build_slot_mins;
    vector<double> build_slot_maxs;
    vector<uint8_t> build_slot_has;
    vector<uint8_t> build_slot_seen;

    // ── Optional runtime trace stats ──
    idx_t build_rows_seen = 0;
    idx_t build_rows_kept = 0;
    idx_t probe_rows_seen = 0;
    idx_t hash_match_rows = 0;
    idx_t range_prefilter_skips = 0;
    idx_t bloom_prefilter_skips = 0;
    bool stats_emitted = false;
};

struct AggJoinSourceState : public GlobalSourceState {
    vector<idx_t> slot_indices; // For HT mode
    vector<idx_t> direct_keys;  // For direct mode: key offsets with data
    idx_t pos = 0;
    bool initialized = false;
    AggregateHTScanState scan_state;
    idx_t MaxThreads() override { return 1; }
};

// ============================================================
// PhysicalAggJoin — 2-child: build side sinks, probe feeds ExecuteInternal
// ============================================================

class PhysicalAggJoin : public CachingPhysicalOperator {
public:
    AggJoinColInfo col;
    // Stored for creating GroupedAggregateHashTable in sink state
    vector<unique_ptr<Expression>> owned_agg_exprs;
    vector<LogicalType> group_types;
    vector<LogicalType> payload_types;

    PhysicalAggJoin(PhysicalPlan &plan, vector<LogicalType> types, idx_t estimated_cardinality)
        : CachingPhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
        caching_supported = false;
    }

    bool IsSink() const override { return true; }
    bool ParallelSink() const override { return false; }
    bool IsSource() const override { return true; }
    vector<const_reference<PhysicalOperator>> GetSources() const override { return {*this}; }

    // Must return CachingOperatorState — CachingPhysicalOperator::Execute casts to it
    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &ctx) const override {
        return make_uniq<CachingOperatorState>();
    }

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &ctx) const override {
        auto state = make_uniq<AggJoinSinkState>();
        // Selective native GroupedAggregateHashTable path for the shapes where the
        // custom flat hash table is still only near-parity: composite or string-heavy
        // grouping with SUM/MIN/MAX only. Excludes build-side aggs, AVG, and COUNT.
        bool has_build_aggs = false;
        for (auto on_build : col.agg_on_build) {
            if (on_build) { has_build_aggs = true; break; }
        }
        bool native_safe = !has_build_aggs && !col.group_cols.empty();
        state->build_agg_slots = 0;
        for (auto on_build : col.agg_on_build) if (on_build) state->build_agg_slots++;
        for (auto &e : owned_agg_exprs) {
            auto &ba = e->Cast<BoundAggregateExpression>();
            auto fn = StringUtil::Upper(ba.function.name);
            if (fn != "SUM" && fn != "MIN" && fn != "MAX") {
                native_safe = false;
                break;
            }
        }
        bool weak_group_shape = (col.group_cols.size() > 1 || col.probe_key_cols.size() > 1);
        state->use_native_ht = native_safe && weak_group_shape;
        if (state->use_native_ht) {
            vector<BoundAggregateExpression *> bindings;
            bindings.reserve(owned_agg_exprs.size());
            for (auto &e : owned_agg_exprs) {
                bindings.push_back(&e->Cast<BoundAggregateExpression>());
            }
            state->agg_ht = make_uniq<GroupedAggregateHashTable>(ctx, BufferAllocator::Get(ctx),
                                                                 group_types, payload_types, bindings);
        }
        return std::move(state);
    }
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &ctx) const override {
        return make_uniq<AggJoinSourceState>();
    }

    // ── Build side: count per key ──
    SinkResultType Sink(ExecutionContext &ctx, DataChunk &chunk, OperatorSinkInput &input) const override {
        auto &sink = input.global_state.Cast<AggJoinSinkState>();
        auto n = chunk.size(); if (!n) return SinkResultType::NEED_MORE_INPUT;
        sink.build_rows_seen += n;

        // Flatten all accessed columns
        for (auto i : col.build_key_cols) chunk.data[i].Flatten(n);
        for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
            if (col.agg_on_build.size() > a && col.agg_on_build[a]) {
                auto bci = col.build_agg_cols[a];
                if (bci != DConstants::INVALID_INDEX && bci < chunk.ColumnCount())
                    chunk.data[bci].Flatten(n);
            }
        }
        Vector hv(LogicalType::HASH, n); hv.Flatten(n);
        VectorOperations::Hash(chunk.data[col.build_key_cols[0]], hv, n);
        for (idx_t i=1;i<col.build_key_cols.size();i++) VectorOperations::CombineHash(hv, chunk.data[col.build_key_cols[i]], n);
        auto h = FlatVector::GetData<hash_t>(hv);
        ApplyAggJoinTestHashBits(h, n);
        // Extract integer key values for direct mode detection
        auto bki = col.build_key_cols[0];
        auto btype = chunk.data[bki].GetType().InternalType();
        for (idx_t r = 0; r < n; r++) {
            // Skip rows with NULL join keys (inner join: NULLs never match)
            bool has_null = false;
            for (auto ki : col.build_key_cols) {
                if (FlatVector::IsNull(chunk.data[ki], r)) { has_null = true; break; }
            }
            if (has_null) continue;
            sink.build_rows_kept++;
            auto &entry = sink.build_ht.Insert(
                h[r], [&](const BuildEntry &b) { return BuildEntryMatches(b, chunk, col.build_key_cols, r); },
                [&](BuildEntry &b) { InitBuildEntryKeys(b, chunk, col.build_key_cols, r); });
            entry.count++;
            // Accumulate build-side aggregate values in separate map keyed by actual value
            {
                auto &bav = entry.bav;
                idx_t ba = 0;
                if (sink.build_agg_slots) bav.EnsureSize(sink.build_agg_slots);
                for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
                    if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                    auto bci = col.build_agg_cols[a];
                    if (bci != DConstants::INVALID_INDEX && bci < chunk.ColumnCount()) {
                        // Use FlatVector typed access instead of GetValue() boxing
                        auto bvtype = chunk.data[bci].GetType().InternalType();
                        auto *bvalidity = FlatVector::Validity(chunk.data[bci]).GetData();
                        bool is_valid = !bvalidity || ((bvalidity[r/64] >> (r%64)) & 1);
                        if (is_valid) {
                            double v;
                            switch (bvtype) {
                            case PhysicalType::DOUBLE: v = FlatVector::GetData<double>(chunk.data[bci])[r]; break;
                            case PhysicalType::FLOAT:  v = FlatVector::GetData<float>(chunk.data[bci])[r]; break;
                            case PhysicalType::INT64:  v = (double)FlatVector::GetData<int64_t>(chunk.data[bci])[r]; break;
                            case PhysicalType::INT32:  v = (double)FlatVector::GetData<int32_t>(chunk.data[bci])[r]; break;
                            case PhysicalType::INT16:  v = (double)FlatVector::GetData<int16_t>(chunk.data[bci])[r]; break;
                            case PhysicalType::INT8:   v = (double)FlatVector::GetData<int8_t>(chunk.data[bci])[r]; break;
                            case PhysicalType::UINT64: v = (double)FlatVector::GetData<uint64_t>(chunk.data[bci])[r]; break;
                            case PhysicalType::UINT32: v = (double)FlatVector::GetData<uint32_t>(chunk.data[bci])[r]; break;
                            case PhysicalType::UINT16: v = (double)FlatVector::GetData<uint16_t>(chunk.data[bci])[r]; break;
                            case PhysicalType::UINT8:  v = (double)FlatVector::GetData<uint8_t>(chunk.data[bci])[r]; break;
                            default: v = chunk.data[bci].GetValue(r).GetValue<double>(); break;
                            }
                            auto &f = col.agg_funcs[a];
                            if (f == "SUM" || f == "AVG") {
                                bav.agg_sum[ba] += v;
                                bav.agg_count[ba] += 1.0;
                            } else if (f == "MIN") {
                                if (!bav.agg_init[ba] || v < bav.agg_min[ba])
                                    { bav.agg_min[ba] = v; bav.agg_init[ba] = 1; }
                            } else if (f == "MAX") {
                                if (!bav.agg_init[ba] || v > bav.agg_max[ba])
                                    { bav.agg_max[ba] = v; bav.agg_init[ba] = 1; }
                            } else if (f == "COUNT") {
                                bav.agg_count[ba] += 1.0;
                            }
                        }
                    }
                    ba++;
                }
            }
        }
        // Build-side accumulation is now done inline in the Insert loop above.
        return SinkResultType::NEED_MORE_INPUT;
    }

    SinkFinalizeType Finalize(Pipeline &p, Event &e, ClientContext &c, OperatorSinkFinalizeInput &input) const override {
        auto &sink = input.global_state.Cast<AggJoinSinkState>();
        sink.finalized = true;

        // Detect if direct mode is possible: single integer build key
        // with small range (< 1M) that fits in a flat array.
        if (col.build_key_cols.size() == 1 && col.probe_key_cols.size() == 1) {
            int64_t kmin = INT64_MAX, kmax = INT64_MIN;
            bool all_int = true;
            sink.build_ht.ForEach([&](BuildEntry &b) {
                if (b.int_key == INT64_MIN) { all_int = false; return; }
                kmin = std::min(kmin, b.int_key);
                kmax = std::max(kmax, b.int_key);
            });
            idx_t range = (kmin <= kmax) ? (idx_t)(kmax - kmin + 1) : 0;
            bool group_is_key_candidate = (col.group_cols.size() == 1 && col.group_cols[0] == col.probe_key_cols[0]);
            bool ungrouped_agg = col.group_cols.empty();

            // Direct mode: flat array indexing. Cost-based threshold:
            // Compute actual memory footprint of all arrays that would be allocated.
            // Allow larger budgets for the simplest output shapes where direct mode
            // still wins beyond the old 16MB heuristic.
            auto na_total = (idx_t)col.agg_funcs.size();
            bool would_have_minmax = false, would_have_avg = false;
            idx_t n_build_aggs_est = 0;
            for (idx_t a = 0; a < na_total; a++) {
                if (col.agg_funcs[a] == "MIN" || col.agg_funcs[a] == "MAX") would_have_minmax = true;
                if (col.agg_funcs[a] == "AVG") would_have_avg = true;
                if (col.agg_on_build.size() > a && col.agg_on_build[a]) n_build_aggs_est++;
            }
            // Memory per key: build_counts(8) + sums(8*na) + optional mins/maxs/has + optional counts + build arrays
            idx_t bytes_per_key = sizeof(idx_t)                          // build_counts
                                + sizeof(double) * na_total              // direct_sums
                                + (would_have_minmax ? (sizeof(double) * 2 + sizeof(uint8_t)) * na_total : 0) // mins+maxs+has
                                + (would_have_avg ? sizeof(double) * na_total : 0)   // direct_counts
                                + (n_build_aggs_est > 0 ? (sizeof(double) * 4 + sizeof(uint8_t)) * n_build_aggs_est : 0); // build arrays
            idx_t max_working_set = 16 * 1024 * 1024; // default target
            if (ungrouped_agg || group_is_key_candidate) {
                max_working_set = 32 * 1024 * 1024;
            }
            if (group_is_key_candidate && !ungrouped_agg && na_total == 1 &&
                !would_have_minmax && !would_have_avg && n_build_aggs_est == 0) {
                max_working_set = 48 * 1024 * 1024;
            }
            idx_t direct_limit = bytes_per_key > 0 ? max_working_set / bytes_per_key : 2000000;
            if (direct_limit < 100000) direct_limit = 100000;  // floor: always allow ≥100K keys
            if (direct_limit > 8000000) direct_limit = 8000000; // avoid pathological over-allocation
            // Disable direct mode when build-side aggs are present and GROUP BY != join key.
            // Direct mode maps one group per key offset, but multiple groups can share one key.
            bool has_build_aggs_for_dm = false;
            for (idx_t a = 0; a < col.agg_funcs.size(); a++)
                if (col.agg_on_build.size() > a && col.agg_on_build[a]) { has_build_aggs_for_dm = true; break; }
            bool allow_direct = !has_build_aggs_for_dm || group_is_key_candidate;
            // Non-numeric MIN/MAX can't use double flat arrays — skip direct mode
            bool has_non_numeric_minmax = false;
            for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
                if ((col.agg_funcs[a] == "MIN" || col.agg_funcs[a] == "MAX") &&
                    a < col.agg_is_numeric.size() && !col.agg_is_numeric[a]) {
                    has_non_numeric_minmax = true; break;
                }
            }
            if (all_int && range > 0 && range <= direct_limit && !has_non_numeric_minmax &&
                (ungrouped_agg || allow_direct)) {
                sink.direct_mode = true;
                sink.key_min = kmin;
                sink.key_range = range;
                sink.num_aggs = col.agg_funcs.size();
                sink.build_counts.resize(range, 0);
                sink.direct_sums.resize(range * sink.num_aggs, 0.0);
                // Check if any aggregates are MIN/MAX or AVG and allocate flat arrays
                for (auto &f : col.agg_funcs) {
                    if (f == "MIN" || f == "MAX") sink.has_min_max = true;
                    if (f == "AVG") sink.has_avg = true;
                }
                if (sink.has_min_max) {
                    sink.direct_mins.resize(range * sink.num_aggs, std::numeric_limits<double>::max());
                    sink.direct_maxs.resize(range * sink.num_aggs, std::numeric_limits<double>::lowest());
                    sink.direct_has.resize(range * sink.num_aggs, false);
                }
                if (sink.has_avg) {
                    sink.direct_counts.resize(range * sink.num_aggs, 0.0);
                }
                // Initialize ungrouped running accumulators
                if (col.group_cols.empty()) {
                    auto na = col.agg_funcs.size();
                    sink.ungrouped_sum.resize(na, 0.0);
                    sink.ungrouped_count.resize(na, 0.0);
                    sink.ungrouped_min.resize(na, std::numeric_limits<double>::max());
                    sink.ungrouped_max.resize(na, std::numeric_limits<double>::lowest());
                    sink.ungrouped_has.resize(na, 0);
                }
                // Detect group_is_key: single group column == probe key column
                // In this case, the group value IS the key: k + kmin. No storage needed.
                if (col.group_cols.size() == 1 && col.group_cols[0] == col.probe_key_cols[0]) {
                    sink.group_is_key = true;
                }
                if (sink.group_is_key && range > 1000000) {
                    sink.track_active_keys = true;
                    sink.direct_key_seen.assign(range, 0);
                    sink.direct_active_keys.reserve(std::min<idx_t>(range, sink.build_ht.count));
                }
                if (!sink.group_is_key) {
                    sink.direct_group_vals.resize(range);
                    sink.direct_group_init.resize(range, false);
                }
                // Populate build_counts and detect all-ones (PK join)
                sink.all_bc_one = true;
                // Check if there are build-side aggs that need direct mode arrays
                bool has_build_aggs_dm = false;
                idx_t n_build_aggs = 0;
                for (idx_t a = 0; a < col.agg_funcs.size(); a++)
                    if (col.agg_on_build.size() > a && col.agg_on_build[a]) { has_build_aggs_dm = true; n_build_aggs++; }

                sink.build_ht.ForEach([&](BuildEntry &b) {
                    auto offset = (idx_t)(b.int_key - kmin);
                    sink.build_counts[offset] = b.count;
                    if (b.count != 1) sink.all_bc_one = false;
                });

                // Transfer build-side agg values from BuildEntry to direct mode.
                // Store per-key build-side sums/mins/maxs that can be looked up during probe.
                if (has_build_aggs_dm) {
                    sink.direct_build_sums.resize(n_build_aggs * range, 0.0);
                    sink.direct_build_mins.resize(n_build_aggs * range, std::numeric_limits<double>::max());
                    sink.direct_build_maxs.resize(n_build_aggs * range, std::numeric_limits<double>::lowest());
                    sink.direct_build_counts.resize(n_build_aggs * range, 0.0);
                    sink.direct_build_has.resize(n_build_aggs * range, 0);
                    sink.build_ht.ForEach([&](BuildEntry &b) {
                        auto offset = (idx_t)(b.int_key - kmin);
                        auto &bav = b.bav;
                        idx_t ba = 0;
                        for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
                            if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                            sink.direct_build_sums[ba * range + offset] = bav.agg_sum[ba];
                            sink.direct_build_mins[ba * range + offset] = bav.agg_min[ba];
                            sink.direct_build_maxs[ba * range + offset] = bav.agg_max[ba];
                            sink.direct_build_counts[ba * range + offset] = bav.agg_count[ba];
                            sink.direct_build_has[ba * range + offset] = bav.agg_init[ba];
                            ba++;
                        }
                    });
                }
            }

            // Segment-backed direct mode for the simplest grouped shape:
            // single aggregate on the probe side, GROUP BY join key.
            // This extends direct-mode behavior beyond the flat-array cutoff
            // without widening the main direct path for every query shape.
            bool segmented_simple_shape = group_is_key_candidate && !ungrouped_agg &&
                                          na_total == 1 && n_build_aggs_est == 0 &&
                                          !would_have_minmax;
            if (!sink.direct_mode && all_int && segmented_simple_shape) {
                auto &f0 = col.agg_funcs[0];
                bool segmented_supported = (f0 == "SUM" || f0 == "COUNT" || f0 == "AVG");
                idx_t segmented_bytes_per_key = sizeof(uint32_t) + sizeof(double) +
                                                (f0 == "AVG" ? sizeof(double) : 0);
                idx_t segmented_budget = 96 * 1024 * 1024;
                idx_t segmented_limit = segmented_bytes_per_key > 0 ? segmented_budget / segmented_bytes_per_key : 0;
                if (segmented_supported && range > direct_limit && range <= segmented_limit) {
                    sink.segmented_direct_mode = true;
                    sink.key_min = kmin;
                    sink.key_range = range;
                    sink.num_aggs = 1;
                    sink.group_is_key = true;
                    sink.all_bc_one = true;
                    auto seg_count = (range + sink.segmented_mask) >> sink.segmented_shift;
                    sink.segmented_build_counts.resize(seg_count);
                    sink.segmented_sums.resize(seg_count);
                    if (f0 == "AVG") sink.segmented_avg_counts.resize(seg_count);
                    sink.segmented_active_bits.assign((range + 63) / 64, 0);
                        sink.segmented_active_keys.reserve(std::min<idx_t>(range, sink.build_ht.count));
                        sink.build_ht.ForEach([&](BuildEntry &b) {
                            auto offset = (idx_t)(b.int_key - kmin);
                        auto seg = offset >> sink.segmented_shift;
                        auto local = offset & sink.segmented_mask;
                        if (sink.segmented_build_counts[seg].empty()) {
                            sink.segmented_build_counts[seg].resize(sink.segmented_size, 0);
                            sink.segmented_sums[seg].resize(sink.segmented_size, 0.0);
                            if (f0 == "AVG") sink.segmented_avg_counts[seg].resize(sink.segmented_size, 0.0);
                        }
                        sink.segmented_build_counts[seg][local] = (uint32_t)b.count;
                        if (b.count != 1) sink.all_bc_one = false;
                    });
                }
            }

            // Segment-backed fused multi-aggregate mode for grouped numeric
            // SUM/COUNT/AVG/MIN/MAX over the join key. Keeps the single-aggregate
            // segmented fast path intact and only handles a narrow, well-behaved shape.
            bool segmented_multi_shape = group_is_key_candidate && !ungrouped_agg &&
                                         na_total >= 2 && na_total <= 4 &&
                                         n_build_aggs_est == 0;
            if (!sink.direct_mode && !sink.segmented_direct_mode && all_int && segmented_multi_shape) {
                bool segmented_multi_supported = true;
                idx_t accum_slots = 0, avg_slots = 0, min_slots = 0, max_slots = 0;
                vector<idx_t> accum_index(na_total, DConstants::INVALID_INDEX);
                vector<idx_t> avg_index(na_total, DConstants::INVALID_INDEX);
                vector<idx_t> min_index(na_total, DConstants::INVALID_INDEX);
                vector<idx_t> max_index(na_total, DConstants::INVALID_INDEX);
                for (idx_t a = 0; a < na_total; a++) {
                    auto &fn = col.agg_funcs[a];
                    auto ai = col.agg_input_cols[a];
                    if (fn == "SUM" || fn == "AVG" || fn == "COUNT") {
                        accum_index[a] = accum_slots++;
                    }
                    if (fn == "AVG") {
                        avg_index[a] = avg_slots++;
                    } else if (fn == "MIN") {
                        min_index[a] = min_slots++;
                    } else if (fn == "MAX") {
                        max_index[a] = max_slots++;
                    } else if (fn != "SUM" && fn != "COUNT") {
                        segmented_multi_supported = false;
                        break;
                    }
                    if ((fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX") &&
                        (a >= payload_types.size() || payload_types[a].InternalType() != PhysicalType::DOUBLE)) {
                        segmented_multi_supported = false;
                        break;
                    }
                }
                idx_t segmented_bytes_per_key = sizeof(uint32_t) +
                                                sizeof(double) * accum_slots +
                                                sizeof(double) * avg_slots +
                                                (sizeof(double) + sizeof(uint8_t)) * min_slots +
                                                (sizeof(double) + sizeof(uint8_t)) * max_slots;
                idx_t segmented_budget = 160 * 1024 * 1024;
                idx_t segmented_limit = segmented_bytes_per_key > 0 ? segmented_budget / segmented_bytes_per_key : 0;
                if (segmented_multi_supported && range > direct_limit && range <= segmented_limit) {
                    sink.segmented_multi_direct_mode = true;
                    sink.key_min = kmin;
                    sink.key_range = range;
                    sink.num_aggs = na_total;
                    sink.group_is_key = true;
                    sink.all_bc_one = true;
                    sink.segmented_accum_slots = accum_slots;
                    sink.segmented_avg_slots = avg_slots;
                    sink.segmented_min_slots = min_slots;
                    sink.segmented_max_slots = max_slots;
                    sink.segmented_accum_index = std::move(accum_index);
                    sink.segmented_avg_index = std::move(avg_index);
                    sink.segmented_min_index = std::move(min_index);
                    sink.segmented_max_index = std::move(max_index);
                    auto seg_count = (range + sink.segmented_mask) >> sink.segmented_shift;
                    sink.segmented_build_counts.resize(seg_count);
                    sink.segmented_multi_accums.resize(seg_count);
                    if (avg_slots) sink.segmented_multi_avg_counts.resize(seg_count);
                    if (min_slots) {
                        sink.segmented_multi_mins.resize(seg_count);
                        sink.segmented_multi_min_has.resize(seg_count);
                    }
                    if (max_slots) {
                        sink.segmented_multi_maxs.resize(seg_count);
                        sink.segmented_multi_max_has.resize(seg_count);
                    }
                    sink.segmented_active_bits.assign((range + 63) / 64, 0);
                    sink.segmented_active_keys.reserve(std::min<idx_t>(range, sink.build_ht.count));
                    sink.build_ht.ForEach([&](BuildEntry &b) {
                        auto offset = (idx_t)(b.int_key - kmin);
                        auto seg = offset >> sink.segmented_shift;
                        auto local = offset & sink.segmented_mask;
                        if (sink.segmented_build_counts[seg].empty()) {
                            sink.segmented_build_counts[seg].resize(sink.segmented_size, 0);
                            if (accum_slots) sink.segmented_multi_accums[seg].resize(sink.segmented_size * accum_slots, 0.0);
                            if (avg_slots) sink.segmented_multi_avg_counts[seg].resize(sink.segmented_size * avg_slots, 0.0);
                            if (min_slots) {
                                sink.segmented_multi_mins[seg].resize(sink.segmented_size * min_slots, std::numeric_limits<double>::max());
                                sink.segmented_multi_min_has[seg].resize(sink.segmented_size * min_slots, 0);
                            }
                            if (max_slots) {
                                sink.segmented_multi_maxs[seg].resize(sink.segmented_size * max_slots, std::numeric_limits<double>::lowest());
                                sink.segmented_multi_max_has[seg].resize(sink.segmented_size * max_slots, 0);
                            }
                        }
                        sink.segmented_build_counts[seg][local] = (uint32_t)b.count;
                        if (b.count != 1) sink.all_bc_one = false;
                    });
                }
            }

            // Set up hash-mode range prefilter: if not direct mode but we have single
            // integer keys, store min/max for early rejection during probe.
            if (!sink.direct_mode && !sink.segmented_direct_mode && !sink.segmented_multi_direct_mode && all_int && range > 0) {
                sink.has_range_prefilter = true;
                sink.build_key_min = kmin;
                sink.build_key_max = kmax;
            }

            // Single-key GROUP BY join_key fallback for shapes that miss direct mode:
            // keep the probe-side numeric path on AGGJOIN, but accumulate directly by
            // build bucket slot instead of paying for a second result-hash-table lookup.
            if (!sink.direct_mode && !sink.segmented_direct_mode && !sink.segmented_multi_direct_mode &&
                !ungrouped_agg && group_is_key_candidate && col.agg_funcs.size() >= 1) {
                auto key_type = group_types.empty() ? LogicalTypeId::INVALID : group_types[0].id();
                bool float_key = (key_type == LogicalTypeId::DOUBLE || key_type == LogicalTypeId::FLOAT);
                bool varchar_key = (key_type == LogicalTypeId::VARCHAR);
                if (!varchar_key && col.group_compress.size() == 1 &&
                    col.group_compress[0].has_compress &&
                    col.group_compress[0].is_string_compress &&
                    col.group_compress[0].original_type.id() == LogicalTypeId::VARCHAR) {
                    varchar_key = true;
                }
                bool supported = float_key || varchar_key;
                for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
                    if (col.agg_on_build.size() > a && col.agg_on_build[a]) { supported = false; break; }
                    auto &fn = col.agg_funcs[a];
                    if (fn != "SUM" && fn != "COUNT" && fn != "AVG" && fn != "MIN" && fn != "MAX") {
                        supported = false;
                        break;
                    }
                    if ((fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX") &&
                        (a >= col.agg_is_numeric.size() || !col.agg_is_numeric[a])) {
                        supported = false;
                        break;
                    }
                }
                if (supported) {
                    sink.build_slot_hash_mode = true;
                    sink.num_aggs = col.agg_funcs.size();
                    auto cap = sink.build_ht.mask + 1;
                    sink.build_slot_sums.assign(cap * sink.num_aggs, 0.0);
                    sink.build_slot_seen.assign(cap, 0);
                    for (auto &fn : col.agg_funcs) {
                        if (fn == "MIN" || fn == "MAX") sink.has_min_max = true;
                        if (fn == "AVG") sink.has_avg = true;
                    }
                    if (sink.has_avg) sink.build_slot_counts.assign(cap * sink.num_aggs, 0.0);
                    if (sink.has_min_max) {
                        sink.build_slot_mins.assign(cap * sink.num_aggs, std::numeric_limits<double>::max());
                        sink.build_slot_maxs.assign(cap * sink.num_aggs, std::numeric_limits<double>::lowest());
                        sink.build_slot_has.assign(cap * sink.num_aggs, 0);
                    }
                }
            }
        }

        // Bloom filter over build hashes — works for all key types (composite, non-integer).
        // Skip for direct mode (has flat array lookup, doesn't need bloom).
        // Size: ~8 bits per build key → <1% false positive rate.
        if (!sink.direct_mode && !sink.segmented_direct_mode && !sink.segmented_multi_direct_mode && sink.build_ht.count > 0) {
            idx_t n_keys = sink.build_ht.count;
            // Size bloom filter: 8 bits per key, minimum 1024 bits, round up to power of 2
            idx_t target_bits = n_keys * 8;
            if (target_bits < 1024) target_bits = 1024;
            idx_t bits = 1024;
            while (bits < target_bits) bits <<= 1;
            sink.bloom_bits = bits;
            sink.bloom_mask = bits - 1;
            sink.bloom_filter.assign(bits / 64, 0);
            // Populate: set 2 bits per key (double hashing for lower false positive rate)
            sink.build_ht.ForEach([&](BuildEntry &b) {
                auto h1 = b.key;
                auto h2 = (h1 >> 17) | (h1 << 47); // second hash from bit rotation
                sink.bloom_filter[(h1 & sink.bloom_mask) / 64] |= (1ULL << ((h1 & sink.bloom_mask) % 64));
                sink.bloom_filter[(h2 & sink.bloom_mask) / 64] |= (1ULL << ((h2 & sink.bloom_mask) % 64));
            });
        }

        return SinkFinalizeType::READY;
    }

    // ── Probe side: join + aggregate in one pass ──
    OperatorResultType ExecuteInternal(ExecutionContext &ctx, DataChunk &input, DataChunk &chunk,
                                       GlobalOperatorState &gstate, OperatorState &state) const override {
        if (!sink_state) { chunk.SetCardinality(0); return OperatorResultType::NEED_MORE_INPUT; }
        auto &sink = sink_state->Cast<AggJoinSinkState>();
        if (!sink.finalized || sink.build_ht.mask == 0) {
            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }
        auto n = input.size(); if (!n) return OperatorResultType::NEED_MORE_INPUT;
        auto na = col.agg_funcs.size();
        sink.probe_rows_seen += n;

        // ═══════════════════════════════════════════════════
        // DIRECT MODE: flat array accumulation (zero hash overhead)
        // ═══════════════════════════════════════════════════
        if (sink.segmented_multi_direct_mode) {
            auto pki = col.probe_key_cols[0];
            input.data[pki].Flatten(n);
            for (auto i : col.agg_input_cols)
                if (i != DConstants::INVALID_INDEX && i < input.ColumnCount()) input.data[i].Flatten(n);

            auto ptype = input.data[pki].GetType().InternalType();
            auto kmin = sink.key_min;
            auto seg_shift = sink.segmented_shift;
            auto seg_mask = sink.segmented_mask;
            auto range = sink.key_range;
            bool pkfk = sink.all_bc_one;
            auto *active_bits = sink.segmented_active_bits.data();
            auto mark_active = [&](idx_t key_offset) {
                auto word = key_offset >> 6;
                auto bit = 1ULL << (key_offset & 63);
                if (!(active_bits[word] & bit)) {
                    active_bits[word] |= bit;
                    sink.segmented_active_keys.push_back(key_offset);
                }
            };

            struct SegAggSlot {
                enum Kind { SUM_VAL, AVG_VAL, COUNT_STAR, COUNT_COL, MIN_VAL, MAX_VAL } kind;
                const double *vals = nullptr;
                const uint64_t *validity = nullptr;
                idx_t accum_idx = DConstants::INVALID_INDEX;
                idx_t avg_idx = DConstants::INVALID_INDEX;
                idx_t mm_idx = DConstants::INVALID_INDEX;
            };
            vector<SegAggSlot> agg_slots(na);
            for (idx_t a = 0; a < na; a++) {
                auto &slot = agg_slots[a];
                auto &f = col.agg_funcs[a];
                auto ai = col.agg_input_cols[a];
                slot.accum_idx = sink.segmented_accum_index[a];
                slot.avg_idx = sink.segmented_avg_index[a];
                if (f == "SUM") slot.kind = SegAggSlot::SUM_VAL;
                else if (f == "AVG") slot.kind = SegAggSlot::AVG_VAL;
                else if (f == "COUNT" && ai == DConstants::INVALID_INDEX) slot.kind = SegAggSlot::COUNT_STAR;
                else if (f == "COUNT") slot.kind = SegAggSlot::COUNT_COL;
                else if (f == "MIN") { slot.kind = SegAggSlot::MIN_VAL; slot.mm_idx = sink.segmented_min_index[a]; }
                else { slot.kind = SegAggSlot::MAX_VAL; slot.mm_idx = sink.segmented_max_index[a]; }
                if (ai != DConstants::INVALID_INDEX && ai < input.ColumnCount()) {
                    slot.validity = FlatVector::Validity(input.data[ai]).GetData();
                    if (f != "COUNT") slot.vals = FlatVector::GetData<double>(input.data[ai]);
                }
            }

            #define SEGMENTED_MULTI_KEY_LOOP(KTYPE) { \
                auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                for (idx_t r = 0; r < n; r++) { \
                    auto k = (idx_t)((int64_t)keys[r] - kmin); \
                    if (k >= range) continue; \
                    auto seg = k >> seg_shift; \
                    auto local = k & seg_mask; \
                    auto &count_seg = sink.segmented_build_counts[seg]; \
                    if (count_seg.empty()) continue; \
                    auto bcount = count_seg[local]; \
                    if (!bcount) continue; \
                    mark_active(k); \
                    auto mult = pkfk ? 1.0 : (double)bcount; \
                    auto *accums = sink.segmented_accum_slots ? sink.segmented_multi_accums[seg].data() : nullptr; \
                    auto *avg_counts = sink.segmented_avg_slots ? sink.segmented_multi_avg_counts[seg].data() : nullptr; \
                    auto *mins = sink.segmented_min_slots ? sink.segmented_multi_mins[seg].data() : nullptr; \
                    auto *maxs = sink.segmented_max_slots ? sink.segmented_multi_maxs[seg].data() : nullptr; \
                    auto *min_has = sink.segmented_min_slots ? sink.segmented_multi_min_has[seg].data() : nullptr; \
                    auto *max_has = sink.segmented_max_slots ? sink.segmented_multi_max_has[seg].data() : nullptr; \
                    auto accum_base = local * sink.segmented_accum_slots; \
                    auto avg_base = local * sink.segmented_avg_slots; \
                    auto min_base = local * sink.segmented_min_slots; \
                    auto max_base = local * sink.segmented_max_slots; \
                    for (idx_t a = 0; a < na; a++) { \
                        auto &slot = agg_slots[a]; \
                        switch (slot.kind) { \
                        case SegAggSlot::COUNT_STAR: \
                            accums[accum_base + slot.accum_idx] += mult; \
                            break; \
                        case SegAggSlot::COUNT_COL: \
                            if (slot.validity && !((slot.validity[r/64] >> (r%64)) & 1)) break; \
                            accums[accum_base + slot.accum_idx] += mult; \
                            break; \
                        case SegAggSlot::SUM_VAL: \
                            if (slot.validity && !((slot.validity[r/64] >> (r%64)) & 1)) break; \
                            accums[accum_base + slot.accum_idx] += slot.vals[r] * mult; \
                            break; \
                        case SegAggSlot::AVG_VAL: \
                            if (slot.validity && !((slot.validity[r/64] >> (r%64)) & 1)) break; \
                            accums[accum_base + slot.accum_idx] += slot.vals[r] * mult; \
                            avg_counts[avg_base + slot.avg_idx] += mult; \
                            break; \
                        case SegAggSlot::MIN_VAL: \
                            if (slot.validity && !((slot.validity[r/64] >> (r%64)) & 1)) break; \
                            if (!min_has[min_base + slot.mm_idx] || slot.vals[r] < mins[min_base + slot.mm_idx]) { \
                                mins[min_base + slot.mm_idx] = slot.vals[r]; \
                                min_has[min_base + slot.mm_idx] = 1; \
                            } \
                            break; \
                        case SegAggSlot::MAX_VAL: \
                            if (slot.validity && !((slot.validity[r/64] >> (r%64)) & 1)) break; \
                            if (!max_has[max_base + slot.mm_idx] || slot.vals[r] > maxs[max_base + slot.mm_idx]) { \
                                maxs[max_base + slot.mm_idx] = slot.vals[r]; \
                                max_has[max_base + slot.mm_idx] = 1; \
                            } \
                            break; \
                        } \
                    } \
                } \
            }
            switch (ptype) {
            case PhysicalType::INT8:   SEGMENTED_MULTI_KEY_LOOP(int8_t); break;
            case PhysicalType::INT16:  SEGMENTED_MULTI_KEY_LOOP(int16_t); break;
            case PhysicalType::INT32:  SEGMENTED_MULTI_KEY_LOOP(int32_t); break;
            case PhysicalType::INT64:  SEGMENTED_MULTI_KEY_LOOP(int64_t); break;
            case PhysicalType::UINT8:  SEGMENTED_MULTI_KEY_LOOP(uint8_t); break;
            case PhysicalType::UINT16: SEGMENTED_MULTI_KEY_LOOP(uint16_t); break;
            case PhysicalType::UINT32: SEGMENTED_MULTI_KEY_LOOP(uint32_t); break;
            case PhysicalType::UINT64: SEGMENTED_MULTI_KEY_LOOP(uint64_t); break;
            default: break;
            }
            #undef SEGMENTED_MULTI_KEY_LOOP
            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }

        if (sink.segmented_direct_mode) {
            auto pki = col.probe_key_cols[0];
            input.data[pki].Flatten(n);
            auto &f0 = col.agg_funcs[0];
            auto ai = col.agg_input_cols[0];
            if (ai != DConstants::INVALID_INDEX && ai < input.ColumnCount()) input.data[ai].Flatten(n);

            auto ptype = input.data[pki].GetType().InternalType();
            auto kmin = sink.key_min;
            auto seg_shift = sink.segmented_shift;
            auto seg_mask = sink.segmented_mask;
            auto range = sink.key_range;
            bool pkfk = sink.all_bc_one;
            auto *active_bits = sink.segmented_active_bits.data();
            auto mark_active = [&](idx_t key_offset) {
                auto word = key_offset >> 6;
                auto bit = 1ULL << (key_offset & 63);
                if (!(active_bits[word] & bit)) {
                    active_bits[word] |= bit;
                    sink.segmented_active_keys.push_back(key_offset);
                }
            };

            #define SEGMENTED_KEY_LOOP(KTYPE, BODY) { \
                auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                for (idx_t r = 0; r < n; r++) { \
                    auto k = (idx_t)((int64_t)keys[r] - kmin); \
                    if (k >= range) continue; \
                    auto seg = k >> seg_shift; \
                    auto local = k & seg_mask; \
                    auto &count_seg = sink.segmented_build_counts[seg]; \
                    if (count_seg.empty()) continue; \
                    auto bcount = count_seg[local]; \
                    if (!bcount) continue; \
                    mark_active(k); \
                    BODY \
                } \
            }

            if (f0 == "COUNT" && ai == DConstants::INVALID_INDEX) {
                switch (ptype) {
                case PhysicalType::INT8:   SEGMENTED_KEY_LOOP(int8_t,   sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                case PhysicalType::INT16:  SEGMENTED_KEY_LOOP(int16_t,  sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                case PhysicalType::INT32:  SEGMENTED_KEY_LOOP(int32_t,  sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                case PhysicalType::INT64:  SEGMENTED_KEY_LOOP(int64_t,  sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                case PhysicalType::UINT8:  SEGMENTED_KEY_LOOP(uint8_t,  sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                case PhysicalType::UINT16: SEGMENTED_KEY_LOOP(uint16_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                case PhysicalType::UINT32: SEGMENTED_KEY_LOOP(uint32_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                case PhysicalType::UINT64: SEGMENTED_KEY_LOOP(uint64_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
                default: break;
                }
                #undef SEGMENTED_KEY_LOOP
                chunk.SetCardinality(0);
                return OperatorResultType::NEED_MORE_INPUT;
            }

            auto *validity = (ai != DConstants::INVALID_INDEX && ai < input.ColumnCount()) ?
                             FlatVector::Validity(input.data[ai]).GetData() : nullptr;
            if (f0 == "COUNT") {
                #define SEGMENTED_COUNT_COL_LOOP(KTYPE) { \
                    auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                    for (idx_t r = 0; r < n; r++) { \
                        auto k = (idx_t)((int64_t)keys[r] - kmin); \
                        if (k >= range) continue; \
                        auto seg = k >> seg_shift; \
                        auto local = k & seg_mask; \
                        auto &count_seg = sink.segmented_build_counts[seg]; \
                        if (count_seg.empty()) continue; \
                        auto bcount = count_seg[local]; \
                        if (!bcount) continue; \
                        if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                        mark_active(k); \
                        sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount; \
                    } \
                }
                switch (ptype) {
                case PhysicalType::INT8:   SEGMENTED_COUNT_COL_LOOP(int8_t); break;
                case PhysicalType::INT16:  SEGMENTED_COUNT_COL_LOOP(int16_t); break;
                case PhysicalType::INT32:  SEGMENTED_COUNT_COL_LOOP(int32_t); break;
                case PhysicalType::INT64:  SEGMENTED_COUNT_COL_LOOP(int64_t); break;
                case PhysicalType::UINT8:  SEGMENTED_COUNT_COL_LOOP(uint8_t); break;
                case PhysicalType::UINT16: SEGMENTED_COUNT_COL_LOOP(uint16_t); break;
                case PhysicalType::UINT32: SEGMENTED_COUNT_COL_LOOP(uint32_t); break;
                case PhysicalType::UINT64: SEGMENTED_COUNT_COL_LOOP(uint64_t); break;
                default: break;
                }
                #undef SEGMENTED_COUNT_COL_LOOP
                #undef SEGMENTED_KEY_LOOP
                chunk.SetCardinality(0);
                return OperatorResultType::NEED_MORE_INPUT;
            }

            auto vtype = input.data[ai].GetType().InternalType();
            #define SEGMENTED_NUMERIC_LOOP(KTYPE, VTYPE) { \
                auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]); \
                for (idx_t r = 0; r < n; r++) { \
                    auto k = (idx_t)((int64_t)keys[r] - kmin); \
                    if (k >= range) continue; \
                    auto seg = k >> seg_shift; \
                    auto local = k & seg_mask; \
                    auto &count_seg = sink.segmented_build_counts[seg]; \
                    if (count_seg.empty()) continue; \
                    auto bcount = count_seg[local]; \
                    if (!bcount) continue; \
                    if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                    mark_active(k); \
                    sink.segmented_sums[seg][local] += (double)vals[r] * (pkfk ? 1.0 : (double)bcount); \
                    if (f0 == "AVG") sink.segmented_avg_counts[seg][local] += pkfk ? 1.0 : (double)bcount; \
                } \
            }
            bool ran_typed = true;
            #define SEGMENTED_NUMERIC_KEY_SWITCH(VTYPE) \
                switch (ptype) { \
                case PhysicalType::INT8:   SEGMENTED_NUMERIC_LOOP(int8_t, VTYPE); break; \
                case PhysicalType::INT16:  SEGMENTED_NUMERIC_LOOP(int16_t, VTYPE); break; \
                case PhysicalType::INT32:  SEGMENTED_NUMERIC_LOOP(int32_t, VTYPE); break; \
                case PhysicalType::INT64:  SEGMENTED_NUMERIC_LOOP(int64_t, VTYPE); break; \
                case PhysicalType::UINT8:  SEGMENTED_NUMERIC_LOOP(uint8_t, VTYPE); break; \
                case PhysicalType::UINT16: SEGMENTED_NUMERIC_LOOP(uint16_t, VTYPE); break; \
                case PhysicalType::UINT32: SEGMENTED_NUMERIC_LOOP(uint32_t, VTYPE); break; \
                case PhysicalType::UINT64: SEGMENTED_NUMERIC_LOOP(uint64_t, VTYPE); break; \
                default: ran_typed = false; break; \
                }
            switch (vtype) {
            case PhysicalType::DOUBLE: SEGMENTED_NUMERIC_KEY_SWITCH(double); break;
            case PhysicalType::FLOAT:  SEGMENTED_NUMERIC_KEY_SWITCH(float); break;
            case PhysicalType::INT64:  SEGMENTED_NUMERIC_KEY_SWITCH(int64_t); break;
            case PhysicalType::INT32:  SEGMENTED_NUMERIC_KEY_SWITCH(int32_t); break;
            case PhysicalType::INT16:  SEGMENTED_NUMERIC_KEY_SWITCH(int16_t); break;
            case PhysicalType::INT8:   SEGMENTED_NUMERIC_KEY_SWITCH(int8_t); break;
            case PhysicalType::UINT64: SEGMENTED_NUMERIC_KEY_SWITCH(uint64_t); break;
            case PhysicalType::UINT32: SEGMENTED_NUMERIC_KEY_SWITCH(uint32_t); break;
            case PhysicalType::UINT16: SEGMENTED_NUMERIC_KEY_SWITCH(uint16_t); break;
            case PhysicalType::UINT8:  SEGMENTED_NUMERIC_KEY_SWITCH(uint8_t); break;
            default: ran_typed = false; break;
            }
            #undef SEGMENTED_NUMERIC_KEY_SWITCH
            #undef SEGMENTED_NUMERIC_LOOP
            #undef SEGMENTED_KEY_LOOP
            if (!ran_typed) {
                chunk.SetCardinality(0);
                return OperatorResultType::NEED_MORE_INPUT;
            }
            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }

        if (sink.direct_mode) {
            auto pki = col.probe_key_cols[0];
            input.data[pki].Flatten(n);
            for (auto i : col.agg_input_cols)
                if (i != DConstants::INVALID_INDEX && i < input.ColumnCount()) input.data[i].Flatten(n);

            auto ptype = input.data[pki].GetType().InternalType();
            auto kmin = sink.key_min;
            auto krange = sink.key_range;
            auto *bc = sink.build_counts.data();
            auto *sums = sink.direct_sums.data();

            // ── Multi-aggregate fusion: single pass over rows ──
            // Extract key once per row, accumulate ALL aggregates in one loop.
            // Reduces key array traversals from na×n to n.

            // Check if all agg inputs are DOUBLE (or COUNT(*)) for the fast fused path
            bool all_sum_count_double = true;
            bool has_minmax = sink.has_min_max;
            for (idx_t a = 0; a < na; a++) {
                auto &f = col.agg_funcs[a];
                auto ai = col.agg_input_cols[a];
                if (f == "COUNT" && ai == DConstants::INVALID_INDEX) continue; // COUNT(*)
                if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;
                if (f == "MIN" || f == "MAX") continue; // handled separately
                if (input.data[ai].GetType().InternalType() != PhysicalType::DOUBLE) {
                    all_sum_count_double = false; break;
                }
            }

            // Collect per-aggregate value pointers and validity masks upfront
            struct AggSlot {
                enum Kind { SUM_VAL, AVG_VAL, COUNT_STAR, COUNT_COL, MIN_VAL, MAX_VAL, SKIP } kind;
                const double *vals = nullptr;
                const uint64_t *validity = nullptr;
            };
            auto *avg_counts = sink.has_avg ? sink.direct_counts.data() : nullptr;
            vector<AggSlot> agg_slots(na);
            for (idx_t a = 0; a < na; a++) {
                auto &f = col.agg_funcs[a];
                auto ai = col.agg_input_cols[a];
                if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
                    agg_slots[a].kind = AggSlot::COUNT_STAR;
                } else if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) {
                    agg_slots[a].kind = AggSlot::SKIP;
                } else if (f == "AVG") {
                    agg_slots[a].kind = AggSlot::AVG_VAL;
                    if (all_sum_count_double)
                        agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
                    agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
                } else if (f == "SUM") {
                    agg_slots[a].kind = AggSlot::SUM_VAL;
                    if (all_sum_count_double)
                        agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
                    agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
                } else if (f == "COUNT") {
                    agg_slots[a].kind = AggSlot::COUNT_COL;
                    agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
                } else if (f == "MIN") {
                    agg_slots[a].kind = AggSlot::MIN_VAL;
                    if (all_sum_count_double)
                        agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
                    agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
                } else if (f == "MAX") {
                    agg_slots[a].kind = AggSlot::MAX_VAL;
                    if (all_sum_count_double)
                        agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
                    agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
                } else {
                    agg_slots[a].kind = AggSlot::SKIP;
                }
            }

            auto *mins = has_minmax ? sink.direct_mins.data() : nullptr;
            auto *maxs = has_minmax ? sink.direct_maxs.data() : nullptr;
            auto *has_arr = has_minmax ? sink.direct_has.data() : nullptr;

            auto is_int_key = (ptype == PhysicalType::INT32 || ptype == PhysicalType::INT64 ||
                               ptype == PhysicalType::UINT32 || ptype == PhysicalType::UINT64 ||
                               ptype == PhysicalType::UINT16 || ptype == PhysicalType::UINT8);
            // ═══ UNGROUPED SCALAR PATH ═══
            // For ungrouped aggregates, accumulate directly into running scalars.
            // Avoids writing to per-key arrays and the O(krange) emit reduction.
            if (col.group_cols.empty() && all_sum_count_double && is_int_key && !sink.ungrouped_sum.empty()) {
                // Extract keys and build counts
                #define UNGROUPED_EXTRACT(KTYPE) { \
                    auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                    for (idx_t r = 0; r < n; r++) { \
                        auto k = (idx_t)((int64_t)keys[r] - kmin); \
                        if (k < krange && bc[k] > 0) { \
                            double bcount = (double)bc[k]; \
                            for (idx_t a = 0; a < na; a++) { \
                                auto &slot = agg_slots[a]; \
                                switch (slot.kind) { \
                                case AggSlot::COUNT_STAR: \
                                    sink.ungrouped_sum[a] += bcount; break; \
                                case AggSlot::SUM_VAL: { \
                                    auto *v = slot.validity; \
                                    if (!v || ((v[r/64] >> (r%64)) & 1)) \
                                        sink.ungrouped_sum[a] += slot.vals[r] * bcount; \
                                    break; } \
                                case AggSlot::AVG_VAL: { \
                                    auto *v = slot.validity; \
                                    if (!v || ((v[r/64] >> (r%64)) & 1)) { \
                                        sink.ungrouped_sum[a] += slot.vals[r] * bcount; \
                                        sink.ungrouped_count[a] += bcount; \
                                    } break; } \
                                case AggSlot::COUNT_COL: { \
                                    auto *v = slot.validity; \
                                    if (!v || ((v[r/64] >> (r%64)) & 1)) \
                                        sink.ungrouped_sum[a] += bcount; \
                                    break; } \
                                case AggSlot::MIN_VAL: { \
                                    auto *v = slot.validity; \
                                    if (!v || ((v[r/64] >> (r%64)) & 1)) { \
                                        double dv = slot.vals[r]; \
                                        if (!sink.ungrouped_has[a] || dv < sink.ungrouped_min[a]) \
                                            { sink.ungrouped_min[a] = dv; sink.ungrouped_has[a] = 1; } \
                                    } break; } \
                                case AggSlot::MAX_VAL: { \
                                    auto *v = slot.validity; \
                                    if (!v || ((v[r/64] >> (r%64)) & 1)) { \
                                        double dv = slot.vals[r]; \
                                        if (!sink.ungrouped_has[a] || dv > sink.ungrouped_max[a]) \
                                            { sink.ungrouped_max[a] = dv; sink.ungrouped_has[a] = 1; } \
                                    } break; } \
                                default: break; \
                                } \
                            } \
                        } \
                    } \
                }
                switch (ptype) {
                case PhysicalType::INT32:  UNGROUPED_EXTRACT(int32_t); break;
                case PhysicalType::INT64:  UNGROUPED_EXTRACT(int64_t); break;
                case PhysicalType::UINT32: UNGROUPED_EXTRACT(uint32_t); break;
                case PhysicalType::UINT64: UNGROUPED_EXTRACT(uint64_t); break;
                case PhysicalType::UINT16: UNGROUPED_EXTRACT(uint16_t); break;
                case PhysicalType::UINT8:  UNGROUPED_EXTRACT(uint8_t); break;
                default: break;
                }
                #undef UNGROUPED_EXTRACT

                // Handle build-side aggregates for ungrouped:
                // For each matched probe row, add the build-side value to running scalar.
                if (!sink.direct_build_sums.empty()) {
                    idx_t ba = 0;
                    for (idx_t a = 0; a < na; a++) {
                        if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                        auto &f = col.agg_funcs[a];
                        auto *bsums = sink.direct_build_sums.data() + ba * krange;
                        auto *bcnts = sink.direct_build_counts.data() + ba * krange;
                        auto *bhas = sink.direct_build_has.empty() ? nullptr : sink.direct_build_has.data() + ba * krange;
                        // For each probe row with a matching key, accumulate build-side value
                        #define UNGROUPED_BUILD_AGG(KTYPE) { \
                            auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                            for (idx_t r = 0; r < n; r++) { \
                                auto k = (idx_t)((int64_t)keys[r] - kmin); \
                                if (k >= krange || !bc[k]) continue; \
                                if (f == "SUM") sink.ungrouped_sum[a] += bsums[k]; \
                                else if (f == "AVG") { sink.ungrouped_sum[a] += bsums[k]; sink.ungrouped_count[a] += bcnts[k]; } \
                                else if (f == "COUNT") sink.ungrouped_sum[a] += bcnts[k]; \
                                else if (f == "MIN" && bhas && bhas[k]) { \
                                    auto bv = sink.direct_build_mins[ba * krange + k]; \
                                    if (!sink.ungrouped_has[a] || bv < sink.ungrouped_min[a]) \
                                        { sink.ungrouped_min[a] = bv; sink.ungrouped_has[a] = 1; } \
                                } else if (f == "MAX" && bhas && bhas[k]) { \
                                    auto bv = sink.direct_build_maxs[ba * krange + k]; \
                                    if (!sink.ungrouped_has[a] || bv > sink.ungrouped_max[a]) \
                                        { sink.ungrouped_max[a] = bv; sink.ungrouped_has[a] = 1; } \
                                } \
                            } \
                        }
                        switch (ptype) {
                        case PhysicalType::INT32:  UNGROUPED_BUILD_AGG(int32_t); break;
                        case PhysicalType::INT64:  UNGROUPED_BUILD_AGG(int64_t); break;
                        case PhysicalType::UINT32: UNGROUPED_BUILD_AGG(uint32_t); break;
                        case PhysicalType::UINT64: UNGROUPED_BUILD_AGG(uint64_t); break;
                        case PhysicalType::UINT16: UNGROUPED_BUILD_AGG(uint16_t); break;
                        case PhysicalType::UINT8:  UNGROUPED_BUILD_AGG(uint8_t); break;
                        default: break;
                        }
                        #undef UNGROUPED_BUILD_AGG
                        ba++;
                    }
                }

                chunk.SetCardinality(0);
                return OperatorResultType::NEED_MORE_INPUT;
            }

            if (all_sum_count_double && is_int_key) {
                // ═══ FUSED FAST PATH: INT32/INT64 keys, DOUBLE values ═══
                // Single loop over rows: extract key once, accumulate all aggregates.
                // Column-major layout: sums[agg * krange + key]
                // Phase 1: Extract keys and build_counts into scratch buffers (once)
                // Reuse pre-allocated buffers on sink state to avoid per-chunk allocation
                sink.direct_key_buf.resize(n);
                auto &key_buf = sink.direct_key_buf;
                bool pkfk = sink.all_bc_one; // PK/FK: all build counts == 1
                auto &bc_buf = sink.direct_bc_buf;
                auto track_active_keys = sink.track_active_keys;
                auto *seen_keys = track_active_keys ? sink.direct_key_seen.data() : nullptr;
                if (!pkfk) { bc_buf.resize(n); std::fill(bc_buf.begin(), bc_buf.end(), 0.0); }
                #define EXTRACT_KEYS(KTYPE) { \
                    auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                    if (pkfk) { \
                        for (idx_t r = 0; r < n; r++) { \
                            auto k = (idx_t)((int64_t)keys[r] - kmin); \
                            key_buf[r] = k; \
                            if (track_active_keys && k < krange && !seen_keys[k]) { seen_keys[k] = 1; sink.direct_active_keys.push_back(k); } \
                        } \
                    } else { \
                        for (idx_t r = 0; r < n; r++) { \
                            auto k = (idx_t)((int64_t)keys[r] - kmin); \
                            key_buf[r] = k; \
                            bc_buf[r] = (k < krange) ? (double)bc[k] : 0.0; \
                            if (track_active_keys && bc_buf[r] != 0.0 && !seen_keys[k]) { seen_keys[k] = 1; sink.direct_active_keys.push_back(k); } \
                        } \
                    } \
                }
                switch (ptype) {
                case PhysicalType::INT32:  EXTRACT_KEYS(int32_t); break;
                case PhysicalType::INT64:  EXTRACT_KEYS(int64_t); break;
                case PhysicalType::UINT32: EXTRACT_KEYS(uint32_t); break;
                case PhysicalType::UINT64: EXTRACT_KEYS(uint64_t); break;
                case PhysicalType::UINT16: EXTRACT_KEYS(uint16_t); break;
                case PhysicalType::UINT8:  EXTRACT_KEYS(uint8_t); break;
                default: break;
                }
                #undef EXTRACT_KEYS

                // Phase 2: Per-aggregate accumulation into contiguous arrays
                // PK/FK fast path: skip bc_buf multiply (all counts == 1)
                for (idx_t a = 0; a < na; a++) {
                    auto &slot = agg_slots[a];
                    double *agg_sums = sums + a * krange;
                    switch (slot.kind) {
                    case AggSlot::COUNT_STAR:
                        if (pkfk) {
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (k < krange) agg_sums[k] += 1.0;
                            }
                        } else {
                            for (idx_t r = 0; r < n; r++) {
                                if (bc_buf[r] != 0.0) agg_sums[key_buf[r]] += bc_buf[r];
                            }
                        }
                        break;
                    case AggSlot::SUM_VAL: {
                        auto *v = slot.validity;
                        if (pkfk) {
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (k >= krange) continue;
                                if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                                agg_sums[k] += slot.vals[r];
                            }
                        } else {
                            for (idx_t r = 0; r < n; r++) {
                                if (bc_buf[r] == 0.0) continue;
                                if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                                agg_sums[key_buf[r]] += slot.vals[r] * bc_buf[r];
                            }
                        }
                        break;
                    }
                    case AggSlot::AVG_VAL: {
                        double *agg_counts = avg_counts + a * krange;
                        auto *v = slot.validity;
                        if (pkfk) {
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (k >= krange) continue;
                                if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                                agg_sums[k] += slot.vals[r];
                                agg_counts[k] += 1.0;
                            }
                        } else {
                            for (idx_t r = 0; r < n; r++) {
                                if (bc_buf[r] == 0.0) continue;
                                if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                                agg_sums[key_buf[r]] += slot.vals[r] * bc_buf[r];
                                agg_counts[key_buf[r]] += bc_buf[r];
                            }
                        }
                        break;
                    }
                    case AggSlot::COUNT_COL: {
                        auto *v = slot.validity;
                        if (pkfk) {
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (k >= krange) continue;
                                if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                                agg_sums[k] += 1.0;
                            }
                        } else {
                            for (idx_t r = 0; r < n; r++) {
                                if (bc_buf[r] == 0.0) continue;
                                if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                                agg_sums[key_buf[r]] += bc_buf[r];
                            }
                        }
                        break;
                    }
                    case AggSlot::MIN_VAL: {
                        double *agg_mins = mins + a * krange;
                        uint8_t *agg_has = has_arr + a * krange;
                        auto *v = slot.validity;
                        for (idx_t r = 0; r < n; r++) {
                            auto k = key_buf[r];
                            if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                            if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                            auto dv = slot.vals[r];
                            if (!agg_has[k] || dv < agg_mins[k]) { agg_mins[k] = dv; agg_has[k] = 1; }
                        }
                        break;
                    }
                    case AggSlot::MAX_VAL: {
                        double *agg_maxs = maxs + a * krange;
                        uint8_t *agg_has = has_arr + a * krange;
                        auto *v = slot.validity;
                        for (idx_t r = 0; r < n; r++) {
                            auto k = key_buf[r];
                            if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                            if (v && !((v[r/64] >> (r%64)) & 1)) continue;
                            auto dv = slot.vals[r];
                            if (!agg_has[k] || dv > agg_maxs[k]) { agg_maxs[k] = dv; agg_has[k] = 1; }
                        }
                        break;
                    }
                    default: break;
                    }
                } // end per-aggregate loop

                // Phase 3: Build-side aggregate accumulation.
                // For each build-side agg, add the pre-accumulated build value per key.
                if (!sink.direct_build_sums.empty()) {
                    idx_t ba = 0;
                    for (idx_t a = 0; a < na; a++) {
                        if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                        auto &f = col.agg_funcs[a];
                        double *agg_sums_a = sums + a * krange;
                        if (f == "SUM") {
                            double *bsums = sink.direct_build_sums.data() + ba * krange;
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                                agg_sums_a[k] += bsums[k];
                            }
                        } else if (f == "MIN" && mins) {
                            double *bmins = sink.direct_build_mins.data() + ba * krange;
                            uint8_t *bhas = sink.direct_build_has.data() + ba * krange;
                            uint8_t *agg_has = has_arr + a * krange;
                            double *agg_mins = mins + a * krange;
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                                if (bhas[k] && (!agg_has[k] || bmins[k] < agg_mins[k]))
                                    { agg_mins[k] = bmins[k]; agg_has[k] = 1; }
                            }
                        } else if (f == "MAX" && maxs) {
                            double *bmaxs = sink.direct_build_maxs.data() + ba * krange;
                            uint8_t *bhas = sink.direct_build_has.data() + ba * krange;
                            uint8_t *agg_has = has_arr + a * krange;
                            double *agg_maxs = maxs + a * krange;
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                                if (bhas[k] && (!agg_has[k] || bmaxs[k] > agg_maxs[k]))
                                    { agg_maxs[k] = bmaxs[k]; agg_has[k] = 1; }
                            }
                        } else if (f == "COUNT") {
                            double *bcnts = sink.direct_build_counts.data() + ba * krange;
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                                agg_sums_a[k] += bcnts[k];
                            }
                        } else if (f == "AVG" && avg_counts) {
                            double *bsums = sink.direct_build_sums.data() + ba * krange;
                            double *bcnts = sink.direct_build_counts.data() + ba * krange;
                            double *agg_counts_a = avg_counts + a * krange;
                            for (idx_t r = 0; r < n; r++) {
                                auto k = key_buf[r];
                                if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                                agg_sums_a[k] += bsums[k];
                                agg_counts_a[k] += bcnts[k];
                            }
                        }
                        ba++;
                    }
                }
            } else {
                // ═══ PER-AGGREGATE FALLBACK ═══
                // Handles non-DOUBLE types, mixed types, and edge cases.
                for (idx_t a = 0; a < na; a++) {
                    auto ai = col.agg_input_cols[a];
                    auto &f = col.agg_funcs[a];

                    if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
                        #define DIRECT_COUNT_LOOP(TYPE) { \
                            auto *keys = FlatVector::GetData<TYPE>(input.data[pki]); \
                            double *agg_s = sums + a * krange; \
                            for (idx_t r = 0; r < n; r++) { \
                                auto k = (idx_t)((int64_t)keys[r] - kmin); \
                                if (k < krange) agg_s[k] += (double)bc[k]; \
                            } \
                        }
                        switch (ptype) {
                        case PhysicalType::INT8:   DIRECT_COUNT_LOOP(int8_t); break;
                        case PhysicalType::INT16:  DIRECT_COUNT_LOOP(int16_t); break;
                        case PhysicalType::INT32:  DIRECT_COUNT_LOOP(int32_t); break;
                        case PhysicalType::INT64:  DIRECT_COUNT_LOOP(int64_t); break;
                        case PhysicalType::UINT8:  DIRECT_COUNT_LOOP(uint8_t); break;
                        case PhysicalType::UINT16: DIRECT_COUNT_LOOP(uint16_t); break;
                        case PhysicalType::UINT32: DIRECT_COUNT_LOOP(uint32_t); break;
                        case PhysicalType::UINT64: DIRECT_COUNT_LOOP(uint64_t); break;
                        default: break;
                        }
                        #undef DIRECT_COUNT_LOOP
                        continue;
                    }
                    // Build-side aggs have ai == INVALID_INDEX but are handled via
                    // direct_build_sums (set in Finalize from build_agg_map)
                    bool is_build_agg = (col.agg_on_build.size() > a && col.agg_on_build[a]);
                    if (is_build_agg && !sink.direct_build_sums.empty()) {
                        idx_t ba = 0;
                        for (idx_t i = 0; i < a; i++)
                            if (col.agg_on_build.size() > i && col.agg_on_build[i]) ba++;
                        double *bsums = sink.direct_build_sums.data() + ba * krange;
                        double *agg_s = sums + a * krange;
                        // Use typed key extraction instead of GetValue()
                        auto *build_has = sink.direct_build_has.empty() ? nullptr : sink.direct_build_has.data() + ba * krange;
                        #define DIRECT_BUILD_AGG_LOOP(KTYPE) { \
                            auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                            for (idx_t r = 0; r < n; r++) { \
                                auto k = (idx_t)((int64_t)keys[r] - kmin); \
                                if (k >= krange || !bc[k]) continue; \
                                if (f == "SUM" || f == "AVG") agg_s[k] += bsums[k]; \
                                else if (f == "COUNT") agg_s[k] += sink.direct_build_counts[ba * krange + k]; \
                                else if (f == "MIN" && has_minmax && build_has) { \
                                    if (!build_has[k]) continue; \
                                    auto bv = sink.direct_build_mins[ba * krange + k]; \
                                    auto s = a * krange + k; \
                                    if (!has_arr[s] || bv < mins[s]) { mins[s] = bv; has_arr[s] = 1; } \
                                } else if (f == "MAX" && has_minmax && build_has) { \
                                    if (!build_has[k]) continue; \
                                    auto bv = sink.direct_build_maxs[ba * krange + k]; \
                                    auto s = a * krange + k; \
                                    if (!has_arr[s] || bv > maxs[s]) { maxs[s] = bv; has_arr[s] = 1; } \
                                } \
                            } \
                        }
                        switch (ptype) {
                        case PhysicalType::INT8:   DIRECT_BUILD_AGG_LOOP(int8_t); break;
                        case PhysicalType::INT16:  DIRECT_BUILD_AGG_LOOP(int16_t); break;
                        case PhysicalType::INT32:  DIRECT_BUILD_AGG_LOOP(int32_t); break;
                        case PhysicalType::INT64:  DIRECT_BUILD_AGG_LOOP(int64_t); break;
                        case PhysicalType::UINT8:  DIRECT_BUILD_AGG_LOOP(uint8_t); break;
                        case PhysicalType::UINT16: DIRECT_BUILD_AGG_LOOP(uint16_t); break;
                        case PhysicalType::UINT32: DIRECT_BUILD_AGG_LOOP(uint32_t); break;
                        case PhysicalType::UINT64: DIRECT_BUILD_AGG_LOOP(uint64_t); break;
                        default: {
                            for (idx_t r = 0; r < n; r++) {
                                auto kv = input.data[pki].GetValue(r);
                                if (kv.IsNull()) continue;
                                auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                                if (k >= krange || !bc[k]) continue;
                                if (f == "SUM" || f == "AVG") agg_s[k] += bsums[k];
                                else if (f == "COUNT") agg_s[k] += sink.direct_build_counts[ba * krange + k];
                                else if (f == "MIN" && has_minmax && build_has) {
                                    if (!build_has[k]) continue;
                                    auto bv = sink.direct_build_mins[ba * krange + k];
                                    auto s = a * krange + k;
                                    if (!has_arr[s] || bv < mins[s]) { mins[s] = bv; has_arr[s] = 1; }
                                } else if (f == "MAX" && has_minmax && build_has) {
                                    if (!build_has[k]) continue;
                                    auto bv = sink.direct_build_maxs[ba * krange + k];
                                    auto s = a * krange + k;
                                    if (!has_arr[s] || bv > maxs[s]) { maxs[s] = bv; has_arr[s] = 1; }
                                }
                            }
                            break;
                        }}
                        #undef DIRECT_BUILD_AGG_LOOP
                        continue;
                    }
                    if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;

                    if (f == "SUM" || f == "AVG" || f == "COUNT") {
                        auto vtype = input.data[ai].GetType().InternalType();
                        auto *validity = FlatVector::Validity(input.data[ai]).GetData();
                        bool is_avg = (f == "AVG");
                        auto *dcounts = is_avg ? sink.direct_counts.data() : nullptr;
                        #define DIRECT_SUM_LOOP(KTYPE, VTYPE) { \
                            auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                            auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]); \
                            double *agg_s = sums + a * krange; \
                            double *agg_c = dcounts ? dcounts + a * krange : nullptr; \
                            for (idx_t r = 0; r < n; r++) { \
                                auto k = (idx_t)((int64_t)keys[r] - kmin); \
                                if (k >= krange) continue; \
                                if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                                agg_s[k] += (double)vals[r] * (double)bc[k]; \
                                if (agg_c) agg_c[k] += (double)bc[k]; \
                            } \
                        }
                        bool ran_typed = true;
                        #define DIRECT_SUM_KEY_SWITCH(VTYPE) \
                            switch (ptype) { \
                            case PhysicalType::INT8:   DIRECT_SUM_LOOP(int8_t, VTYPE); break; \
                            case PhysicalType::INT16:  DIRECT_SUM_LOOP(int16_t, VTYPE); break; \
                            case PhysicalType::INT32:  DIRECT_SUM_LOOP(int32_t, VTYPE); break; \
                            case PhysicalType::INT64:  DIRECT_SUM_LOOP(int64_t, VTYPE); break; \
                            case PhysicalType::UINT8:  DIRECT_SUM_LOOP(uint8_t, VTYPE); break; \
                            case PhysicalType::UINT16: DIRECT_SUM_LOOP(uint16_t, VTYPE); break; \
                            case PhysicalType::UINT32: DIRECT_SUM_LOOP(uint32_t, VTYPE); break; \
                            case PhysicalType::UINT64: DIRECT_SUM_LOOP(uint64_t, VTYPE); break; \
                            default: ran_typed = false; break; \
                            }
                        switch (vtype) {
                        case PhysicalType::DOUBLE: DIRECT_SUM_KEY_SWITCH(double); break;
                        case PhysicalType::FLOAT:  DIRECT_SUM_KEY_SWITCH(float); break;
                        case PhysicalType::INT64:  DIRECT_SUM_KEY_SWITCH(int64_t); break;
                        case PhysicalType::INT32:  DIRECT_SUM_KEY_SWITCH(int32_t); break;
                        case PhysicalType::INT16:  DIRECT_SUM_KEY_SWITCH(int16_t); break;
                        case PhysicalType::INT8:   DIRECT_SUM_KEY_SWITCH(int8_t); break;
                        case PhysicalType::UINT64: DIRECT_SUM_KEY_SWITCH(uint64_t); break;
                        case PhysicalType::UINT32: DIRECT_SUM_KEY_SWITCH(uint32_t); break;
                        case PhysicalType::UINT16: DIRECT_SUM_KEY_SWITCH(uint16_t); break;
                        case PhysicalType::UINT8:  DIRECT_SUM_KEY_SWITCH(uint8_t); break;
                        default: ran_typed = false; break;
                        }
                        #undef DIRECT_SUM_KEY_SWITCH
                        if (!ran_typed) {
                            double *agg_s = sums + a * krange;
                            double *agg_c = dcounts ? dcounts + a * krange : nullptr;
                            for (idx_t r = 0; r < n; r++) {
                                auto kv = input.data[pki].GetValue(r);
                                if (kv.IsNull()) continue;
                                auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                                if (k >= krange || !bc[k]) continue;
                                auto v = input.data[ai].GetValue(r);
                                if (v.IsNull()) continue;
                                agg_s[k] += v.GetValue<double>() * (double)bc[k];
                                if (agg_c) agg_c[k] += (double)bc[k];
                            }
                        }
                        #undef DIRECT_SUM_LOOP
                    } else if (f == "MIN" || f == "MAX") {
                        bool is_min = (f == "MIN");
                        double *agg_m = is_min ? (mins + a * krange) : (maxs + a * krange);
                        uint8_t *agg_h = has_arr + a * krange;
                        auto vtype = input.data[ai].GetType().InternalType();
                        auto *mm_validity = FlatVector::Validity(input.data[ai]).GetData();
                        #define DIRECT_MINMAX_LOOP(KTYPE, VTYPE) { \
                            auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                            auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]); \
                            for (idx_t r = 0; r < n; r++) { \
                                auto k = (idx_t)((int64_t)keys[r] - kmin); \
                                if (k >= krange || !bc[k]) continue; \
                                if (mm_validity && !((mm_validity[r/64] >> (r%64)) & 1)) continue; \
                                if (has_minmax) { \
                                    double dv = (double)vals[r]; \
                                    if (is_min) { if (!agg_h[k] || dv < agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; } } \
                                    else { if (!agg_h[k] || dv > agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; } } \
                                } \
                            } \
                        }
                        if (has_minmax) {
                            bool ran_typed = true;
                            #define DIRECT_MINMAX_KEY_SWITCH(VTYPE) \
                                switch (ptype) { \
                                case PhysicalType::INT8:   DIRECT_MINMAX_LOOP(int8_t, VTYPE); break; \
                                case PhysicalType::INT16:  DIRECT_MINMAX_LOOP(int16_t, VTYPE); break; \
                                case PhysicalType::INT32:  DIRECT_MINMAX_LOOP(int32_t, VTYPE); break; \
                                case PhysicalType::INT64:  DIRECT_MINMAX_LOOP(int64_t, VTYPE); break; \
                                case PhysicalType::UINT8:  DIRECT_MINMAX_LOOP(uint8_t, VTYPE); break; \
                                case PhysicalType::UINT16: DIRECT_MINMAX_LOOP(uint16_t, VTYPE); break; \
                                case PhysicalType::UINT32: DIRECT_MINMAX_LOOP(uint32_t, VTYPE); break; \
                                case PhysicalType::UINT64: DIRECT_MINMAX_LOOP(uint64_t, VTYPE); break; \
                                default: ran_typed = false; break; \
                                }
                            switch (vtype) {
                            case PhysicalType::DOUBLE: DIRECT_MINMAX_KEY_SWITCH(double); break;
                            case PhysicalType::FLOAT:  DIRECT_MINMAX_KEY_SWITCH(float); break;
                            case PhysicalType::INT64:  DIRECT_MINMAX_KEY_SWITCH(int64_t); break;
                            case PhysicalType::INT32:  DIRECT_MINMAX_KEY_SWITCH(int32_t); break;
                            case PhysicalType::INT16:  DIRECT_MINMAX_KEY_SWITCH(int16_t); break;
                            case PhysicalType::INT8:   DIRECT_MINMAX_KEY_SWITCH(int8_t); break;
                            case PhysicalType::UINT64: DIRECT_MINMAX_KEY_SWITCH(uint64_t); break;
                            case PhysicalType::UINT32: DIRECT_MINMAX_KEY_SWITCH(uint32_t); break;
                            case PhysicalType::UINT16: DIRECT_MINMAX_KEY_SWITCH(uint16_t); break;
                            case PhysicalType::UINT8:  DIRECT_MINMAX_KEY_SWITCH(uint8_t); break;
                            default: ran_typed = false; break;
                            }
                            #undef DIRECT_MINMAX_KEY_SWITCH
                            if (!ran_typed) {
                                for (idx_t r = 0; r < n; r++) {
                                    auto kv = input.data[pki].GetValue(r);
                                    if (kv.IsNull()) continue;
                                    auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                                    if (k >= krange || !bc[k]) continue;
                                    auto v = input.data[ai].GetValue(r);
                                    if (v.IsNull()) continue;
                                    auto dv = v.GetValue<double>();
                                    if (is_min) { if (!agg_h[k] || dv < agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; } }
                                    else { if (!agg_h[k] || dv > agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; } }
                                }
                            }
                        }
                        #undef DIRECT_MINMAX_LOOP
                    }
                }
            }

            // Store group values on first chunk (for output).
            // Skip for ungrouped aggregates and when group_is_key.
            if (!sink.group_is_key && !col.group_cols.empty()) {
                #define DIRECT_GROUP_CAPTURE(KTYPE) { \
                    auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]); \
                    for (idx_t r = 0; r < n; r++) { \
                        auto k = (idx_t)((int64_t)keys[r] - kmin); \
                        if (k >= krange || !bc[k] || sink.direct_group_init[k]) continue; \
                        sink.direct_group_init[k] = true; \
                        for (auto gi : col.group_cols) sink.direct_group_vals[k].push_back(input.data[gi].GetValue(r)); \
                    } \
                }
                switch (ptype) {
                case PhysicalType::INT8:   DIRECT_GROUP_CAPTURE(int8_t); break;
                case PhysicalType::INT16:  DIRECT_GROUP_CAPTURE(int16_t); break;
                case PhysicalType::INT32:  DIRECT_GROUP_CAPTURE(int32_t); break;
                case PhysicalType::INT64:  DIRECT_GROUP_CAPTURE(int64_t); break;
                case PhysicalType::UINT8:  DIRECT_GROUP_CAPTURE(uint8_t); break;
                case PhysicalType::UINT16: DIRECT_GROUP_CAPTURE(uint16_t); break;
                case PhysicalType::UINT32: DIRECT_GROUP_CAPTURE(uint32_t); break;
                case PhysicalType::UINT64: DIRECT_GROUP_CAPTURE(uint64_t); break;
                default:
                    for (idx_t r = 0; r < n; r++) {
                        auto kv = input.data[pki].GetValue(r);
                        if (kv.IsNull()) continue;
                        auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                        if (k >= krange || !bc[k] || sink.direct_group_init[k]) continue;
                        sink.direct_group_init[k] = true;
                        for (auto gi : col.group_cols) sink.direct_group_vals[k].push_back(input.data[gi].GetValue(r));
                    }
                    break;
                }
                #undef DIRECT_GROUP_CAPTURE
            }

            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }

        // ═══════════════════════════════════════════════════
        // HASH MODE: fallback for non-integer or large-range keys
        // ═══════════════════════════════════════════════════

        // Flatten probe key + group + agg cols
        for (auto i : col.probe_key_cols) input.data[i].Flatten(n);
        for (auto i : col.group_cols) if (i < input.ColumnCount()) input.data[i].Flatten(n);
        for (auto i : col.agg_input_cols) if (i!=DConstants::INVALID_INDEX && i<input.ColumnCount()) input.data[i].Flatten(n);

        Vector hv(LogicalType::HASH, n); hv.Flatten(n);
        VectorOperations::Hash(input.data[col.probe_key_cols[0]], hv, n);
        for (idx_t i=1;i<col.probe_key_cols.size();i++) VectorOperations::CombineHash(hv, input.data[col.probe_key_cols[i]], n);
        auto h = FlatVector::GetData<hash_t>(hv);
        ApplyAggJoinTestHashBits(h, n);

        // Hash group cols for result HT key
        // Optimization: if group_cols == probe_key_cols, reuse probe hash
        bool same_keys = (col.group_cols.size() == col.probe_key_cols.size());
        if (same_keys) {
            for (idx_t i = 0; i < col.group_cols.size(); i++) {
                if (col.group_cols[i] != col.probe_key_cols[i]) { same_keys = false; break; }
            }
        }
        if (sink.build_slot_hash_mode && same_keys) {
            auto *bf = sink.bloom_filter.empty() ? nullptr : sink.bloom_filter.data();
            auto bf_mask = sink.bloom_mask;
            auto *build_base = sink.build_ht.buckets.data();
            sink.probe_bc.resize(n);
            sink.probe_slots.resize(n);
            std::fill(sink.probe_bc.begin(), sink.probe_bc.end(), 0.0);
            std::fill(sink.probe_slots.begin(), sink.probe_slots.end(), DConstants::INVALID_INDEX);
            auto &row_bc = sink.probe_bc;
            auto &row_slots = sink.probe_slots;
            idx_t match_count = 0;
            for (idx_t r = 0; r < n; r++) {
                if (bf) {
                    auto bh1 = h[r];
                    auto bh2 = (bh1 >> 17) | (bh1 << 47);
                    if (!(bf[(bh1 & bf_mask) / 64] & (1ULL << ((bh1 & bf_mask) % 64))) ||
                        !(bf[(bh2 & bf_mask) / 64] & (1ULL << ((bh2 & bf_mask) % 64)))) {
                        sink.bloom_prefilter_skips++;
                        continue;
                    }
                }
                if (r + 4 < n) sink.build_ht.Prefetch(h[r + 4]);
                auto *build = sink.build_ht.Find(h[r], [&](const BuildEntry &b) {
                    return BuildEntryMatches(b, input, col.probe_key_cols, r);
                });
                if (!build) continue;
                bool has_null = false;
                for (auto ki : col.probe_key_cols) {
                    if (FlatVector::IsNull(input.data[ki], r)) { has_null = true; break; }
                }
                if (has_null) continue;
                auto slot = (idx_t)(build - build_base);
                row_slots[r] = slot;
                row_bc[r] = (double)build->count;
                sink.build_slot_seen[slot] = 1;
                match_count++;
            }
            sink.hash_match_rows += match_count;
            if (!match_count) { chunk.SetCardinality(0); return OperatorResultType::NEED_MORE_INPUT; }

            // Fused single-pass path for the common grouped single-key same-key
            // fallback: avoids rescanning the same probe rows once per aggregate.
            bool all_fused_double = true;
            struct BuildSlotAggSlot {
                enum Kind { SUM_VAL, AVG_VAL, COUNT_STAR, COUNT_COL, MIN_VAL, MAX_VAL, SKIP } kind;
                const double *vals = nullptr;
                const uint64_t *validity = nullptr;
            };
            vector<BuildSlotAggSlot> agg_slots(na);
            for (idx_t a = 0; a < na; a++) {
                auto &slot = agg_slots[a];
                auto ai = col.agg_input_cols[a];
                auto &f = col.agg_funcs[a];
                if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
                    slot.kind = BuildSlotAggSlot::COUNT_STAR;
                    continue;
                }
                if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) {
                    slot.kind = BuildSlotAggSlot::SKIP;
                    continue;
                }
                slot.validity = FlatVector::Validity(input.data[ai]).GetData();
                if (f == "COUNT") {
                    slot.kind = BuildSlotAggSlot::COUNT_COL;
                    continue;
                }
                if (input.data[ai].GetType().InternalType() != PhysicalType::DOUBLE) {
                    all_fused_double = false;
                    break;
                }
                slot.vals = FlatVector::GetData<double>(input.data[ai]);
                if (f == "SUM") slot.kind = BuildSlotAggSlot::SUM_VAL;
                else if (f == "AVG") slot.kind = BuildSlotAggSlot::AVG_VAL;
                else if (f == "MIN") slot.kind = BuildSlotAggSlot::MIN_VAL;
                else if (f == "MAX") slot.kind = BuildSlotAggSlot::MAX_VAL;
                else {
                    all_fused_double = false;
                    break;
                }
            }
            if (all_fused_double) {
                auto pkfk = sink.all_bc_one;
                auto cap = sink.build_ht.mask + 1;
                auto *sl = row_slots.data();
                auto *bc = row_bc.data();
                auto *sums = sink.build_slot_sums.data();
                auto *avg_counts = sink.has_avg ? sink.build_slot_counts.data() : nullptr;
                auto *mins = sink.has_min_max ? sink.build_slot_mins.data() : nullptr;
                auto *maxs = sink.has_min_max ? sink.build_slot_maxs.data() : nullptr;
                auto *has = sink.has_min_max ? sink.build_slot_has.data() : nullptr;
                for (idx_t r = 0; r < n; r++) {
                    auto slot_idx = sl[r];
                    if (slot_idx == DConstants::INVALID_INDEX) continue;
                    auto mult = pkfk ? 1.0 : bc[r];
                    for (idx_t a = 0; a < na; a++) {
                        auto &slot = agg_slots[a];
                        switch (slot.kind) {
                        case BuildSlotAggSlot::COUNT_STAR:
                            sums[a * cap + slot_idx] += mult;
                            break;
                        case BuildSlotAggSlot::COUNT_COL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            sums[a * cap + slot_idx] += mult;
                            break;
                        case BuildSlotAggSlot::SUM_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            sums[a * cap + slot_idx] += slot.vals[r] * mult;
                            break;
                        case BuildSlotAggSlot::AVG_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            sums[a * cap + slot_idx] += slot.vals[r] * mult;
                            avg_counts[a * cap + slot_idx] += mult;
                            break;
                        case BuildSlotAggSlot::MIN_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            if (!has[a * cap + slot_idx] || slot.vals[r] < mins[a * cap + slot_idx]) {
                                mins[a * cap + slot_idx] = slot.vals[r];
                                has[a * cap + slot_idx] = 1;
                            }
                            break;
                        case BuildSlotAggSlot::MAX_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            if (!has[a * cap + slot_idx] || slot.vals[r] > maxs[a * cap + slot_idx]) {
                                maxs[a * cap + slot_idx] = slot.vals[r];
                                has[a * cap + slot_idx] = 1;
                            }
                            break;
                        case BuildSlotAggSlot::SKIP:
                            break;
                        }
                    }
                }
                chunk.SetCardinality(0);
                return OperatorResultType::NEED_MORE_INPUT;
            }

            for (idx_t a = 0; a < na; a++) {
                auto ai = col.agg_input_cols[a];
                auto &f = col.agg_funcs[a];
                auto *sl = row_slots.data();
                auto *bc = row_bc.data();
                if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
                    if (sink.all_bc_one) {
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] != DConstants::INVALID_INDEX) sink.build_slot_sums[a * (sink.build_ht.mask + 1) + sl[r]] += 1.0;
                        }
                    } else {
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] != DConstants::INVALID_INDEX) sink.build_slot_sums[a * (sink.build_ht.mask + 1) + sl[r]] += bc[r];
                        }
                    }
                    continue;
                }
                if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;
                auto ptype = input.data[ai].GetType().InternalType();
                auto *validity = FlatVector::Validity(input.data[ai]).GetData();
                if (f == "SUM" || f == "AVG" || f == "COUNT") {
                    bool is_avg = (f == "AVG");
                    bool is_count = (f == "COUNT");
                    bool pkfk = sink.all_bc_one;
                    auto cap = sink.build_ht.mask + 1;
                    #define BUILD_SLOT_SUM_LOOP(TYPE) { \
                        auto *vals = FlatVector::GetData<TYPE>(input.data[ai]); \
                        if (pkfk) { \
                            for (idx_t r = 0; r < n; r++) { \
                                if (sl[r] == DConstants::INVALID_INDEX) continue; \
                                if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                                sink.build_slot_sums[a * cap + sl[r]] += is_count ? 1.0 : (double)vals[r]; \
                                if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += 1.0; \
                            } \
                        } else { \
                            for (idx_t r = 0; r < n; r++) { \
                                if (sl[r] == DConstants::INVALID_INDEX) continue; \
                                if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                                sink.build_slot_sums[a * cap + sl[r]] += (is_count ? 1.0 : (double)vals[r]) * bc[r]; \
                                if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += bc[r]; \
                            } \
                        } \
                    }
                    switch (ptype) {
                    case PhysicalType::DOUBLE: BUILD_SLOT_SUM_LOOP(double); break;
                    case PhysicalType::FLOAT:  BUILD_SLOT_SUM_LOOP(float); break;
                    case PhysicalType::INT64:  BUILD_SLOT_SUM_LOOP(int64_t); break;
                    case PhysicalType::INT32:  BUILD_SLOT_SUM_LOOP(int32_t); break;
                    case PhysicalType::INT16:  BUILD_SLOT_SUM_LOOP(int16_t); break;
                    case PhysicalType::INT8:   BUILD_SLOT_SUM_LOOP(int8_t); break;
                    case PhysicalType::UINT64: BUILD_SLOT_SUM_LOOP(uint64_t); break;
                    case PhysicalType::UINT32: BUILD_SLOT_SUM_LOOP(uint32_t); break;
                    case PhysicalType::UINT16: BUILD_SLOT_SUM_LOOP(uint16_t); break;
                    case PhysicalType::UINT8:  BUILD_SLOT_SUM_LOOP(uint8_t); break;
                    default:
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] == DConstants::INVALID_INDEX) continue;
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue;
                            sink.build_slot_sums[a * cap + sl[r]] += input.data[ai].GetValue(r).GetValue<double>() * (is_count ? 1.0 : bc[r]);
                            if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += bc[r];
                        }
                        break;
                    }
                    #undef BUILD_SLOT_SUM_LOOP
                } else if (f == "MIN" || f == "MAX") {
                    bool is_min = (f == "MIN");
                    auto cap = sink.build_ht.mask + 1;
                    #define BUILD_SLOT_MINMAX_LOOP(TYPE) { \
                        auto *vals = FlatVector::GetData<TYPE>(input.data[ai]); \
                        for (idx_t r = 0; r < n; r++) { \
                            if (sl[r] == DConstants::INVALID_INDEX) continue; \
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                            double dv = (double)vals[r]; \
                            auto off = a * cap + sl[r]; \
                            if (is_min) { \
                                if (!sink.build_slot_has[off] || dv < sink.build_slot_mins[off]) { sink.build_slot_mins[off] = dv; sink.build_slot_has[off] = 1; } \
                            } else { \
                                if (!sink.build_slot_has[off] || dv > sink.build_slot_maxs[off]) { sink.build_slot_maxs[off] = dv; sink.build_slot_has[off] = 1; } \
                            } \
                        } \
                    }
                    switch (ptype) {
                    case PhysicalType::DOUBLE: BUILD_SLOT_MINMAX_LOOP(double); break;
                    case PhysicalType::FLOAT:  BUILD_SLOT_MINMAX_LOOP(float); break;
                    case PhysicalType::INT64:  BUILD_SLOT_MINMAX_LOOP(int64_t); break;
                    case PhysicalType::INT32:  BUILD_SLOT_MINMAX_LOOP(int32_t); break;
                    case PhysicalType::INT16:  BUILD_SLOT_MINMAX_LOOP(int16_t); break;
                    case PhysicalType::INT8:   BUILD_SLOT_MINMAX_LOOP(int8_t); break;
                    case PhysicalType::UINT64: BUILD_SLOT_MINMAX_LOOP(uint64_t); break;
                    case PhysicalType::UINT32: BUILD_SLOT_MINMAX_LOOP(uint32_t); break;
                    case PhysicalType::UINT16: BUILD_SLOT_MINMAX_LOOP(uint16_t); break;
                    case PhysicalType::UINT8:  BUILD_SLOT_MINMAX_LOOP(uint8_t); break;
                    default:
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] == DConstants::INVALID_INDEX) continue;
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue;
                            double dv = input.data[ai].GetValue(r).GetValue<double>();
                            auto off = a * cap + sl[r];
                            if (is_min) {
                                if (!sink.build_slot_has[off] || dv < sink.build_slot_mins[off]) { sink.build_slot_mins[off] = dv; sink.build_slot_has[off] = 1; }
                            } else {
                                if (!sink.build_slot_has[off] || dv > sink.build_slot_maxs[off]) { sink.build_slot_maxs[off] = dv; sink.build_slot_has[off] = 1; }
                            }
                        }
                        break;
                    }
                    #undef BUILD_SLOT_MINMAX_LOOP
                }
            }
            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }
        hash_t *gh;
        Vector ghv(LogicalType::HASH, n);
        bool ungrouped = col.group_cols.empty();
        if (ungrouped) {
            // Ungrouped aggregate: all rows map to single result slot
            ghv.Flatten(n);
            gh = FlatVector::GetData<hash_t>(ghv);
            for (idx_t r = 0; r < n; r++) gh[r] = 0;
        } else if (same_keys) {
            gh = h; // Reuse probe hash — skip second hash computation
        } else {
            ghv.Flatten(n);
            VectorOperations::Hash(input.data[col.group_cols[0]], ghv, n);
            for (idx_t i=1;i<col.group_cols.size();i++) VectorOperations::CombineHash(ghv, input.data[col.group_cols[i]], n);
            gh = FlatVector::GetData<hash_t>(ghv);
            ApplyAggJoinTestHashBits(gh, n);
        }

        // ── Phase 2: Batch probe build HT → multiplier array ──
        // For ALL rows, compute build_count (0 if no match).
        // This vectorizes the build probe into a flat array.
        auto &rht = sink.result_ht;
        // Pre-size to avoid Grow during probe (which invalidates cached row_slots)
        if (!rht.mask) {
            idx_t est_groups = 0;
            if (ungrouped) {
                est_groups = 1;
            } else if (same_keys) {
                // GROUP BY join key: one output group per build key in the steady state.
                est_groups = sink.build_ht.count;
            } else {
                // When grouping diverges from the join key, leave headroom for multiple
                // groups per build key without the old 16x over-allocation.
                idx_t multiplier = (col.group_cols.size() <= 2) ? 2 : 4;
                est_groups = sink.build_ht.count;
                if (est_groups > std::numeric_limits<idx_t>::max() / multiplier) {
                    est_groups = std::numeric_limits<idx_t>::max();
                } else {
                    est_groups *= multiplier;
                }
            }
            if (est_groups < 4096) est_groups = 4096;
            // Determine group column types for typed storage
            idx_t ng = col.group_cols.size();
            vector<bool> g_is_int(ng, true);
            vector<bool> g_use_value(ng, false);
            for (idx_t g = 0; g < ng; g++) {
                auto gt = input.data[col.group_cols[g]].GetType().InternalType();
                g_is_int[g] = (gt != PhysicalType::DOUBLE && gt != PhysicalType::FLOAT);
                g_use_value[g] = NeedsValueGroupStorage(gt);
            }
            // Determine which aggregates need Value-based MIN/MAX (non-numeric types)
            vector<bool> val_mm(na, false);
            for (idx_t a = 0; a < na; a++) {
                auto &f = col.agg_funcs[a];
                if ((f == "MIN" || f == "MAX") && a < col.agg_is_numeric.size() && !col.agg_is_numeric[a]) {
                    val_mm[a] = true;
                }
            }
            rht.Init(est_groups, na, ng, &g_is_int, &val_mm, &g_use_value);
        }

        // Use pre-allocated scratch buffers (avoid per-chunk allocation)
        sink.probe_bc.resize(n);
        sink.probe_slots.resize(n);
        sink.probe_build_ptrs.resize(n);
        std::fill(sink.probe_bc.begin(), sink.probe_bc.end(), 0.0);
        std::fill(sink.probe_slots.begin(), sink.probe_slots.end(), DConstants::INVALID_INDEX);
        std::fill(sink.probe_build_ptrs.begin(), sink.probe_build_ptrs.end(), nullptr);
        auto &row_bc = sink.probe_bc;
        auto &row_slots = sink.probe_slots;
        auto &row_builds = sink.probe_build_ptrs;
        idx_t match_count = 0;

        // Prefilter: for single integer keys, extract typed key array for range check.
        // Avoids hash table probe for keys outside build min/max range.
        int64_t *prefilter_keys = nullptr;
        if (sink.has_range_prefilter && col.probe_key_cols.size() == 1) {
            auto pki = col.probe_key_cols[0];
            auto pt = input.data[pki].GetType().InternalType();
            sink.prefilter_keys_buf.resize(n);
            prefilter_keys = sink.prefilter_keys_buf.data();
            switch (pt) {
            case PhysicalType::INT8:  { auto *k = FlatVector::GetData<int8_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=k[r]; break; }
            case PhysicalType::INT16: { auto *k = FlatVector::GetData<int16_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=k[r]; break; }
            case PhysicalType::INT32: { auto *k = FlatVector::GetData<int32_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=k[r]; break; }
            case PhysicalType::INT64: { auto *k = FlatVector::GetData<int64_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=k[r]; break; }
            case PhysicalType::UINT8:  { auto *k = FlatVector::GetData<uint8_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=(int64_t)k[r]; break; }
            case PhysicalType::UINT16: { auto *k = FlatVector::GetData<uint16_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=(int64_t)k[r]; break; }
            case PhysicalType::UINT32: { auto *k = FlatVector::GetData<uint32_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=(int64_t)k[r]; break; }
            case PhysicalType::UINT64: { auto *k = FlatVector::GetData<uint64_t>(input.data[pki]); for (idx_t r=0;r<n;r++) prefilter_keys[r]=(int64_t)k[r]; break; }
            default: prefilter_keys = nullptr; break;
            }
        }

        // Bloom filter check: inline lambda for probe loop
        auto *bf = sink.bloom_filter.empty() ? nullptr : sink.bloom_filter.data();
        auto bf_mask = sink.bloom_mask;

        // Probe build HT and resolve result slots per-row from group hash.
        // Cache build pointer to avoid re-probing in Phase 3 build-side aggregates.
        for (idx_t r = 0; r < n; r++) {
            // Range prefilter: skip hash probe if key is outside build range
            if (prefilter_keys && (prefilter_keys[r] < sink.build_key_min || prefilter_keys[r] > sink.build_key_max)) {
                sink.range_prefilter_skips++;
                continue;
            }
            // Bloom filter: skip if hash is definitely not in build set
            if (bf) {
                auto bh1 = h[r];
                auto bh2 = (bh1 >> 17) | (bh1 << 47);
                if (!(bf[(bh1 & bf_mask) / 64] & (1ULL << ((bh1 & bf_mask) % 64))) ||
                    !(bf[(bh2 & bf_mask) / 64] & (1ULL << ((bh2 & bf_mask) % 64)))) {
                    sink.bloom_prefilter_skips++;
                    continue;
                }
            }
            if (r + 4 < n) sink.build_ht.Prefetch(h[r + 4]);
            auto *build = sink.build_ht.Find(h[r], [&](const BuildEntry &b) {
                return BuildEntryMatches(b, input, col.probe_key_cols, r);
            });
            if (!build) continue;
            // Skip rows with NULL join keys (inner join semantics)
            bool has_null = false;
            for (auto ki : col.probe_key_cols) {
                if (FlatVector::IsNull(input.data[ki], r)) { has_null = true; break; }
            }
            if (has_null) continue;
            row_bc[r] = (double)build->count;
            row_builds[r] = build;
            // Resolve result slot from group hash (not cached on build entry)
            auto slot = rht.FindOrCreate(
                gh[r], [&](idx_t existing_slot) { return ungrouped || ResultSlotGroupMatches(rht, existing_slot, input, col, r); },
                [&](idx_t new_slot) {
                    rht.SetGroupInit(new_slot);
                    for (idx_t g = 0; g < col.group_cols.size(); g++) {
                        auto gi = col.group_cols[g];
                        auto gt = input.data[gi].GetType().InternalType();
                        if (rht.GroupUsesValue(g)) {
                            rht.GroupVal(g, new_slot) = input.data[gi].GetValue(r);
                            continue;
                        }
                        switch (gt) {
                        case PhysicalType::INT32:  rht.GroupInt(g, new_slot) = FlatVector::GetData<int32_t>(input.data[gi])[r]; break;
                        case PhysicalType::INT64:  rht.GroupInt(g, new_slot) = FlatVector::GetData<int64_t>(input.data[gi])[r]; break;
                        case PhysicalType::INT16:  rht.GroupInt(g, new_slot) = FlatVector::GetData<int16_t>(input.data[gi])[r]; break;
                        case PhysicalType::INT8:   rht.GroupInt(g, new_slot) = FlatVector::GetData<int8_t>(input.data[gi])[r]; break;
                        case PhysicalType::UINT32: rht.GroupInt(g, new_slot) = FlatVector::GetData<uint32_t>(input.data[gi])[r]; break;
                        case PhysicalType::UINT16: rht.GroupInt(g, new_slot) = FlatVector::GetData<uint16_t>(input.data[gi])[r]; break;
                        case PhysicalType::UINT8:  rht.GroupInt(g, new_slot) = FlatVector::GetData<uint8_t>(input.data[gi])[r]; break;
                        case PhysicalType::DOUBLE: rht.GroupDbl(g, new_slot) = FlatVector::GetData<double>(input.data[gi])[r]; break;
                        case PhysicalType::FLOAT:  rht.GroupDbl(g, new_slot) = FlatVector::GetData<float>(input.data[gi])[r]; break;
                        default: rht.GroupVal(g, new_slot) = input.data[gi].GetValue(r); break;
                        }
                    }
                });
            row_slots[r] = slot;
            match_count++;
        }
        sink.hash_match_rows += match_count;
        if (!match_count) { chunk.SetCardinality(0); return OperatorResultType::NEED_MORE_INPUT; }

        // ── Phase 3: Accumulation ──
        if (sink.use_native_ht && sink.agg_ht) {
            // NATIVE PATH: Use DuckDB's GroupedAggregateHashTable::AddChunk
            // Build group chunk from matched rows
            DataChunk group_chunk;
            group_chunk.Initialize(ctx.client, group_types);
            DataChunk payload_chunk;
            payload_chunk.Initialize(ctx.client, payload_types);

            // Build selection vector of matched rows
            SelectionVector match_sel(n);
            idx_t mc = 0;
            for (idx_t r = 0; r < n; r++) {
                if (row_bc[r] > 0) match_sel.set_index(mc++, r);
            }

            // Cast group columns from scan types to decompressed types
            for (idx_t g = 0; g < col.group_cols.size(); g++) {
                auto &src_vec = input.data[col.group_cols[g]];
                auto &dst_type = group_types[g];
                if (src_vec.GetType() == dst_type) {
                    // Same type — just reference + slice
                    group_chunk.data[g].Reference(src_vec);
                    group_chunk.data[g].Slice(match_sel, mc);
                } else {
                    // Type mismatch (e.g. UINT8 → INTEGER) — cast matched rows
                    Vector sliced(src_vec.GetType(), mc);
                    VectorOperations::Copy(src_vec, sliced, match_sel, mc, 0, 0);
                    VectorOperations::Cast(ctx.client, sliced, group_chunk.data[g], mc);
                }
            }
            group_chunk.SetCardinality(mc);

            // Build payload: for SUM, pre-multiply val by build_count.
            // Write directly into payload_chunk vectors.
            for (idx_t a = 0; a < na; a++) {
                auto ai = col.agg_input_cols[a];
                if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) {
                    // COUNT(*) — fill with build counts
                    auto *dst = FlatVector::GetData<int64_t>(payload_chunk.data[a]);
                    for (idx_t m = 0; m < mc; m++) {
                        dst[m] = (int64_t)row_bc[match_sel.get_index(m)];
                    }
                    continue;
                }
                // Pre-multiply by build count into payload vector
                auto ptype = input.data[ai].GetType().InternalType();
                if (ptype == PhysicalType::DOUBLE) {
                    auto *src = FlatVector::GetData<double>(input.data[ai]);
                    auto *dst = FlatVector::GetData<double>(payload_chunk.data[a]);
                    for (idx_t m = 0; m < mc; m++) {
                        auto r = match_sel.get_index(m);
                        dst[m] = src[r] * row_bc[r];
                    }
                } else {
                    // Cast to double, multiply
                    auto *dst = FlatVector::GetData<double>(payload_chunk.data[a]);
                    #define NATIVE_PAYLOAD_LOOP(TYPE) { \
                        auto *src = FlatVector::GetData<TYPE>(input.data[ai]); \
                        for (idx_t m = 0; m < mc; m++) { \
                            auto r = match_sel.get_index(m); \
                            dst[m] = (double)src[r] * row_bc[r]; \
                        } \
                    }
                    switch (ptype) {
                    case PhysicalType::FLOAT:  NATIVE_PAYLOAD_LOOP(float); break;
                    case PhysicalType::INT64:  NATIVE_PAYLOAD_LOOP(int64_t); break;
                    case PhysicalType::INT32:  NATIVE_PAYLOAD_LOOP(int32_t); break;
                    case PhysicalType::INT16:  NATIVE_PAYLOAD_LOOP(int16_t); break;
                    case PhysicalType::INT8:   NATIVE_PAYLOAD_LOOP(int8_t); break;
                    case PhysicalType::UINT64: NATIVE_PAYLOAD_LOOP(uint64_t); break;
                    case PhysicalType::UINT32: NATIVE_PAYLOAD_LOOP(uint32_t); break;
                    case PhysicalType::UINT16: NATIVE_PAYLOAD_LOOP(uint16_t); break;
                    case PhysicalType::UINT8:  NATIVE_PAYLOAD_LOOP(uint8_t); break;
                    default:
                        for (idx_t m = 0; m < mc; m++) {
                            auto r = match_sel.get_index(m);
                            dst[m] = input.data[ai].GetValue(r).GetValue<double>() * row_bc[r];
                        }
                        break;
                    }
                    #undef NATIVE_PAYLOAD_LOOP
                }
            }
            payload_chunk.SetCardinality(mc);

            // Compute group hashes for AddChunk
            Vector group_hashes(LogicalType::HASH, mc);
            group_hashes.Flatten(mc);
            auto *ghd = FlatVector::GetData<hash_t>(group_hashes);
            for (idx_t m = 0; m < mc; m++) ghd[m] = gh[match_sel.get_index(m)];

            // Call AddChunk — DuckDB handles vectorized group resolution + accumulation
            unsafe_vector<idx_t> filter;
            for (idx_t a = 0; a < na; a++) filter.push_back(a);
            sink.agg_ht->AddChunk(group_chunk, group_hashes, payload_chunk, filter);

            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }

        // FALLBACK: per-aggregate column-major accumulation
        // Process each aggregate column in a tight loop over matched rows.
        for (idx_t a = 0; a < na; a++) {
            auto ai = col.agg_input_cols[a];
            auto &f = col.agg_funcs[a];
            bool on_build = (col.agg_on_build.size() > a && col.agg_on_build[a]);

            if (f == "COUNT" && ai == DConstants::INVALID_INDEX && !on_build) {
                // COUNT(*): scatter-add build counts into result slots
                auto *sl = row_slots.data();
                if (sink.all_bc_one) {
                    for (idx_t r = 0; r < n; r++) {
                        if (sl[r] != DConstants::INVALID_INDEX) rht.Sum(sl[r], a) += 1.0;
                    }
                } else {
                    auto *bc = row_bc.data();
                    for (idx_t r = 0; r < n; r++) {
                        if (sl[r] != DConstants::INVALID_INDEX) rht.Sum(sl[r], a) += bc[r];
                    }
                }
                continue;
            }

            // Build-side aggregate: look up pre-accumulated value from BuildEntry
            if (on_build) {
                auto *sl = row_slots.data();
                idx_t ba = 0;
                for (idx_t i = 0; i < a; i++)
                    if (col.agg_on_build.size() > i && col.agg_on_build[i]) ba++;
                for (idx_t r = 0; r < n; r++) {
                    if (row_slots[r] == DConstants::INVALID_INDEX) continue;
                    auto *build = row_builds[r];
                    if (!build) continue;
                    auto &bav = build->bav;
                    if (f == "SUM") {
                        rht.Sum(sl[r], a) += bav.agg_sum[ba];
                    } else if (f == "AVG") {
                        rht.Sum(sl[r], a) += bav.agg_sum[ba];
                        rht.Count(sl[r], a) += bav.agg_count[ba];
                    } else if (f == "MIN") {
                            if (bav.agg_init[ba]) {
                                auto bv = bav.agg_min[ba];
                            if (!rht.GetHas(sl[r], a) || bv < rht.Min(sl[r], a))
                                { rht.Min(sl[r], a) = bv; rht.SetHas(sl[r], a, true); }
                        }
                    } else if (f == "MAX") {
                            if (bav.agg_init[ba]) {
                                auto bv = bav.agg_max[ba];
                            if (!rht.GetHas(sl[r], a) || bv > rht.Max(sl[r], a))
                                { rht.Max(sl[r], a) = bv; rht.SetHas(sl[r], a, true); }
                        }
                    } else if (f == "COUNT") {
                        rht.Sum(sl[r], a) += bav.agg_count[ba];
                    }
                }
                continue;
            }

            if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;

            auto ptype = input.data[ai].GetType().InternalType();
            auto *validity = FlatVector::Validity(input.data[ai]).GetData();
            auto *bc = row_bc.data();
            auto *sl = row_slots.data();

            if (f == "SUM" || f == "AVG" || f == "COUNT") {
                bool is_avg = (f == "AVG");
                bool pkfk = sink.all_bc_one;
                // Tight vectorized scatter-add: for each row, sum[slot] += val * bc
                // PK/FK fast path: skip multiply-by-1.0 when all build counts == 1.
                #define SUM_LOOP(TYPE) { \
                    auto *vals = FlatVector::GetData<TYPE>(input.data[ai]); \
                    if (pkfk) { \
                        for (idx_t r = 0; r < n; r++) { \
                            if (sl[r] == DConstants::INVALID_INDEX) continue; \
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                            rht.Sum(sl[r], a) += (double)vals[r]; \
                            if (is_avg) rht.Count(sl[r], a) += 1.0; \
                        } \
                    } else { \
                        for (idx_t r = 0; r < n; r++) { \
                            if (sl[r] == DConstants::INVALID_INDEX) continue; \
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                            rht.Sum(sl[r], a) += (double)vals[r] * bc[r]; \
                            if (is_avg) rht.Count(sl[r], a) += bc[r]; \
                        } \
                    } \
                }
                switch (ptype) {
                case PhysicalType::DOUBLE: SUM_LOOP(double); break;
                case PhysicalType::FLOAT:  SUM_LOOP(float);  break;
                case PhysicalType::INT64:  SUM_LOOP(int64_t); break;
                case PhysicalType::INT32:  SUM_LOOP(int32_t); break;
                case PhysicalType::INT16:  SUM_LOOP(int16_t); break;
                case PhysicalType::INT8:   SUM_LOOP(int8_t); break;
                case PhysicalType::UINT64: SUM_LOOP(uint64_t); break;
                case PhysicalType::UINT32: SUM_LOOP(uint32_t); break;
                case PhysicalType::UINT16: SUM_LOOP(uint16_t); break;
                case PhysicalType::UINT8:  SUM_LOOP(uint8_t); break;
                default:
                    for (idx_t r = 0; r < n; r++) {
                        if (sl[r] == DConstants::INVALID_INDEX) continue;
                        if (validity && !((validity[r/64] >> (r%64)) & 1)) continue;
                        rht.Sum(sl[r], a) += input.data[ai].GetValue(r).GetValue<double>() * bc[r];
                        if (is_avg) rht.Count(sl[r], a) += bc[r];
                    }
                    break;
                }
                #undef SUM_LOOP
            } else if (f == "MIN" || f == "MAX") {
                bool is_min = (f == "MIN");
                if (rht.UsesValMinMax(a)) {
                    // Value-based MIN/MAX for non-numeric types (VARCHAR, DATE, etc.)
                    for (idx_t r = 0; r < n; r++) {
                        if (sl[r] == DConstants::INVALID_INDEX) continue;
                        if (validity && !((validity[r/64] >> (r%64)) & 1)) continue;
                        auto val = input.data[ai].GetValue(r);
                        if (val.IsNull()) continue;
                        auto slot = sl[r];
                        if (!rht.GetHas(slot, a)) {
                            if (is_min) rht.ValMin(slot, a) = val;
                            else rht.ValMax(slot, a) = val;
                            rht.SetHas(slot, a, true);
                        } else {
                            if (is_min) {
                                if (val < rht.ValMin(slot, a)) rht.ValMin(slot, a) = val;
                            } else {
                                if (val > rht.ValMax(slot, a)) rht.ValMax(slot, a) = val;
                            }
                        }
                    }
                } else {
                    // Numeric MIN/MAX: typed double path
                    #define MINMAX_HASH_LOOP(TYPE) { \
                        auto *vals = FlatVector::GetData<TYPE>(input.data[ai]); \
                        for (idx_t r = 0; r < n; r++) { \
                            if (sl[r] == DConstants::INVALID_INDEX) continue; \
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                            double dv = (double)vals[r]; \
                            auto slot = sl[r]; \
                            if (is_min) { \
                                if (!rht.GetHas(slot, a) || dv < rht.Min(slot, a)) { rht.Min(slot, a) = dv; rht.SetHas(slot, a, true); } \
                            } else { \
                                if (!rht.GetHas(slot, a) || dv > rht.Max(slot, a)) { rht.Max(slot, a) = dv; rht.SetHas(slot, a, true); } \
                            } \
                        } \
                    }
                    switch (ptype) {
                    case PhysicalType::DOUBLE: MINMAX_HASH_LOOP(double); break;
                    case PhysicalType::FLOAT:  MINMAX_HASH_LOOP(float);  break;
                    case PhysicalType::INT64:  MINMAX_HASH_LOOP(int64_t); break;
                    case PhysicalType::INT32:  MINMAX_HASH_LOOP(int32_t); break;
                    case PhysicalType::INT16:  MINMAX_HASH_LOOP(int16_t); break;
                    case PhysicalType::INT8:   MINMAX_HASH_LOOP(int8_t); break;
                    case PhysicalType::UINT64: MINMAX_HASH_LOOP(uint64_t); break;
                    case PhysicalType::UINT32: MINMAX_HASH_LOOP(uint32_t); break;
                    case PhysicalType::UINT16: MINMAX_HASH_LOOP(uint16_t); break;
                    case PhysicalType::UINT8:  MINMAX_HASH_LOOP(uint8_t); break;
                    default:
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] == DConstants::INVALID_INDEX) continue;
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue;
                            auto dv = input.data[ai].GetValue(r).GetValue<double>();
                            auto slot = sl[r];
                            if (is_min) {
                                if (!rht.GetHas(slot, a) || dv < rht.Min(slot, a)) { rht.Min(slot, a) = dv; rht.SetHas(slot, a, true); }
                            } else {
                                if (!rht.GetHas(slot, a) || dv > rht.Max(slot, a)) { rht.Max(slot, a) = dv; rht.SetHas(slot, a, true); }
                            }
                        }
                        break;
                    }
                    #undef MINMAX_HASH_LOOP
                }
            }
        }
        chunk.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    // ── Emit results ──
    SourceResultType AGGJOIN_GETDATA(ExecutionContext &ctx, DataChunk &chunk, OperatorSourceInput &input) const override {
        if (!sink_state) { chunk.SetCardinality(0); return SourceResultType::FINISHED; }
        auto &sink = sink_state->Cast<AggJoinSinkState>();
        // Empty build side — no results for grouped, default row for ungrouped
        if (sink.build_ht.mask == 0) {
            if (col.group_cols.empty()) {
                auto &src0 = input.global_state.Cast<AggJoinSourceState>();
                if (!src0.initialized) {
                    // Ungrouped aggregate with no matches: return 1 row with defaults
                    src0.initialized = true;
                    auto na = col.agg_funcs.size();
                    for (idx_t a = 0; a < na; a++) {
                        auto &f = col.agg_funcs[a];
                        if (f == "COUNT") {
                            chunk.data[a].SetValue(0, Value::BIGINT(0));
                        } else {
                            chunk.data[a].SetValue(0, Value());
                        }
                    }
                    chunk.SetCardinality(1);
                    return SourceResultType::HAVE_MORE_OUTPUT;
                }
            }
            chunk.SetCardinality(0); return SourceResultType::FINISHED;
        }
        auto &src = input.global_state.Cast<AggJoinSourceState>();

        if (AggJoinTraceStatsEnabled() && !sink.stats_emitted) {
            sink.stats_emitted = true;
            const char *path = sink.use_native_ht ? "native_ht"
                                : sink.segmented_multi_direct_mode ? "segmented_multi_direct"
                                : sink.segmented_direct_mode ? "segmented_direct"
                                : sink.direct_mode ? "direct"
                                : sink.build_slot_hash_mode ? "build_slot_hash"
                                : "hash";
            idx_t result_groups = 0;
            if (col.group_cols.empty()) {
                result_groups = sink.build_ht.mask ? 1 : 0;
            } else if (sink.segmented_multi_direct_mode || sink.segmented_direct_mode) {
                result_groups = sink.segmented_active_keys.size();
            } else if (sink.direct_mode) {
                if (sink.group_is_key && sink.track_active_keys) {
                    result_groups = sink.direct_active_keys.size();
                } else if (sink.group_is_key) {
                    for (idx_t k = 0; k < sink.key_range; k++) {
                        if (sink.build_counts[k] > 0) result_groups++;
                    }
                } else {
                    for (idx_t k = 0; k < sink.direct_group_init.size(); k++) {
                        if (sink.direct_group_init[k]) result_groups++;
                    }
                }
            } else if (sink.build_slot_hash_mode) {
                for (idx_t i = 0; i < sink.build_slot_seen.size(); i++) {
                    if (sink.build_slot_seen[i]) result_groups++;
                }
            } else if (!sink.use_native_ht) {
                result_groups = sink.result_ht.count;
            }
            idx_t build_capacity = sink.build_ht.mask ? sink.build_ht.mask + 1 : 0;
            idx_t hash_capacity = sink.result_ht.capacity;
            fprintf(stderr,
                    "[AGGJOIN_STATS] path=%s probe_est=%llu build_est=%llu group_est=%llu build_rows=%llu build_rows_kept=%llu build_ht_count=%llu build_ht_capacity=%llu probe_rows=%llu hash_matches=%llu range_skips=%llu bloom_skips=%llu result_groups=%llu result_capacity=%llu key_range=%llu build_aggs=%llu\n",
                    path,
                    (unsigned long long)col.probe_estimate,
                    (unsigned long long)col.build_estimate,
                    (unsigned long long)col.group_estimate,
                    (unsigned long long)sink.build_rows_seen,
                    (unsigned long long)sink.build_rows_kept,
                    (unsigned long long)sink.build_ht.count,
                    (unsigned long long)build_capacity,
                    (unsigned long long)sink.probe_rows_seen,
                    (unsigned long long)sink.hash_match_rows,
                    (unsigned long long)sink.range_prefilter_skips,
                    (unsigned long long)sink.bloom_prefilter_skips,
                    (unsigned long long)result_groups,
                    (unsigned long long)hash_capacity,
                    (unsigned long long)sink.key_range,
                    (unsigned long long)sink.build_agg_slots);
        }

        // SEGMENTED DIRECT MODE: grouped emit from segment-backed arrays
        if (sink.segmented_multi_direct_mode) {
            if (!src.initialized) {
                src.initialized = true;
                src.direct_keys = sink.segmented_active_keys;
            }
            if (src.pos >= src.direct_keys.size()) {
                chunk.SetCardinality(0); return SourceResultType::FINISHED;
            }
            idx_t batch = std::min((idx_t)(src.direct_keys.size() - src.pos), (idx_t)STANDARD_VECTOR_SIZE);
            auto &ci = col.group_compress[0];
            if (ci.has_compress && !ci.is_string_compress) {
                auto compress_offset = ci.offset;
                switch (ci.compressed_type.InternalType()) {
                case PhysicalType::UINT8: {
                    auto *dst = FlatVector::GetData<uint8_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (uint8_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                    break;
                }
                case PhysicalType::UINT16: {
                    auto *dst = FlatVector::GetData<uint16_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (uint16_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                    break;
                }
                case PhysicalType::UINT32: {
                    auto *dst = FlatVector::GetData<uint32_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (uint32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                    break;
                }
                default: {
                    auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                    break;
                }
                }
            } else {
                auto out_type = chunk.data[0].GetType().InternalType();
                switch (out_type) {
                case PhysicalType::INT32: {
                    auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                    break;
                }
                case PhysicalType::INT64: {
                    auto *dst = FlatVector::GetData<int64_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (int64_t)src.direct_keys[src.pos + i] + sink.key_min;
                    break;
                }
                default:
                    for (idx_t i = 0; i < batch; i++) chunk.data[0].SetValue(i, Value::BIGINT((int64_t)src.direct_keys[src.pos + i] + sink.key_min));
                    break;
                }
            }
            for (idx_t a = 0; a < sink.num_aggs; a++) {
                auto &f = col.agg_funcs[a];
                auto out_idx = 1 + a;
                auto out_type = chunk.data[out_idx].GetType().InternalType();
                auto *dst = (out_type == PhysicalType::INT64) ? nullptr : FlatVector::GetData<double>(chunk.data[out_idx]);
                auto &validity = FlatVector::Validity(chunk.data[out_idx]);
                for (idx_t i = 0; i < batch; i++) {
                    auto k = src.direct_keys[src.pos + i];
                    auto seg = k >> sink.segmented_shift;
                    auto local = k & sink.segmented_mask;
                    if (f == "AVG") {
                        auto sum = sink.segmented_multi_accums[seg][local * sink.segmented_accum_slots + sink.segmented_accum_index[a]];
                        auto count = sink.segmented_multi_avg_counts[seg][local * sink.segmented_avg_slots + sink.segmented_avg_index[a]];
                        auto v = count > 0 ? sum / count : 0.0;
                        if (dst) dst[i] = v;
                        else chunk.data[out_idx].SetValue(i, Value::DOUBLE(v));
                    } else if (f == "MIN") {
                        auto slot = local * sink.segmented_min_slots + sink.segmented_min_index[a];
                        if (sink.segmented_multi_min_has[seg][slot]) {
                            dst[i] = sink.segmented_multi_mins[seg][slot];
                        } else {
                            validity.SetInvalid(i);
                        }
                    } else if (f == "MAX") {
                        auto slot = local * sink.segmented_max_slots + sink.segmented_max_index[a];
                        if (sink.segmented_multi_max_has[seg][slot]) {
                            dst[i] = sink.segmented_multi_maxs[seg][slot];
                        } else {
                            validity.SetInvalid(i);
                        }
                    } else {
                        auto sum = sink.segmented_multi_accums[seg][local * sink.segmented_accum_slots + sink.segmented_accum_index[a]];
                        if (out_type == PhysicalType::INT64) chunk.data[out_idx].SetValue(i, Value::BIGINT((int64_t)sum));
                        else dst[i] = sum;
                    }
                }
            }
            src.pos += batch;
            chunk.SetCardinality(batch);
            return SourceResultType::HAVE_MORE_OUTPUT;
        }

        if (sink.segmented_direct_mode) {
            if (!src.initialized) {
                src.initialized = true;
                src.direct_keys = sink.segmented_active_keys;
            }
            if (src.pos >= src.direct_keys.size()) {
                chunk.SetCardinality(0); return SourceResultType::FINISHED;
            }
            idx_t batch = std::min((idx_t)(src.direct_keys.size() - src.pos), (idx_t)STANDARD_VECTOR_SIZE);
            auto &ci = col.group_compress[0];
            if (ci.has_compress && !ci.is_string_compress) {
                auto compress_offset = ci.offset;
                switch (ci.compressed_type.InternalType()) {
                case PhysicalType::UINT8: {
                    auto *dst = FlatVector::GetData<uint8_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (uint8_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                    break;
                }
                case PhysicalType::UINT16: {
                    auto *dst = FlatVector::GetData<uint16_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (uint16_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                    break;
                }
                case PhysicalType::UINT32: {
                    auto *dst = FlatVector::GetData<uint32_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (uint32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                    break;
                }
                default: {
                    auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                    break;
                }
                }
            } else {
                auto out_type = chunk.data[0].GetType().InternalType();
                switch (out_type) {
                case PhysicalType::INT32: {
                    auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                    break;
                }
                case PhysicalType::INT64: {
                    auto *dst = FlatVector::GetData<int64_t>(chunk.data[0]);
                    for (idx_t i = 0; i < batch; i++) dst[i] = (int64_t)src.direct_keys[src.pos + i] + sink.key_min;
                    break;
                }
                default:
                    for (idx_t i = 0; i < batch; i++) chunk.data[0].SetValue(i, Value::BIGINT((int64_t)src.direct_keys[src.pos + i] + sink.key_min));
                    break;
                }
            }
            auto &f0 = col.agg_funcs[0];
            auto out_type = chunk.data[1].GetType().InternalType();
            auto *dst = (out_type == PhysicalType::INT64) ? nullptr : FlatVector::GetData<double>(chunk.data[1]);
            for (idx_t i = 0; i < batch; i++) {
                auto k = src.direct_keys[src.pos + i];
                auto seg = k >> sink.segmented_shift;
                auto local = k & sink.segmented_mask;
                auto sum = sink.segmented_sums[seg][local];
                if (f0 == "AVG") {
                    auto count = sink.segmented_avg_counts[seg][local];
                    auto v = count > 0 ? sum / count : 0.0;
                    if (dst) dst[i] = v;
                    else chunk.data[1].SetValue(i, Value::DOUBLE(v));
                } else if (out_type == PhysicalType::INT64) {
                    chunk.data[1].SetValue(i, Value::BIGINT((int64_t)sum));
                } else {
                    dst[i] = sum;
                }
            }
            src.pos += batch;
            chunk.SetCardinality(batch);
            return SourceResultType::HAVE_MORE_OUTPUT;
        }

        // DIRECT MODE: emit from flat arrays — vectorized output
        if (sink.direct_mode) {
            // ── Ungrouped direct mode: emit 1 row from running accumulators ──
            if (col.group_cols.empty()) {
                if (src.initialized) {
                    chunk.SetCardinality(0); return SourceResultType::FINISHED;
                }
                src.initialized = true;
                auto na = col.agg_funcs.size();
                bool has_running = !sink.ungrouped_sum.empty();
                for (idx_t a = 0; a < na; a++) {
                    auto &f = col.agg_funcs[a];
                    auto out_type = chunk.data[a].GetType().InternalType();
                    if (has_running) {
                        // O(1) emit from running accumulators (populated during probe)
                        if (f == "SUM" || f == "COUNT") {
                            if (out_type == PhysicalType::INT64)
                                chunk.data[a].SetValue(0, Value::BIGINT((int64_t)sink.ungrouped_sum[a]));
                            else
                                chunk.data[a].SetValue(0, Value::DOUBLE(sink.ungrouped_sum[a]));
                        } else if (f == "AVG") {
                            auto c = sink.ungrouped_count[a];
                            chunk.data[a].SetValue(0, c > 0 ? Value::DOUBLE(sink.ungrouped_sum[a] / c) : Value());
                        } else if (f == "MIN") {
                            chunk.data[a].SetValue(0, sink.ungrouped_has[a] ? Value::DOUBLE(sink.ungrouped_min[a]) : Value());
                        } else if (f == "MAX") {
                            chunk.data[a].SetValue(0, sink.ungrouped_has[a] ? Value::DOUBLE(sink.ungrouped_max[a]) : Value());
                        } else {
                            chunk.data[a].SetValue(0, Value());
                        }
                    } else {
                        // Fallback: O(krange) reduction from per-key arrays
                        auto krange = sink.key_range;
                        if (f == "SUM" || f == "COUNT") {
                            double total = 0.0;
                            auto *s = sink.direct_sums.data() + a * krange;
                            for (idx_t k = 0; k < krange; k++) total += s[k];
                            if (out_type == PhysicalType::INT64)
                                chunk.data[a].SetValue(0, Value::BIGINT((int64_t)total));
                            else
                                chunk.data[a].SetValue(0, Value::DOUBLE(total));
                        } else if (f == "AVG" && sink.has_avg) {
                            double sum_total = 0.0, count_total = 0.0;
                            auto *s = sink.direct_sums.data() + a * krange;
                            auto *c = sink.direct_counts.data() + a * krange;
                            for (idx_t k = 0; k < krange; k++) { sum_total += s[k]; count_total += c[k]; }
                            chunk.data[a].SetValue(0, count_total > 0 ? Value::DOUBLE(sum_total / count_total) : Value());
                        } else if (f == "MIN" && sink.has_min_max) {
                            auto *m = sink.direct_mins.data() + a * krange;
                            auto *h = sink.direct_has.data() + a * krange;
                            double best = std::numeric_limits<double>::max(); bool found = false;
                            for (idx_t k = 0; k < krange; k++) { if (h[k] && m[k] < best) { best = m[k]; found = true; } }
                            chunk.data[a].SetValue(0, found ? Value::DOUBLE(best) : Value());
                        } else if (f == "MAX" && sink.has_min_max) {
                            auto *m = sink.direct_maxs.data() + a * krange;
                            auto *h = sink.direct_has.data() + a * krange;
                            double best = std::numeric_limits<double>::lowest(); bool found = false;
                            for (idx_t k = 0; k < krange; k++) { if (h[k] && m[k] > best) { best = m[k]; found = true; } }
                            chunk.data[a].SetValue(0, found ? Value::DOUBLE(best) : Value());
                        } else {
                            chunk.data[a].SetValue(0, Value());
                        }
                    }
                }
                chunk.SetCardinality(1);
                return SourceResultType::HAVE_MORE_OUTPUT;
            }

            // ── Grouped direct mode ──
            if (!src.initialized) {
                src.initialized = true;
                if (sink.track_active_keys) {
                    src.direct_keys = sink.direct_active_keys;
                } else {
                    for (idx_t k = 0; k < sink.key_range; k++) {
                        if (sink.build_counts[k] > 0 &&
                            (sink.group_is_key || sink.direct_group_init[k]))
                            src.direct_keys.push_back(k);
                    }
                }
            }
            if (src.pos >= src.direct_keys.size()) {
                chunk.SetCardinality(0); return SourceResultType::FINISHED;
            }
            auto na = col.agg_funcs.size();
            auto ng = col.group_cols.size();
            idx_t batch = std::min((idx_t)(src.direct_keys.size() - src.pos),
                                   (idx_t)STANDARD_VECTOR_SIZE);

            // ── Vectorized group column output ──
            if (sink.group_is_key && ng == 1) {
                // Fast path: group value = key = array_index + kmin.
                // Apply compression if needed, write directly to typed FlatVector.
                auto &ci = col.group_compress[0];
                if (ci.has_compress && !ci.is_string_compress) {
                    auto compress_offset = ci.offset;
                    auto kmin = sink.key_min;
                    switch (ci.compressed_type.InternalType()) {
                    case PhysicalType::UINT8: {
                        auto *dst = FlatVector::GetData<uint8_t>(chunk.data[0]);
                        for (idx_t i = 0; i < batch; i++)
                            dst[i] = (uint8_t)((int64_t)src.direct_keys[src.pos + i] + kmin - compress_offset);
                        break;
                    }
                    case PhysicalType::UINT16: {
                        auto *dst = FlatVector::GetData<uint16_t>(chunk.data[0]);
                        for (idx_t i = 0; i < batch; i++)
                            dst[i] = (uint16_t)((int64_t)src.direct_keys[src.pos + i] + kmin - compress_offset);
                        break;
                    }
                    case PhysicalType::UINT32: {
                        auto *dst = FlatVector::GetData<uint32_t>(chunk.data[0]);
                        for (idx_t i = 0; i < batch; i++)
                            dst[i] = (uint32_t)((int64_t)src.direct_keys[src.pos + i] + kmin - compress_offset);
                        break;
                    }
                    default: {
                        auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                        for (idx_t i = 0; i < batch; i++)
                            dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + kmin);
                        break;
                    }
                    }
                } else {
                    // No compression — output raw key value
                    auto key_type = chunk.data[0].GetType().InternalType();
                    switch (key_type) {
                    case PhysicalType::INT32: {
                        auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                        for (idx_t i = 0; i < batch; i++)
                            dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                        break;
                    }
                    case PhysicalType::INT64: {
                        auto *dst = FlatVector::GetData<int64_t>(chunk.data[0]);
                        for (idx_t i = 0; i < batch; i++)
                            dst[i] = (int64_t)src.direct_keys[src.pos + i] + sink.key_min;
                        break;
                    }
                    default:
                        for (idx_t i = 0; i < batch; i++)
                            chunk.data[0].SetValue(i, Value::BIGINT((int64_t)src.direct_keys[src.pos + i] + sink.key_min));
                        break;
                    }
                }
            } else {
                // Fallback: multiple group columns or non-key groups
                for (idx_t i = 0; i < batch; i++) {
                    auto k = src.direct_keys[src.pos + i];
                    auto &gv = sink.direct_group_vals[k];
                    for (idx_t g = 0; g < gv.size(); g++) {
                        if (g < col.group_compress.size() && col.group_compress[g].has_compress &&
                            !col.group_compress[g].is_string_compress) {
                            auto raw_int = gv[g].GetValue<int64_t>();
                            auto compressed = raw_int - col.group_compress[g].offset;
                            switch (col.group_compress[g].compressed_type.InternalType()) {
                            case PhysicalType::UINT8:
                                chunk.data[g].SetValue(i, Value::UTINYINT((uint8_t)compressed)); break;
                            case PhysicalType::UINT16:
                                chunk.data[g].SetValue(i, Value::USMALLINT((uint16_t)compressed)); break;
                            default:
                                chunk.data[g].SetValue(i, gv[g]); break;
                            }
                        } else {
                            chunk.data[g].SetValue(i, gv[g]);
                        }
                    }
                }
            }

            // ── Vectorized aggregate output: direct FlatVector write ──
            for (idx_t a = 0; a < na; a++) {
                auto &f = col.agg_funcs[a];
                auto out_type = chunk.data[ng + a].GetType().InternalType();
                // COUNT output is BIGINT — write as int64
                if (out_type == PhysicalType::INT64) {
                    auto *dst = FlatVector::GetData<int64_t>(chunk.data[ng + a]);
                    auto *sums = sink.direct_sums.data();
                    for (idx_t i = 0; i < batch; i++) {
                        dst[i] = (int64_t)sums[a * sink.key_range + src.direct_keys[src.pos + i]];
                    }
                    continue;
                }
                auto *dst = FlatVector::GetData<double>(chunk.data[ng + a]);
                if (f == "MIN" && sink.has_min_max) {
                    auto *mins = sink.direct_mins.data();
                    auto *has = sink.direct_has.data();
                    auto &validity = FlatVector::Validity(chunk.data[ng + a]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto slot = a * sink.key_range + src.direct_keys[src.pos + i];
                        if (has[slot]) { dst[i] = mins[slot]; }
                        else { validity.SetInvalid(i); }
                    }
                } else if (f == "MAX" && sink.has_min_max) {
                    auto *maxs = sink.direct_maxs.data();
                    auto *has = sink.direct_has.data();
                    auto &validity = FlatVector::Validity(chunk.data[ng + a]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto slot = a * sink.key_range + src.direct_keys[src.pos + i];
                        if (has[slot]) { dst[i] = maxs[slot]; }
                        else { validity.SetInvalid(i); }
                    }
                } else if (f == "AVG" && sink.has_avg) {
                    auto *sums = sink.direct_sums.data();
                    auto *counts = sink.direct_counts.data();
                    for (idx_t i = 0; i < batch; i++) {
                        auto slot = a * sink.key_range + src.direct_keys[src.pos + i];
                        dst[i] = counts[slot] > 0 ? sums[slot] / counts[slot] : 0.0;
                    }
                } else {
                    auto *sums = sink.direct_sums.data();
                    for (idx_t i = 0; i < batch; i++) {
                        dst[i] = sums[a * sink.key_range + src.direct_keys[src.pos + i]];
                    }
                }
            }

            src.pos += batch;
            chunk.SetCardinality(batch);
            return batch == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
        }

        if (sink.build_slot_hash_mode) {
            if (!src.initialized) {
                src.initialized = true;
                auto cap = sink.build_ht.mask + 1;
                src.slot_indices.reserve(sink.build_ht.count);
                for (idx_t i = 0; i < cap; i++) {
                    if (sink.build_ht.buckets[i].occupied && sink.build_slot_seen[i]) src.slot_indices.push_back(i);
                }
            }
            if (src.pos >= src.slot_indices.size()) {
                chunk.SetCardinality(0);
                return SourceResultType::FINISHED;
            }
            auto na = col.agg_funcs.size();
            auto cap = sink.build_ht.mask + 1;
            idx_t batch = std::min((idx_t)(src.slot_indices.size() - src.pos), (idx_t)STANDARD_VECTOR_SIZE);
            auto key_type = chunk.data[0].GetType().InternalType();
            if (key_type == PhysicalType::DOUBLE) {
                auto *dst = FlatVector::GetData<double>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = sink.build_ht.buckets[src.slot_indices[src.pos + i]].dbl_key;
            } else if (key_type == PhysicalType::FLOAT) {
                auto *dst = FlatVector::GetData<float>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = (float)sink.build_ht.buckets[src.slot_indices[src.pos + i]].dbl_key;
            } else if (key_type == PhysicalType::VARCHAR) {
                auto *dst = FlatVector::GetData<string_t>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) {
                    auto &entry = sink.build_ht.buckets[src.slot_indices[src.pos + i]];
                    if (entry.uses_string_key) dst[i] = StringVector::AddString(chunk.data[0], entry.str_key);
                    else if (!entry.key_vals.empty()) chunk.data[0].SetValue(i, entry.key_vals[0]);
                    else chunk.data[0].SetValue(i, Value());
                }
            } else {
                for (idx_t i = 0; i < batch; i++) {
                    chunk.data[0].SetValue(i, Value::DOUBLE(sink.build_ht.buckets[src.slot_indices[src.pos + i]].dbl_key));
                }
            }
            for (idx_t a = 0; a < na; a++) {
                auto out_idx = 1 + a;
                auto &fn = col.agg_funcs[a];
                auto out_type = chunk.data[out_idx].GetType().InternalType();
                auto &validity = FlatVector::Validity(chunk.data[out_idx]);
                if (fn == "AVG") {
                    if (out_type == PhysicalType::DOUBLE) {
                        auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                        for (idx_t i = 0; i < batch; i++) {
                            auto off = a * cap + src.slot_indices[src.pos + i];
                            auto c = sink.build_slot_counts[off];
                            if (c > 0) dst[i] = sink.build_slot_sums[off] / c;
                            else validity.SetInvalid(i);
                        }
                    } else {
                        for (idx_t i = 0; i < batch; i++) {
                            auto off = a * cap + src.slot_indices[src.pos + i];
                            auto c = sink.build_slot_counts[off];
                            if (c > 0) chunk.data[out_idx].SetValue(i, Value::DOUBLE(sink.build_slot_sums[off] / c));
                            else chunk.data[out_idx].SetValue(i, Value());
                        }
                    }
                } else if (fn == "MIN" || fn == "MAX") {
                    auto *dst = (out_type == PhysicalType::DOUBLE) ? FlatVector::GetData<double>(chunk.data[out_idx]) : nullptr;
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        if (!sink.build_slot_has[off]) {
                            if (dst) validity.SetInvalid(i);
                            else chunk.data[out_idx].SetValue(i, Value());
                            continue;
                        }
                        auto v = (fn == "MIN") ? sink.build_slot_mins[off] : sink.build_slot_maxs[off];
                        if (dst) dst[i] = v;
                        else chunk.data[out_idx].SetValue(i, Value::DOUBLE(v));
                    }
                } else if (out_type == PhysicalType::INT64) {
                    auto *dst = FlatVector::GetData<int64_t>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        dst[i] = (int64_t)sink.build_slot_sums[off];
                    }
                } else if (out_type == PhysicalType::DOUBLE) {
                    auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        dst[i] = sink.build_slot_sums[off];
                    }
                } else {
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        chunk.data[out_idx].SetValue(i, Value::DOUBLE(sink.build_slot_sums[off]));
                    }
                }
            }
            src.pos += batch;
            chunk.SetCardinality(batch);
            return batch ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
        }

        // NATIVE PATH: use GroupedAggregateHashTable::Scan
        if (sink.use_native_ht && sink.agg_ht) {
            if (!src.initialized) {
                src.initialized = true;
                sink.agg_ht->InitializeScan(src.scan_state);
            }
            DataChunk group_rows, agg_rows;
            group_rows.Initialize(ctx.client, group_types);
            // Aggregate output types = payload_types (what the HT stores)
            agg_rows.Initialize(ctx.client, payload_types);
            bool has_data = sink.agg_ht->Scan(src.scan_state, group_rows, agg_rows);
            if (!has_data || group_rows.size() == 0) {
                chunk.SetCardinality(0);
                return SourceResultType::FINISHED;
            }
            // Combine group + agg columns into output
            auto ng = group_types.size();
            for (idx_t g = 0; g < ng; g++) chunk.data[g].Reference(group_rows.data[g]);
            for (idx_t a = 0; a < agg_rows.ColumnCount(); a++) chunk.data[ng + a].Reference(agg_rows.data[a]);
            chunk.SetCardinality(group_rows.size());
            return SourceResultType::HAVE_MORE_OUTPUT;
        }

        // FALLBACK: flat result HT scan
        auto &rht = sink.result_ht;
        if (!src.initialized) {
            src.initialized = true;
            for (idx_t i = 0; i < rht.capacity; i++) {
                if (rht.slot_occupied[i]) src.slot_indices.push_back(i);
            }
        }
        if (src.slot_indices.empty()) {
            // Ungrouped with no matches: emit default row
            if (col.group_cols.empty() && src.pos == 0) {
                auto na = col.agg_funcs.size();
                for (idx_t a = 0; a < na; a++) {
                    if (col.agg_funcs[a] == "COUNT") {
                        if (chunk.data[a].GetType().InternalType() == PhysicalType::INT64)
                            chunk.data[a].SetValue(0, Value::BIGINT(0));
                        else
                            chunk.data[a].SetValue(0, Value(0.0));
                    } else {
                        chunk.data[a].SetValue(0, Value());
                    }
                }
                src.pos = 1;
                chunk.SetCardinality(1);
                return SourceResultType::HAVE_MORE_OUTPUT;
            }
            chunk.SetCardinality(0);
            return SourceResultType::FINISHED;
        }
        idx_t cnt = 0;
        auto na = col.agg_funcs.size();
        auto ng = col.group_cols.size();
        while (src.pos < src.slot_indices.size() && cnt < STANDARD_VECTOR_SIZE) {
            idx_t slot = src.slot_indices[src.pos];
            // Output group columns — typed access from flat arrays
            for (idx_t g = 0; g < ng; g++) {
                bool is_int = g < rht.group_is_int.size() && rht.group_is_int[g];
                if (g < col.group_compress.size() && col.group_compress[g].has_compress &&
                    !col.group_compress[g].is_string_compress) {
                    auto raw_int = rht.GroupInt(g, slot);
                    auto compressed = raw_int - col.group_compress[g].offset;
                    switch (col.group_compress[g].compressed_type.InternalType()) {
                    case PhysicalType::UINT8:
                        FlatVector::GetData<uint8_t>(chunk.data[g])[cnt] = (uint8_t)compressed; break;
                    case PhysicalType::UINT16:
                        FlatVector::GetData<uint16_t>(chunk.data[g])[cnt] = (uint16_t)compressed; break;
                    case PhysicalType::UINT32:
                        FlatVector::GetData<uint32_t>(chunk.data[g])[cnt] = (uint32_t)compressed; break;
                    case PhysicalType::UINT64:
                        FlatVector::GetData<uint64_t>(chunk.data[g])[cnt] = (uint64_t)compressed; break;
                    default:
                        FlatVector::GetData<int32_t>(chunk.data[g])[cnt] = (int32_t)raw_int; break;
                    }
                } else if (rht.GroupUsesValue(g)) {
                    chunk.data[g].SetValue(cnt, rht.GroupVal(g, slot));
                } else if (is_int) {
                    auto out_type = chunk.data[g].GetType().InternalType();
                    auto val = rht.GroupInt(g, slot);
                    switch (out_type) {
                    case PhysicalType::INT32:  FlatVector::GetData<int32_t>(chunk.data[g])[cnt] = (int32_t)val; break;
                    case PhysicalType::INT64:  FlatVector::GetData<int64_t>(chunk.data[g])[cnt] = val; break;
                    case PhysicalType::UINT32: FlatVector::GetData<uint32_t>(chunk.data[g])[cnt] = (uint32_t)val; break;
                    case PhysicalType::UINT64: FlatVector::GetData<uint64_t>(chunk.data[g])[cnt] = (uint64_t)val; break;
                    default: chunk.data[g].SetValue(cnt, Value::BIGINT(val)); break;
                    }
                } else {
                    FlatVector::GetData<double>(chunk.data[g])[cnt] = rht.GroupDbl(g, slot);
                }
            }
            for (idx_t a = 0; a < na; a++) {
                Value v; auto &f = col.agg_funcs[a];
                if ((f == "MIN" || f == "MAX") && rht.UsesValMinMax(a)) {
                    // Value-based MIN/MAX for non-numeric types
                    if (f == "MIN") v = rht.GetHas(slot, a) ? rht.ValMin(slot, a) : Value();
                    else v = rht.GetHas(slot, a) ? rht.ValMax(slot, a) : Value();
                } else if (f == "MIN") {
                    v = rht.GetHas(slot, a) ? Value(rht.Min(slot, a)) : Value();
                } else if (f == "MAX") {
                    v = rht.GetHas(slot, a) ? Value(rht.Max(slot, a)) : Value();
                } else if (f == "AVG") {
                    v = rht.Count(slot, a) > 0 ? Value(rht.Sum(slot, a) / rht.Count(slot, a)) : Value(0.0);
                } else if (chunk.data[ng + a].GetType().InternalType() == PhysicalType::INT64) {
                    v = Value::BIGINT((int64_t)rht.Sum(slot, a));
                } else {
                    v = Value(rht.Sum(slot, a));
                }
                chunk.data[ng + a].SetValue(cnt, v);
            }
            cnt++; src.pos++;
        }
        chunk.SetCardinality(cnt);
        return cnt==0?SourceResultType::FINISHED:SourceResultType::HAVE_MORE_OUTPUT;
    }

    // ── Pipeline: build side sinks, probe side feeds ExecuteInternal ──
    void BuildPipelines(Pipeline &cur, MetaPipeline &mp) override {
        op_state.reset(); sink_state.reset();
        auto &st = mp.GetState();
        st.AddPipelineOperator(cur, *this);
        vector<shared_ptr<Pipeline>> pips;
        mp.GetPipelines(pips, false);
        if (pips.empty()) {
            // Fallback: no pipelines available — should not happen, but guard against crash
            return;
        }
        auto &last = *pips.back();
        auto &bmp = mp.CreateChildMetaPipeline(cur, *this, MetaPipelineType::JOIN_BUILD);
        bmp.Build(children[1].get());
        children[0].get().BuildPipelines(cur, mp);
        mp.CreateChildPipeline(cur, *this, last);
    }

    string GetName() const override { return "AGGJOIN"; }
};

// ============================================================
// LogicalAggJoin
// ============================================================

class LogicalAggJoin : public LogicalExtensionOperator {
public:
    vector<LogicalType> return_types;
    idx_t group_index=0, aggregate_index=0;
    AggJoinColInfo col;
    // Stored for GroupedAggregateHashTable creation
    vector<unique_ptr<Expression>> agg_expressions;  // owned copies
    vector<unique_ptr<Expression>> group_expressions; // owned copies

    vector<ColumnBinding> GetColumnBindings() override {
        vector<ColumnBinding> r;
        idx_t ng = col.group_cols.size();
        idx_t na = col.agg_funcs.size();
        for(idx_t i=0;i<ng;i++) r.emplace_back(group_index, i);
        for(idx_t i=0;i<na;i++) r.emplace_back(aggregate_index, i);
        return r;
    }

    PhysicalOperator &CreatePlan(ClientContext &ctx, PhysicalPlanGenerator &planner) override {
        D_ASSERT(children.size()==2);
        auto &probe = planner.CreatePlan(*children[0]);
        auto &build = planner.CreatePlan(*children[1]);
        auto &ref = planner.Make<PhysicalAggJoin>(return_types, estimated_cardinality);
        auto &phys = ref.Cast<PhysicalAggJoin>();
        phys.children.push_back(probe);
        phys.children.push_back(build);
        phys.col = col;

        // Set up types for GroupedAggregateHashTable.
        // Group types: from group expression return_type (decompressed).
        for (auto &g : group_expressions) {
            phys.group_types.push_back(g->return_type);
        }
        // Payload types: from aggregate child expression return_type.
        for (auto &e : agg_expressions) {
            auto &ba = e->Cast<BoundAggregateExpression>();
            if (!ba.children.empty()) {
                phys.payload_types.push_back(ba.children[0]->return_type);
            } else {
                phys.payload_types.push_back(LogicalType::BIGINT); // COUNT(*)
            }
        }
        // Copy aggregate expressions
        for (auto &e : agg_expressions) {
            phys.owned_agg_exprs.push_back(e->Copy());
        }

        return phys;
    }

    void ResolveTypes() override { types = return_types; }

protected:
    void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) override {
        for(auto &c:children) res.VisitOperator(*c);
        bindings = GetColumnBindings();
    }
};

// ============================================================
// Optimizer: post-optimize with Projection chain tracing
// ============================================================

static bool IsEquiJoin(LogicalOperator &op) {
    if(op.type!=LogicalOperatorType::LOGICAL_COMPARISON_JOIN) return false;
    auto &j=op.Cast<LogicalComparisonJoin>();
    if(j.join_type!=JoinType::INNER) return false;
    for(auto &c:j.conditions) if(c.comparison!=ExpressionType::COMPARE_EQUAL) return false;
    return !j.conditions.empty();
}

static bool IsAggregate(LogicalOperator &op) {
    if(op.type!=LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) return false;
    auto &a=op.Cast<LogicalAggregate>();
    
    // Ungrouped aggregates (no GROUP BY) are supported — single result row
    for(auto &e:a.expressions) {
        if(e->type!=ExpressionType::BOUND_AGGREGATE) { return false; }
        auto &ba = e->Cast<BoundAggregateExpression>();
        auto fn=StringUtil::Upper(ba.function.name);
        if(fn!="SUM"&&fn!="MIN"&&fn!="MAX"&&fn!="COUNT"&&fn!="COUNT_STAR"&&fn!="AVG") return false;
        // SUM/AVG still require the current numeric fast path. MIN/MAX can be
        // admitted here and later lowered back to native if they stay on the
        // old Value-heavy execution path.
        if (fn != "COUNT" && fn != "COUNT_STAR" && fn != "MIN" && fn != "MAX") {
            auto &ret_type = ba.return_type;
            auto phys = ret_type.InternalType();
            if (phys != PhysicalType::DOUBLE && phys != PhysicalType::FLOAT &&
                phys != PhysicalType::INT32 && phys != PhysicalType::INT64 &&
                phys != PhysicalType::INT16 && phys != PhysicalType::INT8) {
                return false; // HUGEINT, DECIMAL, VARCHAR, DATE — can't beat native
            }
        }
        if ((fn == "SUM" || fn == "AVG") &&
            !ba.children.empty() && !ba.children[0]->return_type.IsNumeric()) {
            return false; // Non-numeric aggregate input — bail to native
        }
    }
    return true;
}

// Find Join through Projection chain, return the Join and the Aggregate's child
static LogicalComparisonJoin *FindJoin(LogicalOperator &op) {
    if(IsEquiJoin(op)) return &op.Cast<LogicalComparisonJoin>();
    if(op.type==LogicalOperatorType::LOGICAL_PROJECTION && op.children.size()==1)
        return FindJoin(*op.children[0]);
    return nullptr;
}

static unique_ptr<BoundAggregateExpression> BindAggregateByName(ClientContext &context, const string &name,
                                                               vector<unique_ptr<Expression>> children) {
    auto &entry =
        Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
    FunctionBinder binder(context);
    ErrorData error;
    auto best_function = binder.BindFunction(entry.name, entry.functions, children, error);
    if (!best_function.IsValid()) {
        throw InternalException("Failed to bind aggregate \"%s\": %s", name, error.Message());
    }
    auto bound_function = entry.functions.GetFunctionByOffset(best_function.GetIndex());
    return binder.BindAggregateFunction(bound_function, std::move(children), nullptr, AggregateType::NON_DISTINCT);
}

static bool TryRewriteNativeMixedSidePreagg(ClientContext &context, Optimizer &optimizer,
                                            unique_ptr<LogicalOperator> &op, LogicalAggregate &agg,
                                            LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                            const AggJoinColInfo &col, bool need_swap,
                                            AggJoinRewriteState &state, bool has_parent) {
    struct MixedAggInfo {
        string fn;
        LogicalType result_type;
        bool on_build = false;
        idx_t side_col = DConstants::INVALID_INDEX;
        idx_t sum_slot = DConstants::INVALID_INDEX;
        idx_t count_slot = DConstants::INVALID_INDEX;
        idx_t min_slot = DConstants::INVALID_INDEX;
        idx_t max_slot = DConstants::INVALID_INDEX;
    };

    bool grouped_by_join_key = agg.groups.size() == col.probe_key_cols.size() && !agg.groups.empty();
    bool ungrouped = agg.groups.empty();
    if (!grouped_by_join_key && !ungrouped) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: groups do not match join key\n");
        return false;
    }
    if (col.probe_key_cols.empty() || col.build_key_cols.empty() ||
        col.probe_key_cols.size() != col.build_key_cols.size()) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: key extraction mismatch\n");
        return false;
    }
    if (ungrouped && col.probe_key_cols.size() != 1) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: ungrouped multi-key not enabled\n");
        return false;
    }
    if (!ungrouped && col.group_cols.size() != col.probe_key_cols.size()) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: group count != key count\n");
        return false;
    }
    if (grouped_by_join_key) {
        for (idx_t i = 0; i < col.group_cols.size(); i++) {
            if (col.group_cols[i] != col.probe_key_cols[i]) {
                if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: group cols differ from probe keys\n");
                return false;
            }
        }
    }
    auto key_count = col.probe_key_cols.size();
    if (join.conditions.size() != key_count) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: join condition count != key count\n");
        return false;
    }

    bool saw_payload_on_build = false;
    bool saw_payload_on_probe = false;
    for (idx_t a = 0; a < agg.expressions.size(); a++) {
        auto &ba = agg.expressions[a]->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        if (fn == "COUNT_STAR") {
            saw_payload_on_probe = true;
            saw_payload_on_build = true;
            continue;
        }
        if (a >= col.agg_on_build.size()) {
            return false;
        }
        if (col.agg_on_build[a]) saw_payload_on_build = true;
        else saw_payload_on_probe = true;
    }
    if (!saw_payload_on_build || !saw_payload_on_probe) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: aggregates are not on both sides\n");
        return false;
    }

    auto get_est = [](LogicalOperator &node) -> idx_t {
        return node.has_estimated_cardinality ? node.estimated_cardinality : 0;
    };
    auto probe_est = get_est(*(need_swap ? join.children[1] : join.children[0]));
    auto build_est = get_est(*(need_swap ? join.children[0] : join.children[1]));
    auto group_est = agg.has_estimated_cardinality ? agg.estimated_cardinality : 0;
    if (probe_est == 0 || build_est == 0 || group_est == 0) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: missing estimates\n");
        return false;
    }
    bool large_balanced_shape = probe_est >= 250000 && build_est >= 250000 &&
                                probe_est <= build_est * 4 && build_est <= probe_est * 4;
    bool probe_heavy_single_key = grouped_by_join_key && key_count == 1 &&
                                  probe_est >= 500000 && build_est >= 50000 &&
                                  probe_est >= build_est * 8;
    if (!large_balanced_shape && !probe_heavy_single_key) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: outside balanced envelope\n");
        return false;
    }

    auto join_bindings = join.GetColumnBindings();
    auto child_bindings = agg_child.GetColumnBindings();
    auto &probe_child_ref = *(need_swap ? join.children[1] : join.children[0]);
    auto &build_child_ref = *(need_swap ? join.children[0] : join.children[1]);
    auto probe_payload_bindings = probe_child_ref.GetColumnBindings();
    auto build_payload_bindings = build_child_ref.GetColumnBindings();

    auto resolve_binding = [&](Expression &e) -> idx_t {
        if (e.GetExpressionClass() == ExpressionClass::BOUND_REF) {
            return e.Cast<BoundReferenceExpression>().index;
        }
        if (e.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
            auto &binding = e.Cast<BoundColumnRefExpression>().binding;
            for (idx_t i = 0; i < child_bindings.size(); i++) {
                if (child_bindings[i] == binding) {
                    return i;
                }
            }
        }
        return DConstants::INVALID_INDEX;
    };

    vector<MixedAggInfo> mixed_aggs;
    mixed_aggs.reserve(agg.expressions.size());
    for (idx_t a = 0; a < agg.expressions.size(); a++) {
        auto &ba = agg.expressions[a]->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        MixedAggInfo info;
        info.fn = fn;
        info.result_type = ba.return_type;
        if (fn != "SUM" && fn != "COUNT" && fn != "COUNT_STAR" && fn != "AVG" && fn != "MIN" && fn != "MAX") {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: unsupported agg fn %s\n", fn.c_str());
            return false;
        }
        if (ba.IsDistinct() || ba.filter || ba.order_bys) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: distinct/filter/order agg\n");
            return false;
        }
        if (fn == "COUNT_STAR") {
            mixed_aggs.push_back(std::move(info));
            continue;
        }
        if (ba.children.empty()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: empty agg children\n");
            return false;
        }
        if ((fn == "SUM" || fn == "AVG") && !ba.children[0]->return_type.IsNumeric()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: nonnumeric SUM/AVG input\n");
            return false;
        }
        if (a >= col.agg_on_build.size()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: agg_on_build size mismatch\n");
            return false;
        }
        info.on_build = col.agg_on_build[a];
        auto child_idx = resolve_binding(*ba.children[0]);
        auto join_idx = child_idx == DConstants::INVALID_INDEX ? DConstants::INVALID_INDEX
                                                               : TraceProjectionChain(agg_child, child_idx);
        if (join_idx == DConstants::INVALID_INDEX || join_idx >= join_bindings.size()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: failed to trace agg binding\n");
            return false;
        }
        auto &payload_bindings = info.on_build ? build_payload_bindings : probe_payload_bindings;
        idx_t payload_idx = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < payload_bindings.size(); i++) {
            if (payload_bindings[i] == join_bindings[join_idx]) {
                payload_idx = i;
                break;
            }
        }
        if (payload_idx == DConstants::INVALID_INDEX) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: failed to map payload binding to side child\n");
            return false;
        }
        info.side_col = payload_idx;
        mixed_aggs.push_back(std::move(info));
    }

    auto probe_child = need_swap ? std::move(join.children[1]) : std::move(join.children[0]);
    auto build_child = need_swap ? std::move(join.children[0]) : std::move(join.children[1]);
    auto probe_bindings = probe_child->GetColumnBindings();
    auto build_bindings = build_child->GetColumnBindings();
    auto &probe_types = probe_child->types;
    auto &build_types = build_child->types;

    auto make_side_preagg = [&](unique_ptr<LogicalOperator> child, const vector<ColumnBinding> &bindings,
                                const vector<LogicalType> &types, const vector<idx_t> &key_idxs, bool on_build_side,
                                idx_t &group_index_out, idx_t &agg_index_out) -> unique_ptr<LogicalAggregate> {
        group_index_out = optimizer.binder.GenerateTableIndex();
        agg_index_out = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> side_aggs;
        side_aggs.push_back(BindAggregateByName(context, "count_star", {}));
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            if (info.fn == "COUNT_STAR" || info.on_build != on_build_side) {
                continue;
            }
            auto build_child_ref_expr = [&](idx_t col_idx) {
                return make_uniq<BoundColumnRefExpression>(types[col_idx], bindings[col_idx]);
            };
            if (info.fn == "SUM") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.sum_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            } else if (info.fn == "COUNT") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.count_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
            } else if (info.fn == "MIN") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.min_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "min", std::move(children)));
            } else if (info.fn == "MAX") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.max_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "max", std::move(children)));
            } else if (info.fn == "AVG") {
                {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(build_child_ref_expr(info.side_col));
                    info.sum_slot = side_aggs.size();
                    side_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
                }
                {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(build_child_ref_expr(info.side_col));
                    info.count_slot = side_aggs.size();
                    side_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
                }
            }
        }
        auto side_preagg = make_uniq<LogicalAggregate>(group_index_out, agg_index_out, std::move(side_aggs));
        for (auto key_idx : key_idxs) {
            side_preagg->groups.push_back(make_uniq<BoundColumnRefExpression>(types[key_idx], bindings[key_idx]));
        }
        side_preagg->children.push_back(std::move(child));
        side_preagg->ResolveOperatorTypes();
        return side_preagg;
    };

    idx_t probe_group_index, probe_aggregate_index;
    auto probe_preagg = make_side_preagg(std::move(probe_child), probe_bindings, probe_types, col.probe_key_cols,
                                         false, probe_group_index, probe_aggregate_index);
    idx_t build_group_index, build_aggregate_index;
    auto build_preagg = make_side_preagg(std::move(build_child), build_bindings, build_types, col.build_key_cols,
                                         true, build_group_index, build_aggregate_index);

    auto native_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    for (idx_t i = 0; i < key_count; i++) {
        JoinCondition cond;
        cond.comparison = ExpressionType::COMPARE_EQUAL;
        cond.left = make_uniq<BoundColumnRefExpression>(probe_preagg->types[i], ColumnBinding(probe_group_index, i));
        cond.right = make_uniq<BoundColumnRefExpression>(build_preagg->types[i], ColumnBinding(build_group_index, i));
        if (probe_preagg->types[i] != build_preagg->types[i]) {
            cond.right = BoundCastExpression::AddCastToType(context, std::move(cond.right), probe_preagg->types[i]);
        }
        native_join->conditions.push_back(std::move(cond));
    }
    native_join->children.push_back(std::move(probe_preagg));
    native_join->children.push_back(std::move(build_preagg));
    native_join->ResolveOperatorTypes();

    auto native_join_bindings = native_join->GetColumnBindings();
    auto probe_count_binding = native_join_bindings[key_count];
    auto build_group_offset = native_join->children[0]->types.size();
    auto build_count_binding = native_join_bindings[build_group_offset + key_count];
    idx_t probe_extra_base = key_count;
    idx_t build_extra_base = build_group_offset + key_count;

    auto other_count_ref = [&](MixedAggInfo &info) -> unique_ptr<Expression> {
        auto binding = info.on_build ? probe_count_binding : build_count_binding;
        auto ref = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, binding);
        return BoundCastExpression::AddCastToType(context, std::move(ref), info.result_type);
    };
    auto side_slot_binding = [&](MixedAggInfo &info, idx_t slot) -> ColumnBinding {
        auto idx = (info.on_build ? build_extra_base : probe_extra_base) + slot;
        return native_join_bindings[idx];
    };
    auto side_slot_type = [&](MixedAggInfo &info, idx_t slot) -> const LogicalType & {
        auto idx = (info.on_build ? build_extra_base : probe_extra_base) + slot;
        return native_join->types[idx];
    };

    unique_ptr<LogicalOperator> replacement;
    idx_t output_index = DConstants::INVALID_INDEX;
    idx_t output_base = 0;

    if (grouped_by_join_key) {
        auto proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> proj_exprs;
        proj_exprs.reserve(key_count + agg.expressions.size());
        for (idx_t i = 0; i < key_count; i++) {
            auto group_expr = make_uniq<BoundColumnRefExpression>(native_join->types[i], native_join_bindings[i]);
            proj_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(group_expr), agg.types[i]));
        }
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            auto result_type = info.result_type;

            unique_ptr<Expression> final_expr;
            if (info.fn == "COUNT_STAR") {
                auto left = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, probe_count_binding);
                auto right = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, build_count_binding);
                auto cast_left = BoundCastExpression::AddCastToType(context, std::move(left), result_type);
                auto cast_right = BoundCastExpression::AddCastToType(context, std::move(right), result_type);
                final_expr = optimizer.BindScalarFunction("*", std::move(cast_left), std::move(cast_right));
            } else if (info.fn == "AVG") {
                auto sum_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.sum_slot),
                                                                   side_slot_binding(info, info.sum_slot));
                auto count_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.count_slot),
                                                                     side_slot_binding(info, info.count_slot));
                auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(sum_ref), result_type);
                auto cast_count = BoundCastExpression::AddCastToType(context, std::move(count_ref), result_type);
                final_expr = optimizer.BindScalarFunction("/", std::move(cast_sum), std::move(cast_count));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                final_expr = BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type);
            } else {
                idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                auto multiplier = other_count_ref(info);
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                auto cast_side = BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type);
                final_expr = optimizer.BindScalarFunction("*", std::move(multiplier), std::move(cast_side));
            }
            proj_exprs.push_back(std::move(final_expr));
        }
        auto proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_exprs));
        proj->children.push_back(std::move(native_join));
        proj->ResolveOperatorTypes();
        if (op->has_estimated_cardinality) {
            proj->SetEstimatedCardinality(op->estimated_cardinality);
        }
        output_index = proj_index;
        output_base = key_count;
        replacement = std::move(proj);
    } else {
        auto contrib_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> contrib_exprs;
        vector<idx_t> sum_slots(mixed_aggs.size(), DConstants::INVALID_INDEX);
        vector<idx_t> count_slots(mixed_aggs.size(), DConstants::INVALID_INDEX);
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            auto result_type = info.result_type;
            if (info.fn == "COUNT_STAR") {
                auto left = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, probe_count_binding);
                auto right = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, build_count_binding);
                auto cast_left = BoundCastExpression::AddCastToType(context, std::move(left), result_type);
                auto cast_right = BoundCastExpression::AddCastToType(context, std::move(right), result_type);
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(cast_left), std::move(cast_right)));
            } else if (info.fn == "AVG") {
                auto mul_num = other_count_ref(info);
                auto sum_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.sum_slot),
                                                                   side_slot_binding(info, info.sum_slot));
                auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(sum_ref), result_type);
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul_num), std::move(cast_sum)));

                auto mul_den = other_count_ref(info);
                auto count_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.count_slot),
                                                                     side_slot_binding(info, info.count_slot));
                auto cast_count = BoundCastExpression::AddCastToType(context, std::move(count_ref), result_type);
                count_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul_den), std::move(cast_count)));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type));
            } else {
                idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                auto mul = other_count_ref(info);
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                auto cast_side = BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type);
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul), std::move(cast_side)));
            }
        }
        auto contrib_proj = make_uniq<LogicalProjection>(contrib_index, std::move(contrib_exprs));
        contrib_proj->children.push_back(std::move(native_join));
        contrib_proj->ResolveOperatorTypes();

        auto final_agg_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_aggs;
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            if (info.fn == "AVG") {
                for (auto slot : {sum_slots[a], count_slots[a]}) {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[slot],
                                                                           ColumnBinding(contrib_index, slot)));
                    final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
                }
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[sum_slots[a]],
                                                                       ColumnBinding(contrib_index, sum_slots[a])));
                final_aggs.push_back(BindAggregateByName(context, StringUtil::Lower(info.fn), std::move(children)));
            } else {
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[sum_slots[a]],
                                                                       ColumnBinding(contrib_index, sum_slots[a])));
                final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
        }
        auto final_agg = make_uniq<LogicalAggregate>(DConstants::INVALID_INDEX, final_agg_index, std::move(final_aggs));
        final_agg->children.push_back(std::move(contrib_proj));
        final_agg->ResolveOperatorTypes();

        auto final_proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_exprs;
        final_exprs.reserve(mixed_aggs.size());
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            unique_ptr<Expression> final_expr;
            if (info.fn == "AVG") {
                auto num_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[sum_slots[a]],
                                                                   ColumnBinding(final_agg_index, sum_slots[a]));
                auto den_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[count_slots[a]],
                                                                   ColumnBinding(final_agg_index, count_slots[a]));
                auto cast_num = BoundCastExpression::AddCastToType(context, std::move(num_ref), info.result_type);
                auto cast_den = BoundCastExpression::AddCastToType(context, std::move(den_ref), info.result_type);
                final_expr = optimizer.BindScalarFunction("/", std::move(cast_num), std::move(cast_den));
            } else {
                auto sum_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[sum_slots[a]],
                                                                   ColumnBinding(final_agg_index, sum_slots[a]));
                final_expr = BoundCastExpression::AddCastToType(context, std::move(sum_ref), info.result_type);
            }
            final_exprs.push_back(std::move(final_expr));
        }
        auto final_proj = make_uniq<LogicalProjection>(final_proj_index, std::move(final_exprs));
        final_proj->children.push_back(std::move(final_agg));
        final_proj->ResolveOperatorTypes();
        if (op->has_estimated_cardinality) {
            final_proj->SetEstimatedCardinality(op->estimated_cardinality);
        }
        output_index = final_proj_index;
        output_base = 0;
        replacement = std::move(final_proj);
    }

    if (has_parent) {
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.aggregate_index, a),
                                                    ColumnBinding(output_index, output_base + a));
        }
        if (grouped_by_join_key) {
            for (idx_t i = 0; i < key_count; i++) {
                state.replacement_bindings.emplace_back(ColumnBinding(agg.group_index, i),
                                                        ColumnBinding(output_index, i));
            }
        }
    }
    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] planner rewrite: native mixed-side preagg (join_conds=%zu, groups=%zu, aggs=%zu, probe_est=%llu, build_est=%llu, group_est=%llu)\n",
                join.conditions.size(), agg.groups.size(), agg.expressions.size(), (unsigned long long)probe_est,
                (unsigned long long)build_est, (unsigned long long)group_est);
    }
    op = std::move(replacement);
    return true;
}

static bool TryRewriteNativeBuildPreagg(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                        LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                        const AggJoinColInfo &col, idx_t build_agg_count, bool need_swap,
                                        AggJoinRewriteState &state, bool has_parent) {
    struct NativeBuildAggInfo {
        string fn;
        LogicalType result_type;
        idx_t build_col = DConstants::INVALID_INDEX;
        idx_t sum_slot = DConstants::INVALID_INDEX;
        idx_t count_slot = DConstants::INVALID_INDEX;
        idx_t min_slot = DConstants::INVALID_INDEX;
        idx_t max_slot = DConstants::INVALID_INDEX;
    };
    if (AggJoinTraceEnabled()) {
        fprintf(stderr, "[AGGJOIN] native-build-preagg candidate: groups=%zu aggs=%zu build_aggs=%zu need_swap=%d agg_on_build=",
                agg.groups.size(), agg.expressions.size(), build_agg_count, need_swap ? 1 : 0);
        for (idx_t a = 0; a < col.agg_on_build.size(); a++) {
            fprintf(stderr, "%s%d", a == 0 ? "" : ",", col.agg_on_build[a] ? 1 : 0);
        }
        fprintf(stderr, "\n");
    }
    bool grouped_by_join_key = agg.groups.size() == 1;
    bool ungrouped = agg.groups.empty();
    if (join.conditions.size() != 1 || (!grouped_by_join_key && !ungrouped)) {
        return false;
    }
    if ((!ungrouped && col.group_cols.size() != 1) || col.probe_key_cols.size() != 1 || col.build_key_cols.size() != 1) {
        return false;
    }
    if (grouped_by_join_key && col.group_cols[0] != col.probe_key_cols[0]) {
        return false;
    }
    bool saw_payload_on_build = false;
    bool saw_payload_on_probe = false;
    for (idx_t a = 0; a < agg.expressions.size(); a++) {
        auto &ba = agg.expressions[a]->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        if (fn == "COUNT_STAR") {
            continue;
        }
        if (a >= col.agg_on_build.size()) {
            return false;
        }
        if (col.agg_on_build[a]) {
            saw_payload_on_build = true;
        } else {
            saw_payload_on_probe = true;
        }
    }
    if (!saw_payload_on_build && !saw_payload_on_probe) {
        return false;
    }
    if (saw_payload_on_build && saw_payload_on_probe) {
        return false;
    }
    bool payload_on_build = saw_payload_on_build;
    auto get_est = [](LogicalOperator &node) -> idx_t {
        return node.has_estimated_cardinality ? node.estimated_cardinality : 0;
    };
    auto probe_est = get_est(*(need_swap ? join.children[1] : join.children[0]));
    auto build_est = get_est(*(need_swap ? join.children[0] : join.children[1]));
    auto group_est = agg.has_estimated_cardinality ? agg.estimated_cardinality : 0;
    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] native-build-preagg estimates: probe_est=%llu build_est=%llu group_est=%llu\n",
                (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est);
    }
    if (probe_est == 0 || build_est == 0 || group_est == 0) {
        return false;
    }
    bool large_balanced_shape = probe_est >= 250000 && build_est >= 250000 &&
                                probe_est <= build_est * 4 && build_est <= probe_est * 4;
    if (!large_balanced_shape) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] native-build-preagg skip: outside balanced build-heavy envelope\n");
        }
        return false;
    }
    auto join_bindings = join.GetColumnBindings();
    auto child_bindings = agg_child.GetColumnBindings();
    auto &payload_child_ref = payload_on_build ? *(need_swap ? join.children[0] : join.children[1])
                                               : *(need_swap ? join.children[1] : join.children[0]);
    auto payload_bindings = payload_child_ref.GetColumnBindings();
    vector<NativeBuildAggInfo> native_aggs;
    native_aggs.reserve(agg.expressions.size());
    auto resolve_binding = [&](Expression &e) -> idx_t {
        if (e.GetExpressionClass() == ExpressionClass::BOUND_REF) {
            return e.Cast<BoundReferenceExpression>().index;
        }
        if (e.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
            auto &binding = e.Cast<BoundColumnRefExpression>().binding;
            for (idx_t i = 0; i < child_bindings.size(); i++) {
                if (child_bindings[i] == binding) {
                    return i;
                }
            }
        }
        return DConstants::INVALID_INDEX;
    };
    for (idx_t a = 0; a < agg.expressions.size(); a++) {
        auto &ba = agg.expressions[a]->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        NativeBuildAggInfo info;
        info.fn = fn;
        info.result_type = ba.return_type;
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] native-build-preagg agg%llu fn=%s child_count=%zu\n",
                    (unsigned long long)a, fn.c_str(), ba.children.size());
        }
        if (fn != "SUM" && fn != "COUNT" && fn != "COUNT_STAR" && fn != "AVG" &&
            fn != "MIN" && fn != "MAX") {
            return false;
        }
        if (ba.IsDistinct() || ba.filter || ba.order_bys) {
            return false;
        }
        if (fn == "COUNT_STAR") {
            native_aggs.push_back(std::move(info));
            continue;
        }
        if (ba.children.empty()) {
            return false;
        }
        if ((fn == "SUM" || fn == "AVG") && !ba.children[0]->return_type.IsNumeric()) {
            return false;
        }
        auto child_idx = resolve_binding(*ba.children[0]);
        auto join_idx = child_idx == DConstants::INVALID_INDEX ? DConstants::INVALID_INDEX
                                                               : TraceProjectionChain(agg_child, child_idx);
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] native-build-preagg agg%llu child_idx=%llu join_idx=%llu\n",
                    (unsigned long long)a, (unsigned long long)child_idx, (unsigned long long)join_idx);
        }
        if (join_idx == DConstants::INVALID_INDEX || join_idx >= join_bindings.size()) {
            return false;
        }
        idx_t payload_idx = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < payload_bindings.size(); i++) {
            if (payload_bindings[i] == join_bindings[join_idx]) {
                payload_idx = i;
                break;
            }
        }
        if (payload_idx == DConstants::INVALID_INDEX) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr, "[AGGJOIN] native-build-preagg agg%llu not on chosen build side\n",
                        (unsigned long long)a);
            }
            return false;
        }
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] native-build-preagg agg%llu build_idx=%llu\n",
                    (unsigned long long)a, (unsigned long long)payload_idx);
        }
        info.build_col = payload_idx;
        native_aggs.push_back(std::move(info));
    }

    auto probe_child = need_swap ? std::move(join.children[1]) : std::move(join.children[0]);
    auto build_child = need_swap ? std::move(join.children[0]) : std::move(join.children[1]);
    auto probe_bindings = probe_child->GetColumnBindings();
    auto build_bindings = build_child->GetColumnBindings();
    auto &probe_types = probe_child->types;
    auto &build_types = build_child->types;

    auto payload_key_idx = payload_on_build ? col.build_key_cols[0] : col.probe_key_cols[0];
    auto count_key_idx = payload_on_build ? col.probe_key_cols[0] : col.build_key_cols[0];
    auto &payload_types = payload_on_build ? build_types : probe_types;
    auto &count_types = payload_on_build ? probe_types : build_types;
    auto &payload_side_bindings = payload_on_build ? build_bindings : probe_bindings;
    auto &count_side_bindings = payload_on_build ? probe_bindings : build_bindings;

    auto payload_child = payload_on_build ? std::move(build_child) : std::move(probe_child);
    auto count_child = payload_on_build ? std::move(probe_child) : std::move(build_child);

    auto count_group_index = optimizer.binder.GenerateTableIndex();
    auto count_aggregate_index = optimizer.binder.GenerateTableIndex();
    vector<unique_ptr<Expression>> count_aggs;
    count_aggs.push_back(BindAggregateByName(context, "count", {}));
    auto count_preagg = make_uniq<LogicalAggregate>(count_group_index, count_aggregate_index, std::move(count_aggs));
    count_preagg->groups.push_back(make_uniq<BoundColumnRefExpression>(
        count_types[count_key_idx], count_side_bindings[count_key_idx]));
    count_preagg->children.push_back(std::move(count_child));
    count_preagg->ResolveOperatorTypes();

    auto build_group_index = optimizer.binder.GenerateTableIndex();
    auto build_aggregate_index = optimizer.binder.GenerateTableIndex();
    vector<unique_ptr<Expression>> build_aggs;
    build_aggs.reserve(agg.expressions.size() * 2);
    for (idx_t a = 0; a < agg.expressions.size(); a++) {
        auto &info = native_aggs[a];
        auto fn = info.fn;
        if (fn == "SUM") {
            vector<unique_ptr<Expression>> children;
            auto build_col = info.build_col;
            children.push_back(
                make_uniq<BoundColumnRefExpression>(payload_types[build_col], payload_side_bindings[build_col]));
            info.sum_slot = build_aggs.size();
            build_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
        } else if (fn == "COUNT") {
            vector<unique_ptr<Expression>> children;
            auto build_col = info.build_col;
            children.push_back(
                make_uniq<BoundColumnRefExpression>(payload_types[build_col], payload_side_bindings[build_col]));
            info.count_slot = build_aggs.size();
            build_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
        } else if (fn == "COUNT_STAR") {
            info.count_slot = build_aggs.size();
            build_aggs.push_back(BindAggregateByName(context, "count_star", {}));
        } else if (fn == "MIN") {
            vector<unique_ptr<Expression>> children;
            auto build_col = info.build_col;
            children.push_back(
                make_uniq<BoundColumnRefExpression>(payload_types[build_col], payload_side_bindings[build_col]));
            info.min_slot = build_aggs.size();
            build_aggs.push_back(BindAggregateByName(context, "min", std::move(children)));
        } else if (fn == "MAX") {
            vector<unique_ptr<Expression>> children;
            auto build_col = info.build_col;
            children.push_back(
                make_uniq<BoundColumnRefExpression>(payload_types[build_col], payload_side_bindings[build_col]));
            info.max_slot = build_aggs.size();
            build_aggs.push_back(BindAggregateByName(context, "max", std::move(children)));
        } else if (fn == "AVG") {
            {
                vector<unique_ptr<Expression>> children;
                auto build_col = info.build_col;
                children.push_back(make_uniq<BoundColumnRefExpression>(payload_types[build_col],
                                                                       payload_side_bindings[build_col]));
                info.sum_slot = build_aggs.size();
                build_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
            {
                vector<unique_ptr<Expression>> children;
                auto build_col = info.build_col;
                children.push_back(make_uniq<BoundColumnRefExpression>(payload_types[build_col],
                                                                       payload_side_bindings[build_col]));
                info.count_slot = build_aggs.size();
                build_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
            }
        }
    }
    auto build_preagg =
        make_uniq<LogicalAggregate>(build_group_index, build_aggregate_index, std::move(build_aggs));
    build_preagg->groups.push_back(make_uniq<BoundColumnRefExpression>(
        payload_types[payload_key_idx], payload_side_bindings[payload_key_idx]));
    build_preagg->children.push_back(std::move(payload_child));
    build_preagg->ResolveOperatorTypes();

    auto native_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    JoinCondition cond;
    cond.comparison = ExpressionType::COMPARE_EQUAL;
    cond.left = make_uniq<BoundColumnRefExpression>(count_preagg->types[0], ColumnBinding(count_group_index, 0));
    cond.right = make_uniq<BoundColumnRefExpression>(build_preagg->types[0], ColumnBinding(build_group_index, 0));
    if (count_preagg->types[0] != build_preagg->types[0]) {
        cond.right = BoundCastExpression::AddCastToType(context, std::move(cond.right), count_preagg->types[0]);
    }
    native_join->conditions.push_back(std::move(cond));
    native_join->children.push_back(std::move(count_preagg));
    native_join->children.push_back(std::move(build_preagg));
    native_join->ResolveOperatorTypes();

    auto native_join_bindings = native_join->GetColumnBindings();
    auto probe_count_binding = native_join_bindings[1];
    auto build_base = 2 + 1; // left(group,count) + right(group)

    unique_ptr<LogicalOperator> replacement;
    idx_t output_index = DConstants::INVALID_INDEX;
    idx_t output_base = 0;

    if (grouped_by_join_key) {
        auto proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> proj_exprs;
        proj_exprs.reserve(1 + agg.expressions.size());
        {
            auto group_expr =
                make_uniq<BoundColumnRefExpression>(native_join->types[0], native_join_bindings[0]);
            proj_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(group_expr), agg.types[0]));
        }
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            auto &info = native_aggs[a];
            auto fn = info.fn;
            auto result_type = info.result_type;
            unique_ptr<Expression> final_expr;
            if (fn == "AVG") {
                auto build_sum = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + info.sum_slot],
                                                                     native_join_bindings[build_base + info.sum_slot]);
                auto build_count =
                    make_uniq<BoundColumnRefExpression>(native_join->types[build_base + info.count_slot],
                                                        native_join_bindings[build_base + info.count_slot]);
                auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(build_sum), result_type);
                auto cast_count = BoundCastExpression::AddCastToType(context, std::move(build_count), result_type);
                final_expr = optimizer.BindScalarFunction("/", std::move(cast_sum), std::move(cast_count));
            } else if (fn == "MIN" || fn == "MAX") {
                idx_t slot = fn == "MIN" ? info.min_slot : info.max_slot;
                auto build_expr = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + slot],
                                                                      native_join_bindings[build_base + slot]);
                final_expr = BoundCastExpression::AddCastToType(context, std::move(build_expr), result_type);
            } else {
                auto probe_count_ref =
                    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, probe_count_binding);
                auto cast_count =
                    BoundCastExpression::AddCastToType(context, std::move(probe_count_ref), result_type);
                idx_t slot = fn == "SUM" ? info.sum_slot : info.count_slot;
                auto build_expr = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + slot],
                                                                      native_join_bindings[build_base + slot]);
                auto cast_value = BoundCastExpression::AddCastToType(context, std::move(build_expr), result_type);
                final_expr = optimizer.BindScalarFunction("*", std::move(cast_count), std::move(cast_value));
            }
            proj_exprs.push_back(std::move(final_expr));
        }
        auto proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_exprs));
        proj->children.push_back(std::move(native_join));
        proj->ResolveOperatorTypes();
        if (op->has_estimated_cardinality) {
            proj->SetEstimatedCardinality(op->estimated_cardinality);
        }
        output_index = proj_index;
        output_base = 1;
        replacement = std::move(proj);
    } else {
        auto contrib_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> contrib_exprs;
        contrib_exprs.reserve(agg.expressions.size() + 2);
        vector<idx_t> sum_slots(agg.expressions.size(), DConstants::INVALID_INDEX);
        vector<idx_t> count_slots(agg.expressions.size(), DConstants::INVALID_INDEX);
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            auto &info = native_aggs[a];
            auto result_type = info.result_type;
            auto build_probe_multiplier = [&]() {
                auto probe_count_ref =
                    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, probe_count_binding);
                return BoundCastExpression::AddCastToType(context, std::move(probe_count_ref), result_type);
            };
            if (info.fn == "AVG") {
                auto num_mul = build_probe_multiplier();
                auto build_sum = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + info.sum_slot],
                                                                     native_join_bindings[build_base + info.sum_slot]);
                auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(build_sum), result_type);
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(num_mul), std::move(cast_sum)));

                auto den_mul = build_probe_multiplier();
                auto build_count =
                    make_uniq<BoundColumnRefExpression>(native_join->types[build_base + info.count_slot],
                                                        native_join_bindings[build_base + info.count_slot]);
                auto cast_count = BoundCastExpression::AddCastToType(context, std::move(build_count), result_type);
                count_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(den_mul), std::move(cast_count)));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                auto build_expr = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + slot],
                                                                      native_join_bindings[build_base + slot]);
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(build_expr), result_type));
            } else {
                auto mul = build_probe_multiplier();
                idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                auto build_expr = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + slot],
                                                                      native_join_bindings[build_base + slot]);
                auto cast_value = BoundCastExpression::AddCastToType(context, std::move(build_expr), result_type);
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul), std::move(cast_value)));
            }
        }
        auto contrib_proj = make_uniq<LogicalProjection>(contrib_index, std::move(contrib_exprs));
        contrib_proj->children.push_back(std::move(native_join));
        contrib_proj->ResolveOperatorTypes();

        auto final_agg_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_aggs;
        final_aggs.reserve(contrib_proj->types.size());
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            auto &info = native_aggs[a];
            auto fn = info.fn;
            if (fn == "AVG") {
                for (auto slot : {sum_slots[a], count_slots[a]}) {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[slot],
                                                                           ColumnBinding(contrib_index, slot)));
                    final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
                }
            } else if (fn == "MIN" || fn == "MAX") {
                auto slot = sum_slots[a];
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[slot],
                                                                       ColumnBinding(contrib_index, slot)));
                final_aggs.push_back(BindAggregateByName(context, StringUtil::Lower(fn), std::move(children)));
            } else {
                auto slot = sum_slots[a];
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[slot],
                                                                       ColumnBinding(contrib_index, slot)));
                final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
        }
        auto final_agg = make_uniq<LogicalAggregate>(DConstants::INVALID_INDEX, final_agg_index, std::move(final_aggs));
        final_agg->children.push_back(std::move(contrib_proj));
        final_agg->ResolveOperatorTypes();

        auto final_proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_exprs;
        final_exprs.reserve(agg.expressions.size());
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            auto &info = native_aggs[a];
            auto result_type = info.result_type;
            unique_ptr<Expression> final_expr;
            if (info.fn == "AVG") {
                auto num_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[sum_slots[a]],
                                                                   ColumnBinding(final_agg_index, sum_slots[a]));
                auto den_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[count_slots[a]],
                                                                   ColumnBinding(final_agg_index, count_slots[a]));
                auto cast_num = BoundCastExpression::AddCastToType(context, std::move(num_ref), result_type);
                auto cast_den = BoundCastExpression::AddCastToType(context, std::move(den_ref), result_type);
                final_expr = optimizer.BindScalarFunction("/", std::move(cast_num), std::move(cast_den));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                auto sum_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[sum_slots[a]],
                                                                   ColumnBinding(final_agg_index, sum_slots[a]));
                final_expr = BoundCastExpression::AddCastToType(context, std::move(sum_ref), result_type);
            } else {
                auto sum_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[sum_slots[a]],
                                                                   ColumnBinding(final_agg_index, sum_slots[a]));
                final_expr = BoundCastExpression::AddCastToType(context, std::move(sum_ref), result_type);
            }
            final_exprs.push_back(std::move(final_expr));
        }
        auto final_proj = make_uniq<LogicalProjection>(final_proj_index, std::move(final_exprs));
        final_proj->children.push_back(std::move(final_agg));
        final_proj->ResolveOperatorTypes();
        output_index = final_proj_index;
        output_base = 0;
        replacement = std::move(final_proj);
    }

    if (has_parent) {
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.aggregate_index, a),
                                                    ColumnBinding(output_index, output_base + a));
        }
        if (grouped_by_join_key) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.group_index, 0), ColumnBinding(output_index, 0));
        }
    }
    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] planner rewrite: native build preagg (join_conds=%zu, groups=%zu, aggs=%zu, build_aggs=%zu, probe_est=%llu, build_est=%llu, group_est=%llu)\n",
                join.conditions.size(), agg.groups.size(), agg.expressions.size(), build_agg_count,
                (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est);
    }
    op = std::move(replacement);
    return true;
}

static void WalkAndReplace(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                           AggJoinRewriteState &state, bool has_parent) {
    for (auto &c : op->children) {
        WalkAndReplace(context, optimizer, c, state, true);
    }
    if(!IsAggregate(*op)||op->children.size()!=1) return;
    auto *join = FindJoin(*op->children[0]);
    if(!join) { return; }
    

    auto &agg = op->Cast<LogicalAggregate>();

    // ── Planner gating: bail on shapes where AGGJOIN is unlikely to beat native ──
    // Disable at compile time with -DAGGJOIN_NO_PLANNER_GATE for testing/benchmarking.
    // When disabled, logs what WOULD have been gated for accuracy analysis.
    {
        const char *gate_reason = nullptr;
        if (join->conditions.size() >= 4) gate_reason = "4+ join conditions";
        else if (agg.expressions.empty()) gate_reason = "no aggregate functions";
        else if (agg.groups.size() > 4) gate_reason = "4+ GROUP BY columns";
        if (gate_reason) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr, "[AGGJOIN] planner gate would bail: %s (join_conds=%zu, aggs=%zu, groups=%zu)\n",
                        gate_reason, join->conditions.size(), agg.expressions.size(), agg.groups.size());
            }
#ifndef AGGJOIN_NO_PLANNER_GATE
            return;
#else
            fprintf(stderr, "[AGGJOIN] planner gate would bail: %s (join_conds=%zu, aggs=%zu, groups=%zu)\n",
                    gate_reason, join->conditions.size(), agg.expressions.size(), agg.groups.size());
#endif
        }
    }

    auto &agg_child = *op->children[0]; // Projection chain above Join

    // Trace each aggregate group/input index through Projection chain → Join output index
    // The Join's output is PRUNED — it only includes columns actually needed.
    // We need to map Join output positions to actual scan column indices.
    auto join_bindings = join->GetColumnBindings();
    auto probe_bindings = join->children[0]->GetColumnBindings();
    auto build_bindings = join->children[1]->GetColumnBindings();
    auto probe_cols = join->children[0]->types.size();

    // Map a join output position to (is_probe, scan_col_index)
    auto resolveJoinCol = [&](idx_t join_pos) -> std::pair<bool, idx_t> {
        if (join_pos >= join_bindings.size()) return {false, DConstants::INVALID_INDEX};
        auto &b = join_bindings[join_pos];
        for (idx_t i = 0; i < probe_bindings.size(); i++) {
            if (probe_bindings[i] == b) return {true, i}; // Probe side, scan index i
        }
        for (idx_t i = 0; i < build_bindings.size(); i++) {
            if (build_bindings[i] == b) return {false, i}; // Build side, scan index i
        }
        return {false, DConstants::INVALID_INDEX};
    };

    AggJoinColInfo col;
    col.probe_col_count = probe_cols;

    // Resolve BoundColumnRefExpression to physical position in the child's output.
    // At post-optimize, group/aggregate expressions are BoundColumnRefExpression
    // with (table_index, column_index). We need to find the POSITION in the child
    // operator's GetColumnBindings() that matches this binding.
    auto child_bindings = agg_child.GetColumnBindings();
    auto resolveBinding = [&](Expression &e) -> idx_t {
        if (e.GetExpressionClass() == ExpressionClass::BOUND_REF) {
            return e.Cast<BoundReferenceExpression>().index;
        }
        if (e.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
            auto &binding = e.Cast<BoundColumnRefExpression>().binding;
            for (idx_t i = 0; i < child_bindings.size(); i++) {
                if (child_bindings[i] == binding) return i;
            }
        }
        return DConstants::INVALID_INDEX;
    };

    // First pass: resolve all group and agg columns to (is_probe, scan_idx) pairs.
    // Then determine if we need to swap children.
    struct ResolvedCol { bool is_probe; idx_t scan_idx; CompressInfo compress; };
    vector<ResolvedCol> resolved_groups, resolved_aggs;

    for(idx_t gi = 0; gi < agg.groups.size(); gi++) {
        auto &g = agg.groups[gi];
        auto agg_idx = resolveBinding(*g);
        if (agg_idx == DConstants::INVALID_INDEX) { return; }
        auto ci = FindCompressInChain(agg_child, agg_idx);
        auto join_idx = TraceProjectionChain(agg_child, agg_idx);
        if(join_idx==DConstants::INVALID_INDEX) return;
        if (join_idx >= join_bindings.size()) return;
        auto &b = join_bindings[join_idx];
        bool is_p = false; idx_t si = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < probe_bindings.size(); i++) {
            if (probe_bindings[i] == b) { is_p = true; si = i; break; }
        }
        if (!is_p) {
            for (idx_t i = 0; i < build_bindings.size(); i++) {
                if (build_bindings[i] == b) { si = i; break; }
            }
        }
        if (si == DConstants::INVALID_INDEX) { return; }
        resolved_groups.push_back({is_p, si, ci});
       
    }

    for(auto &e : agg.expressions) {
        auto &ba = e->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        // Normalize COUNT_STAR → COUNT for uniform handling
        if (fn == "COUNT_STAR") fn = "COUNT";
        col.agg_funcs.push_back(fn);
        bool is_numeric = true;
        if (!ba.children.empty() && !ba.children[0]->return_type.IsNumeric()) {
            is_numeric = false; // VARCHAR, DATE, etc.
        }
        col.agg_is_numeric.push_back(is_numeric);
        idx_t agg_child_idx = DConstants::INVALID_INDEX;
        if (!ba.children.empty()) {
            agg_child_idx = resolveBinding(*ba.children[0]);
        }
        if (agg_child_idx == DConstants::INVALID_INDEX) {
            resolved_aggs.push_back({true, DConstants::INVALID_INDEX, {}}); // COUNT(*)
            continue;
        }
        auto join_idx = TraceProjectionChain(agg_child, agg_child_idx);
        if (join_idx == DConstants::INVALID_INDEX || join_idx >= join_bindings.size()) {
            resolved_aggs.push_back({true, DConstants::INVALID_INDEX, {}});
            continue;
        }
        auto &b = join_bindings[join_idx];
        bool is_p = false; idx_t si = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < probe_bindings.size(); i++) {
            if (probe_bindings[i] == b) { is_p = true; si = i; break; }
        }
        if (!is_p) {
            for (idx_t i = 0; i < build_bindings.size(); i++) {
                if (build_bindings[i] == b) { si = i; break; }
            }
        }
        resolved_aggs.push_back({is_p, si, {}});
    }

    // Determine if ALL group columns are on the build side.
    // If so, swap probe/build so groups are accessible from ExecuteInternal.
    // Empty groups (ungrouped aggregate) → treat as "all on probe" (no swap needed)
    bool all_groups_build = !resolved_groups.empty();
    bool all_groups_probe = true; // Empty groups trivially satisfied
    for (auto &rg : resolved_groups) {
        if (rg.is_probe) all_groups_build = false;
        else all_groups_probe = false;
    }
    bool need_swap = all_groups_build;
    // With build-side aggregate access, we CAN swap even when agg inputs are on probe
    // (they become build-side after swap, handled via build-side accumulation in Sink).
    // Bail if groups span both sides.
    if (!all_groups_probe && !all_groups_build) { return; }

    if (need_swap) {
        // Swap bindings and probe_cols
        std::swap(probe_bindings, build_bindings);
        probe_cols = probe_bindings.size(); // Not types.size() — use binding count
        col.probe_col_count = probe_cols;
        // Flip is_probe for all resolved columns
        for (auto &rg : resolved_groups) rg.is_probe = true;
        for (auto &ra : resolved_aggs) {
            if (ra.scan_idx != DConstants::INVALID_INDEX) ra.is_probe = !ra.is_probe;
        }
    }

    // Bail if any group is still on build side after potential swap
    for (idx_t i = 0; i < resolved_groups.size(); i++) {
        auto &rg = resolved_groups[i];
        if (!rg.is_probe || rg.scan_idx == DConstants::INVALID_INDEX) { return; }
    }
    // Populate col from resolved columns.
    // Build-side aggregate inputs are now supported — accumulated during Sink.
    for (auto &rg : resolved_groups) {
        col.group_compress.push_back(rg.compress);
        col.group_cols.push_back(rg.scan_idx);
    }
    // Count build-side aggregates for build-side storage sizing.
    idx_t build_agg_count = 0;
    for (auto &ra : resolved_aggs) {
        if (ra.scan_idx != DConstants::INVALID_INDEX && !ra.is_probe) build_agg_count++;
    }

    bool has_nonnumeric_minmax = false;
    {
        idx_t ra_idx = 0;
        for (auto &e : agg.expressions) {
            auto &ba = e->Cast<BoundAggregateExpression>();
            auto fn = StringUtil::Upper(ba.function.name);
            if (ra_idx < resolved_aggs.size()) {
                auto &ra = resolved_aggs[ra_idx];
                if (ra.scan_idx != DConstants::INVALID_INDEX && (fn == "MIN" || fn == "MAX")) {
                    if (!ba.children.empty() && !ba.children[0]->return_type.IsNumeric()) {
                        has_nonnumeric_minmax = true;
                        break;
                    }
                }
            }
            ra_idx++;
        }
    }

    for (auto &ra : resolved_aggs) {
        bool on_build = (ra.scan_idx != DConstants::INVALID_INDEX && !ra.is_probe);
        col.agg_on_build.push_back(on_build);
        if (ra.scan_idx == DConstants::INVALID_INDEX) {
            col.agg_input_cols.push_back(DConstants::INVALID_INDEX);
            col.build_agg_cols.push_back(DConstants::INVALID_INDEX);
        } else if (on_build) {
            col.agg_input_cols.push_back(DConstants::INVALID_INDEX); // Not in probe scan
            col.build_agg_cols.push_back(ra.scan_idx); // Build-side scan index
        } else {
            col.agg_input_cols.push_back(ra.scan_idx);
            col.build_agg_cols.push_back(DConstants::INVALID_INDEX);
        }
    }

    // Extract join key column indices from join conditions.
    // Left = original probe child, Right = original build child.
    std::function<idx_t(Expression &)> getIdx = [&](Expression &e) -> idx_t {
        if (e.GetExpressionClass() == ExpressionClass::BOUND_REF)
            return e.Cast<BoundReferenceExpression>().index;
        if (e.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF)
            return e.Cast<BoundColumnRefExpression>().binding.column_index;
        if (e.GetExpressionClass() == ExpressionClass::BOUND_CAST)
            return getIdx(*e.Cast<BoundCastExpression>().child);
        if (e.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            for (auto &child : e.Cast<BoundFunctionExpression>().children) {
                auto r = getIdx(*child);
                if (r != DConstants::INVALID_INDEX) return r;
            }
        }
        return DConstants::INVALID_INDEX;
    };
    for(auto &cond : join->conditions) {
        auto li = getIdx(*cond.left), ri = getIdx(*cond.right);
        if (need_swap) {
            // After swap: original left (probe) is now build, right is now probe
            if (ri != DConstants::INVALID_INDEX) col.probe_key_cols.push_back(ri);
            if (li != DConstants::INVALID_INDEX) col.build_key_cols.push_back(li);
        } else {
            if (li != DConstants::INVALID_INDEX) col.probe_key_cols.push_back(li);
            if (ri != DConstants::INVALID_INDEX) col.build_key_cols.push_back(ri);
        }
    }

    // Bail if key columns couldn't be extracted
    if (col.probe_key_cols.empty() || col.build_key_cols.empty()) return;
    if (col.probe_key_cols.size() != col.build_key_cols.size()) return;

    if (TryRewriteNativeMixedSidePreagg(context, optimizer, op, agg, *join, agg_child, col, need_swap,
                                        state, has_parent)) {
        return;
    }

    if (TryRewriteNativeBuildPreagg(context, optimizer, op, agg, *join, agg_child, col, build_agg_count, need_swap,
                                    state, has_parent)) {
        return;
    }

    if (has_nonnumeric_minmax) {
        return;
    }

    // ── Benchmark-informed planner gate: avoid weak key shapes ──
    {
        auto &probe_types = need_swap ? join->children[1]->types : join->children[0]->types;
        auto &build_types = need_swap ? join->children[0]->types : join->children[1]->types;
        auto get_est = [](LogicalOperator &node) -> idx_t {
            return node.has_estimated_cardinality ? node.estimated_cardinality : 0;
        };
        auto probe_est = get_est(*(need_swap ? join->children[1] : join->children[0]));
        auto build_est = get_est(*(need_swap ? join->children[0] : join->children[1]));
        auto group_est = agg.has_estimated_cardinality ? agg.estimated_cardinality : 0;
        col.probe_estimate = probe_est;
        col.build_estimate = build_est;
        col.group_estimate = group_est;
        auto is_varlen_key_type = [&](const LogicalType &type) {
            auto id = type.id();
            return id == LogicalTypeId::VARCHAR || id == LogicalTypeId::BLOB;
        };
        bool has_varlen_key = false;
        bool has_non_integral_key = false;
        for (auto idx : col.probe_key_cols) {
            if (idx >= probe_types.size()) continue;
            has_varlen_key |= is_varlen_key_type(probe_types[idx]);
            has_non_integral_key |= !probe_types[idx].IsIntegral();
        }
        for (auto idx : col.build_key_cols) {
            if (idx >= build_types.size()) continue;
            has_varlen_key |= is_varlen_key_type(build_types[idx]);
            has_non_integral_key |= !build_types[idx].IsIntegral();
        }
        for (auto idx : col.group_cols) {
            if (idx >= probe_types.size()) continue;
            has_varlen_key |= is_varlen_key_type(probe_types[idx]);
            has_non_integral_key |= !probe_types[idx].IsIntegral();
        }
        bool composite_shape = (join->conditions.size() > 1 || agg.groups.size() > 1);
        bool group_matches_join_key = (col.group_cols.size() == col.probe_key_cols.size());
        if (group_matches_join_key) {
            for (idx_t i = 0; i < col.group_cols.size(); i++) {
                if (col.group_cols[i] != col.probe_key_cols[i]) {
                    group_matches_join_key = false;
                    break;
                }
            }
        }
        bool has_count_or_avg = false;
        for (auto &fn : col.agg_funcs) {
            if (fn == "COUNT" || fn == "AVG") {
                has_count_or_avg = true;
                break;
            }
        }
        bool direct_like_shape = !has_varlen_key && !has_non_integral_key &&
                                 col.probe_key_cols.size() == 1 &&
                                 (col.group_cols.empty() || group_matches_join_key);
        bool large_inputs = probe_est >= 100000 && build_est >= 100000;
        bool huge_inputs = probe_est >= 500000 && build_est >= 500000;
        bool group_known = group_est > 0;
        bool low_probe_fanout = group_known && probe_est <= group_est * 2;
        bool low_build_fanout = group_known && build_est <= group_est * 2;
        bool low_fanout_shape = low_probe_fanout && low_build_fanout;
        bool has_build_aggs = build_agg_count > 0;
        bool simple_varlen_hash_shape = false;
        if (has_varlen_key && !composite_shape && !has_build_aggs && col.probe_key_cols.size() == 1 &&
            (col.group_cols.empty() || group_matches_join_key)) {
            simple_varlen_hash_shape = true;
            for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
                auto &fn = col.agg_funcs[a];
                bool numeric_ok = (a >= col.agg_is_numeric.size() || col.agg_is_numeric[a]);
                if (fn == "COUNT") continue;
                if ((fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX") && numeric_ok) continue;
                simple_varlen_hash_shape = false;
                break;
            }
            if (simple_varlen_hash_shape && !col.group_cols.empty() && group_known &&
                probe_est >= 100000 && group_est >= probe_est / 4) {
                simple_varlen_hash_shape = false;
            }
        }
        bool build_rollup = has_build_aggs && !group_matches_join_key;
        bool heavy_build_aggs = build_agg_count >= 3;
        bool asym_build_heavy = has_build_aggs && group_matches_join_key &&
                                build_est >= probe_est * 8 && build_est >= 1000000;
        if (AggJoinTraceEnabled() && has_build_aggs) {
            fprintf(stderr,
                    "[AGGJOIN] gate build-agg shape: probe_est=%llu build_est=%llu group_est=%llu group_matches_join_key=%d build_agg_count=%llu asym_build_heavy=%d\n",
                    (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est,
                    group_matches_join_key ? 1 : 0, (unsigned long long)build_agg_count, asym_build_heavy ? 1 : 0);
        }
        bool composite_rollup = join->conditions.size() > 1 && !group_matches_join_key;
        bool native_ht_friendly = composite_shape && !has_varlen_key && !has_count_or_avg &&
                                  build_agg_count == 0 && join->conditions.size() <= 2;
        const char *gate_reason = nullptr;
        if (has_varlen_key && !simple_varlen_hash_shape) gate_reason = "variable-width join/group key";
        else if (asym_build_heavy)
            gate_reason = "build-heavy aggregate shape better handled natively";
        else if (build_rollup && large_inputs && (build_agg_count >= 2 || group_est == 0 || group_est >= 256))
            gate_reason = "build-side rollup outside fast path";
        else if (has_build_aggs && heavy_build_aggs && large_inputs && !direct_like_shape)
            gate_reason = "build-side aggregate fanout outside direct path";
        else if (has_build_aggs && large_inputs && group_matches_join_key &&
                 build_agg_count >= 4 && has_count_or_avg)
            gate_reason = "build-side aggregate mix outside direct fast path";
        else if (!has_build_aggs && !composite_shape && has_non_integral_key &&
                 !simple_varlen_hash_shape &&
                 group_matches_join_key && large_inputs)
            gate_reason = "non-integral single-key shape outside direct path";
        else if (!has_build_aggs && !has_count_or_avg && !composite_shape &&
                 large_inputs && low_fanout_shape)
            gate_reason = "low estimated fanout";
        else if (!direct_like_shape && composite_shape && has_non_integral_key && large_inputs)
            gate_reason = "non-integral composite key";
        else if (!direct_like_shape && composite_rollup && huge_inputs && (group_est == 0 || group_est >= 512))
            gate_reason = "estimated expensive composite rollup";
        else if (!direct_like_shape && composite_shape && !native_ht_friendly && large_inputs)
            gate_reason = "composite shape outside native-ht fast path";
        if (gate_reason) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr,
                        "[AGGJOIN] planner cost gate would bail: %s (join_conds=%zu, groups=%zu, build_aggs=%zu, probe_est=%llu, build_est=%llu, group_est=%llu)\n",
                        gate_reason, join->conditions.size(), agg.groups.size(), build_agg_count,
                        (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est);
            }
#ifndef AGGJOIN_NO_PLANNER_GATE
            return;
#else
            fprintf(stderr,
                    "[AGGJOIN] planner cost gate would bail: %s (join_conds=%zu, groups=%zu, build_aggs=%zu, probe_est=%llu, build_est=%llu, group_est=%llu)\n",
                    gate_reason, join->conditions.size(), agg.groups.size(), build_agg_count,
                    (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est);
#endif
        }
    }

    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] planner fired: join_conds=%zu groups=%zu aggs=%zu build_aggs=%zu need_swap=%d\n",
                join->conditions.size(), agg.groups.size(), agg.expressions.size(), build_agg_count,
                need_swap ? 1 : 0);
    }

    // Create LogicalAggJoin
    auto aj = make_uniq<LogicalAggJoin>();
    // Build return types: group columns use compressed types (if compression exists)
    // so that the decompress Projection above us can correctly restore original types.
    // Aggregate columns keep their original types (SUM output is DOUBLE/BIGINT, not compressed).
    {
        vector<LogicalType> ret_types;
        idx_t ng = col.group_compress.size();
        for (idx_t g = 0; g < ng; g++) {
            if (col.group_compress[g].has_compress && !col.group_compress[g].is_string_compress) {
                ret_types.push_back(col.group_compress[g].compressed_type);
            } else if (col.group_compress[g].has_compress && col.group_compress[g].is_string_compress) {
                ret_types.push_back(col.group_compress[g].original_type);
            } else {
                ret_types.push_back(agg.types[g]);
            }
        }
        // Aggregate output types (unchanged)
        for (idx_t a = ng; a < agg.types.size(); a++) {
            ret_types.push_back(agg.types[a]);
        }
        aj->return_types = std::move(ret_types);
    }
    aj->estimated_cardinality = agg.estimated_cardinality;
    aj->group_index = agg.group_index;
    aj->aggregate_index = agg.aggregate_index;
    aj->col = std::move(col);
    // Store expressions for native HT creation
    for (auto &e : agg.expressions) {
        aj->agg_expressions.push_back(e->Copy());
    }
    for (auto &g : agg.groups) {
        aj->group_expressions.push_back(g->Copy());
    }
    // Take Join's children — swap if needed so probe side has GROUP BY columns
    if (need_swap) {
        aj->children.push_back(std::move(join->children[1])); // old build → new probe
        aj->children.push_back(std::move(join->children[0])); // old probe → new build
    } else {
        aj->children = std::move(join->children);
    }


    op = std::move(aj);
}

// Second pass: strip string-decompress projections above AggJoin.
// Narrow VARCHAR support keeps raw strings inside AggJoin, so a parent
// __internal_decompress_string(#i) must become a passthrough reference.
// Integral decompress projections remain untouched because AggJoin still emits
// compressed integral keys for that path.
static void StripDecompressProjections(unique_ptr<LogicalOperator> &op) {
    for (auto &child : op->children) {
        StripDecompressProjections(child);
    }

    // Match: Projection whose child is a LogicalExtensionOperator (our AggJoin)
    if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) return;
    if (op->children.size() != 1) return;
    if (op->children[0]->type != LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) return;

    // Check if all Projection expressions are either:
    // 1. BoundReferenceExpression (passthrough) — keep as-is
    // 2. BoundFunctionExpression with decompress — replace with passthrough
    auto &proj = op->Cast<LogicalProjection>();
    bool has_string_decompress = false;
    for (auto &expr : proj.expressions) {
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            auto &func = expr->Cast<BoundFunctionExpression>();
            if (func.function.name.find("decompress_string") != string::npos) {
                has_string_decompress = true;
            }
        }
    }

    if (!has_string_decompress) return;

    // Replace decompress functions with passthrough references.
    // The decompress function's first child is the column reference to pass through.
    for (idx_t i = 0; i < proj.expressions.size(); i++) {
        auto &expr = proj.expressions[i];
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            auto &func = expr->Cast<BoundFunctionExpression>();
            if (func.function.name.find("decompress_string") != string::npos) {
                // Find the column reference child
                for (auto &child : func.children) {
                    if (child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
                        auto ref_idx = child->Cast<BoundReferenceExpression>().index;
                        proj.expressions[i] = make_uniq<BoundReferenceExpression>(func.return_type, ref_idx);
                        break;
                    }
                    if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                        // BoundColumnRef: use index directly
                        auto &binding = child->Cast<BoundColumnRefExpression>().binding;
                        proj.expressions[i] = make_uniq<BoundReferenceExpression>(func.return_type, binding.column_index);
                        break;
                    }
                }
            }
        }
    }

    // Also update the Projection's return types to match the uncompressed types
    proj.types.clear();
    for (auto &expr : proj.expressions) {
        proj.types.push_back(expr->return_type);
    }
}

static void AggJoinOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    bool ignore_disable_static = false;
    if (input.info) {
        auto info = dynamic_cast<AggJoinOptimizerInfo *>(input.info.get());
        if (info) {
            ignore_disable_static = info->ignore_disable_static;
        }
    }
    if (!ignore_disable_static && AggJoinStaticDisabledByEnv()) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] built-in optimizer disabled by AGGJOIN_DISABLE_STATIC\n");
        }
        return;
    }
    AggJoinRewriteState state;
    WalkAndReplace(input.context, input.optimizer, plan, state, false);
    StripDecompressProjections(plan);
    if (!state.replacement_bindings.empty()) {
        ColumnBindingReplacer replacer;
        replacer.replacement_bindings = std::move(state.replacement_bindings);
        replacer.VisitOperator(*plan);
    }
}

void RegisterAggJoinOptimizer(DatabaseInstance &db, bool ignore_disable_static) {
    OptimizerExtension ext;
    ext.optimize_function = AggJoinOptimize;
    ext.optimizer_info = make_shared_ptr<AggJoinOptimizerInfo>(ignore_disable_static);
#if HAS_CALLBACK_MANAGER
    ExtensionCallbackManager::Get(db).Register(std::move(ext));
#else
    auto &config = DBConfig::GetConfig(db);
    config.optimizer_extensions.push_back(std::move(ext));
#endif
}

} // namespace duckdb
