#pragma once

#include "aggjoin_optimizer_shared.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/function_binder.hpp"
#include <algorithm>
#include <cstring>

namespace duckdb {
static inline hash_t ApplyAggJoinTestHashBits(hash_t h) {
    auto bits = GetAggJoinTestHashBits();
    if (bits < 0 || bits >= 64) return h;
    if (bits == 0) return 0;
    return h & ((((hash_t)1) << bits) - 1);
}

static inline void ApplyAggJoinTestHashBits(hash_t *hashes, idx_t count) {
    auto bits = GetAggJoinTestHashBits();
    if (bits < 0 || bits >= 64) return;
    hash_t mask = bits == 0 ? 0 : ((((hash_t)1) << bits) - 1);
    for (idx_t i = 0; i < count; i++) hashes[i] &= mask;
}

static constexpr idx_t AGGJOIN_MAX_HASH_TABLE_CAPACITY = idx_t(1) << 23;

static idx_t GetAggJoinHashTableCapacityCap() {
    auto test_cap = GetAggJoinTestHTCapacity();
    if (test_cap >= 16) {
        idx_t cap = 16;
        auto requested = NumericCast<idx_t>(test_cap);
        while (cap < requested && cap < AGGJOIN_MAX_HASH_TABLE_CAPACITY) {
            cap <<= 1;
        }
        return cap;
    }
    return AGGJOIN_MAX_HASH_TABLE_CAPACITY;
}

static idx_t ComputeAggJoinHashCapacity(idx_t expected, const char *ht_name) {
    auto target = expected > NumericLimits<idx_t>::Maximum() / 2 ? NumericLimits<idx_t>::Maximum() : expected * 2;
    target = MaxValue<idx_t>(target, 16);
    auto cap_limit = GetAggJoinHashTableCapacityCap();
    idx_t cap = 16;
    while (cap < target) {
        if (cap >= cap_limit) {
            throw OutOfMemoryException("AGGJOIN %s exceeded capacity cap of %llu buckets", ht_name, cap_limit);
        }
        cap <<= 1;
    }
    return cap;
}

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
    bool uses_uint64_key = false;
    uint64_t uint64_key = 0; // Raw UINT64 key payload for exact-match hash paths
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
    void Init(idx_t n) {
        auto c = ComputeAggJoinHashCapacity(n, "build hash table");
        mask = c - 1;
        buckets.resize(c);
        count = 0;
    }
    void Grow() {
        // Double capacity and rehash
        auto old = std::move(buckets);
        auto old_cap = mask + 1;
        auto cap_limit = GetAggJoinHashTableCapacityCap();
        if (old_cap >= cap_limit) {
            throw OutOfMemoryException("AGGJOIN build hash table exceeded capacity cap of %llu buckets", cap_limit);
        }
        idx_t new_cap = old_cap * 2;
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

static void InitBuildEntryKeys(BuildEntry &entry, DataChunk &chunk, const vector<idx_t> &key_cols, idx_t r) {
    entry.uses_uint64_key = false;
    entry.uint64_key = 0;
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
    case PhysicalType::UINT64:
        // Keep raw UINT64 keys out of the signed-offset direct/range paths.
        // They still participate in the exact-match hash path via uint64_key.
        entry.uses_uint64_key = true;
        entry.uint64_key = FlatVector::GetData<uint64_t>(chunk.data[ki])[r];
        entry.int_key = INT64_MIN;
        break;
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
    if (entry.uses_uint64_key) {
        if (key_cols.size() != 1) return false;
        auto ki = key_cols[0];
        if (chunk.data[ki].GetType().InternalType() != PhysicalType::UINT64) return false;
        return entry.uint64_key == FlatVector::GetData<uint64_t>(chunk.data[ki])[r];
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
inline CompressInfo ExtractCompressInfo(LogicalOperator &op, idx_t idx) {
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
    // Extract offset value as int64. Raw UINT64 offsets outside the signed range
    // are not safe for the signed-offset direct/compress logic, so drop compress info.
    auto &val = constant.value;
    if (val.IsNull()) {
        info.has_compress = false;
        return info;
    }
    switch (val.type().InternalType()) {
    case PhysicalType::INT16: info.offset = val.GetValue<int16_t>(); break;
    case PhysicalType::INT32: info.offset = val.GetValue<int32_t>(); break;
    case PhysicalType::INT64: info.offset = val.GetValue<int64_t>(); break;
    case PhysicalType::INT8:  info.offset = val.GetValue<int8_t>(); break;
    case PhysicalType::UINT8: info.offset = val.GetValue<uint8_t>(); break;
    case PhysicalType::UINT16: info.offset = val.GetValue<uint16_t>(); break;
    case PhysicalType::UINT32: info.offset = val.GetValue<uint32_t>(); break;
    case PhysicalType::UINT64: {
        auto uval = val.GetValue<uint64_t>();
        if (uval > NumericLimits<int64_t>::Maximum()) {
            info.has_compress = false;
            break;
        }
        info.offset = UnsafeNumericCast<int64_t>(uval);
        break;
    }
    default: info.has_compress = false; break; // Can't extract offset
    }
    return info;
}

// Walk the Projection chain from agg_child down to find compress info
// for a column at position `agg_idx` in the agg_child's output.
inline CompressInfo FindCompressInChain(LogicalOperator &op, idx_t idx) {
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
inline idx_t TraceProjectionChain(LogicalOperator &op, idx_t idx, int depth) {
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
    // Value-based MIN/MAX for non-numeric types (VARCHAR, DATE, etc.).
    // This remains reachable for hash-mode queries that are admitted by the
    // planner but not lowered back to a native preaggregation rewrite.
    vector<vector<Value>> val_min_data;  // [agg][slot]
    vector<vector<Value>> val_max_data;  // [agg][slot]
    vector<bool> agg_uses_val_minmax;    // [agg] true if this agg uses Value path
    idx_t mask = 0, num_aggs = 0, count = 0, capacity = 0;

    void Init(idx_t expected, idx_t na, idx_t ng = 0, const vector<bool> *g_is_int = nullptr,
              const vector<bool> *val_minmax = nullptr, const vector<bool> *g_use_value = nullptr) {
        num_aggs = na; num_groups = ng;
        auto cap = ComputeAggJoinHashCapacity(expected, "result hash table");
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
        auto cap_limit = GetAggJoinHashTableCapacityCap();
        if (old_cap >= cap_limit) {
            throw OutOfMemoryException("AGGJOIN result hash table exceeded capacity cap of %llu buckets", cap_limit);
        }
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

} // namespace duckdb
