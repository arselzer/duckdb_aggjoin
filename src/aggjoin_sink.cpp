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
#include <cstring>
#include "aggjoin_physical.hpp"
#include "aggjoin_runtime.hpp"
#include "aggjoin_state.hpp"

namespace duckdb {
unique_ptr<GlobalSinkState> PhysicalAggJoin::GetGlobalSinkState(ClientContext &ctx) const {
    auto state = make_uniq<AggJoinSinkState>();
    bool has_build_aggs = false;
    for (auto on_build : col.agg_on_build) {
        if (on_build) {
            has_build_aggs = true;
            break;
        }
    }
    bool native_safe = !has_build_aggs && !col.group_cols.empty();
    state->build_agg_slots = 0;
    for (auto on_build : col.agg_on_build) {
        if (on_build) {
            state->build_agg_slots++;
        }
    }
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
        state->agg_ht = make_uniq<GroupedAggregateHashTable>(ctx, BufferAllocator::Get(ctx), group_types,
                                                             payload_types, bindings);
    }
    return std::move(state);
}

// ── Build side: count per key ──
SinkResultType PhysicalAggJoin::Sink(ExecutionContext &ctx, DataChunk &chunk, OperatorSinkInput &input) const {
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

SinkFinalizeType PhysicalAggJoin::Finalize(Pipeline &p, Event &e, ClientContext &c,
                                           OperatorSinkFinalizeInput &input) const {
        auto &sink = input.global_state.Cast<AggJoinSinkState>();
        sink.finalized = true;

        // Detect if direct/segmented mode is possible: single signed-offset-safe
        // integer build key with small range that fits in a flat array.
        // Raw UINT64 keys stay on the exact-match hash/native paths because the
        // fast array modes and range prefilter rely on int64 offsets.
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
                    if (varchar_key && sink.has_min_max && col.agg_funcs.size() >= 2 && sink.build_ht.count <= 5000) {
                        sink.build_string_lookup_mode = true;
                        sink.build_string_slots.clear();
                        sink.build_string_slots.reserve(sink.build_ht.count * 2);
                        for (idx_t slot = 0; slot < cap; slot++) {
                            auto &entry = sink.build_ht.buckets[slot];
                            if (!entry.occupied || !entry.uses_string_key) {
                                sink.build_string_lookup_mode = false;
                                sink.build_string_slots.clear();
                                break;
                            }
                            sink.build_string_slots.emplace(string_t(entry.str_key.data(),
                                                                     UnsafeNumericCast<uint32_t>(entry.str_key.size())),
                                                            slot);
                        }
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

} // namespace duckdb
