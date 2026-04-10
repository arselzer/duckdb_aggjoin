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

#if HAS_CALLBACK_MANAGER
#define AGGJOIN_GETDATA GetDataInternal
#else
#define AGGJOIN_GETDATA GetData
#endif

#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include "aggjoin_emit_internal.hpp"
#include "aggjoin_physical.hpp"
#include "aggjoin_runtime.hpp"
#include "aggjoin_state.hpp"

namespace duckdb {

SourceResultType PhysicalAggJoin::AGGJOIN_GETDATA(ExecutionContext &ctx, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
    if (!sink_state) {
        chunk.SetCardinality(0);
        return SourceResultType::FINISHED;
    }
    auto &sink = sink_state->Cast<AggJoinSinkState>();

    if (sink.build_ht.mask == 0) {
        if (col.group_cols.empty()) {
            auto &src0 = input.global_state.Cast<AggJoinSourceState>();
            if (!src0.initialized) {
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
        chunk.SetCardinality(0);
        return SourceResultType::FINISHED;
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
                path, (unsigned long long)col.probe_estimate, (unsigned long long)col.build_estimate,
                (unsigned long long)col.group_estimate, (unsigned long long)sink.build_rows_seen,
                (unsigned long long)sink.build_rows_kept, (unsigned long long)sink.build_ht.count,
                (unsigned long long)build_capacity, (unsigned long long)sink.probe_rows_seen,
                (unsigned long long)sink.hash_match_rows, (unsigned long long)sink.range_prefilter_skips,
                (unsigned long long)sink.bloom_prefilter_skips, (unsigned long long)result_groups,
                (unsigned long long)hash_capacity, (unsigned long long)sink.key_range,
                (unsigned long long)sink.build_agg_slots);
    }

    SourceResultType direct_result;
    if (TryEmitDirectLikeResult(*this, chunk, sink, src, direct_result)) {
        return direct_result;
    }

    if (sink.use_native_ht && sink.agg_ht) {
        if (!src.initialized) {
            src.initialized = true;
            sink.agg_ht->InitializeScan(src.scan_state);
        }
        DataChunk group_rows, agg_rows;
        group_rows.Initialize(ctx.client, group_types);
        agg_rows.Initialize(ctx.client, payload_types);
        bool has_data = sink.agg_ht->Scan(src.scan_state, group_rows, agg_rows);
        if (!has_data || group_rows.size() == 0) {
            chunk.SetCardinality(0);
            return SourceResultType::FINISHED;
        }
        auto ng = group_types.size();
        for (idx_t g = 0; g < ng; g++) chunk.data[g].Reference(group_rows.data[g]);
        for (idx_t a = 0; a < agg_rows.ColumnCount(); a++) chunk.data[ng + a].Reference(agg_rows.data[a]);
        chunk.SetCardinality(group_rows.size());
        return SourceResultType::HAVE_MORE_OUTPUT;
    }

    auto &rht = sink.result_ht;
    if (!src.initialized) {
        src.initialized = true;
        for (idx_t i = 0; i < rht.capacity; i++) {
            if (rht.slot_occupied[i]) src.slot_indices.push_back(i);
        }
    }
    if (src.slot_indices.empty()) {
        if (col.group_cols.empty() && src.pos == 0) {
            auto na = col.agg_funcs.size();
            for (idx_t a = 0; a < na; a++) {
                if (col.agg_funcs[a] == "COUNT") {
                    if (chunk.data[a].GetType().InternalType() == PhysicalType::INT64) {
                        chunk.data[a].SetValue(0, Value::BIGINT(0));
                    } else {
                        chunk.data[a].SetValue(0, Value(0.0));
                    }
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
        for (idx_t g = 0; g < ng; g++) {
            bool is_int = g < rht.group_is_int.size() && rht.group_is_int[g];
            if (g < col.group_compress.size() && col.group_compress[g].has_compress &&
                !col.group_compress[g].is_string_compress) {
                auto raw_int = rht.GroupInt(g, slot);
                auto compressed = raw_int - col.group_compress[g].offset;
                switch (col.group_compress[g].compressed_type.InternalType()) {
                case PhysicalType::UINT8:
                    FlatVector::GetData<uint8_t>(chunk.data[g])[cnt] = (uint8_t)compressed;
                    break;
                case PhysicalType::UINT16:
                    FlatVector::GetData<uint16_t>(chunk.data[g])[cnt] = (uint16_t)compressed;
                    break;
                case PhysicalType::UINT32:
                    FlatVector::GetData<uint32_t>(chunk.data[g])[cnt] = (uint32_t)compressed;
                    break;
                case PhysicalType::UINT64:
                    FlatVector::GetData<uint64_t>(chunk.data[g])[cnt] = (uint64_t)compressed;
                    break;
                default:
                    FlatVector::GetData<int32_t>(chunk.data[g])[cnt] = (int32_t)raw_int;
                    break;
                }
            } else if (rht.GroupUsesValue(g)) {
                chunk.data[g].SetValue(cnt, rht.GroupVal(g, slot));
            } else if (is_int) {
                auto out_type = chunk.data[g].GetType().InternalType();
                auto val = rht.GroupInt(g, slot);
                switch (out_type) {
                case PhysicalType::INT32:
                    FlatVector::GetData<int32_t>(chunk.data[g])[cnt] = (int32_t)val;
                    break;
                case PhysicalType::INT64:
                    FlatVector::GetData<int64_t>(chunk.data[g])[cnt] = val;
                    break;
                case PhysicalType::UINT32:
                    FlatVector::GetData<uint32_t>(chunk.data[g])[cnt] = (uint32_t)val;
                    break;
                case PhysicalType::UINT64:
                    FlatVector::GetData<uint64_t>(chunk.data[g])[cnt] = (uint64_t)val;
                    break;
                default:
                    chunk.data[g].SetValue(cnt, Value::BIGINT(val));
                    break;
                }
            } else {
                FlatVector::GetData<double>(chunk.data[g])[cnt] = rht.GroupDbl(g, slot);
            }
        }
        for (idx_t a = 0; a < na; a++) {
            Value v;
            auto &f = col.agg_funcs[a];
            if ((f == "MIN" || f == "MAX") && rht.UsesValMinMax(a)) {
                // Intentional Value-based path for the surviving non-numeric MIN/MAX
                // hash cases. Planner rewrites should have already handled the
                // stronger native-lowering shapes before execution reaches here.
                if (f == "MIN") {
                    v = rht.GetHas(slot, a) ? rht.ValMin(slot, a) : Value();
                } else {
                    v = rht.GetHas(slot, a) ? rht.ValMax(slot, a) : Value();
                }
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
        cnt++;
        src.pos++;
    }
    chunk.SetCardinality(cnt);
    return cnt == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
