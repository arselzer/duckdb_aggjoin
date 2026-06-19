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

// ============================================================
// PhysicalAggJoin — 2-child: build side sinks, probe feeds ExecuteInternal
// ============================================================

PhysicalAggJoin::PhysicalAggJoin(PhysicalPlan &plan, vector<LogicalType> types, idx_t estimated_cardinality)
    : CachingPhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
    caching_supported = false;
}

unique_ptr<OperatorState> PhysicalAggJoin::GetOperatorState(ExecutionContext &ctx) const {
    return make_uniq<AggJoinOperatorState>();
}

static bool SupportsParallelPlannedDirect(const PhysicalAggJoin &op) {
    auto &col = op.col;
    if (!col.planned_direct_mode || col.probe_key_cols.size() != 1 || col.build_key_cols.size() != 1) {
        return false;
    }
    bool ungrouped = col.group_cols.empty();
    bool grouped_by_key = col.group_cols.size() == 1 && col.group_cols[0] == col.probe_key_cols[0];
    if (!ungrouped && !grouped_by_key) {
        return false;
    }
    for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
        bool on_build = col.agg_on_build.size() > a && col.agg_on_build[a];
        auto &fn = col.agg_funcs[a];
        auto ai = a < col.agg_input_cols.size() ? col.agg_input_cols[a] : DConstants::INVALID_INDEX;
        if (fn == "COUNT") {
            if (!on_build && ai != DConstants::INVALID_INDEX && ai >= col.probe_col_count) {
                return false;
            }
            continue;
        }
        if (fn != "SUM" && fn != "AVG" && fn != "MIN" && fn != "MAX") {
            return false;
        }
        if (!on_build && (ai == DConstants::INVALID_INDEX || ai >= col.probe_col_count)) {
            return false;
        }
        if (a >= op.payload_types.size()) {
            return false;
        }
        auto payload_type = op.payload_types[a].InternalType();
        bool payload_ok = payload_type == PhysicalType::DOUBLE || payload_type == PhysicalType::FLOAT;
        if (on_build && (fn == "MIN" || fn == "MAX")) {
            payload_ok = payload_ok || payload_type == PhysicalType::INT8 || payload_type == PhysicalType::INT16 ||
                         payload_type == PhysicalType::INT32 || payload_type == PhysicalType::UINT8 ||
                         payload_type == PhysicalType::UINT16 || payload_type == PhysicalType::UINT32;
        }
        if (!payload_ok) {
            return false;
        }
    }
    return true;
}

bool PhysicalAggJoin::ParallelOperator() const {
    return SupportsParallelPlannedDirect(*this);
}

void AggJoinOperatorState::Finalize(const PhysicalOperator &op, ExecutionContext &context) {
    if (!parallel_direct_active || parallel_direct_merged) {
        return;
    }
    auto &physical = op.Cast<PhysicalAggJoin>();
    if (!physical.sink_state) {
        return;
    }
    auto &sink = physical.sink_state->Cast<AggJoinSinkState>();
    lock_guard<mutex> guard(sink.direct_merge_lock);
    sink.probe_rows_seen += parallel_probe_rows_seen;
    auto na = physical.col.agg_funcs.size();
    if (parallel_direct_grouped) {
        auto krange = sink.key_range;
        if (parallel_direct_sparse_grouped) {
            for (idx_t slot = 0; slot < parallel_direct_sparse_keys.size(); slot++) {
                auto k = parallel_direct_sparse_keys[slot];
                if (!sink.direct_key_seen[k]) {
                    sink.direct_key_seen[k] = 1;
                    sink.direct_active_keys.push_back(k);
                }
                for (idx_t a = 0; a < na; a++) {
                    auto &fn = physical.col.agg_funcs[a];
                    auto global_off = a * krange + k;
                    auto local_off = slot * na + a;
                    if (fn == "AVG") {
                        sink.direct_sums[global_off] += parallel_sparse_sums[local_off];
                        sink.direct_counts[global_off] += parallel_sparse_counts[local_off];
                    } else if (fn == "MIN") {
                        if (parallel_sparse_has[local_off] &&
                            (!sink.direct_has[global_off] ||
                             parallel_sparse_mins[local_off] < sink.direct_mins[global_off])) {
                            sink.direct_mins[global_off] = parallel_sparse_mins[local_off];
                            sink.direct_has[global_off] = 1;
                        }
                    } else if (fn == "MAX") {
                        if (parallel_sparse_has[local_off] &&
                            (!sink.direct_has[global_off] ||
                             parallel_sparse_maxs[local_off] > sink.direct_maxs[global_off])) {
                            sink.direct_maxs[global_off] = parallel_sparse_maxs[local_off];
                            sink.direct_has[global_off] = 1;
                        }
                    } else {
                        sink.direct_sums[global_off] += parallel_sparse_sums[local_off];
                        if (fn == "SUM" && parallel_sparse_has[local_off]) {
                            sink.direct_has[global_off] = 1;
                        }
                    }
                }
            }
            parallel_direct_merged = true;
            return;
        }
        for (auto k : parallel_direct_active_keys) {
            if (!sink.direct_key_seen[k]) {
                sink.direct_key_seen[k] = 1;
                sink.direct_active_keys.push_back(k);
            }
            for (idx_t a = 0; a < na; a++) {
                auto &fn = physical.col.agg_funcs[a];
                auto off = a * krange + k;
                if (fn == "AVG") {
                    sink.direct_sums[off] += parallel_direct_sums[off];
                    sink.direct_counts[off] += parallel_direct_counts[off];
                } else if (fn == "MIN") {
                    if (parallel_direct_has[off] &&
                        (!sink.direct_has[off] || parallel_direct_mins[off] < sink.direct_mins[off])) {
                        sink.direct_mins[off] = parallel_direct_mins[off];
                        sink.direct_has[off] = 1;
                    }
                } else if (fn == "MAX") {
                    if (parallel_direct_has[off] &&
                        (!sink.direct_has[off] || parallel_direct_maxs[off] > sink.direct_maxs[off])) {
                        sink.direct_maxs[off] = parallel_direct_maxs[off];
                        sink.direct_has[off] = 1;
                    }
                } else {
                    sink.direct_sums[off] += parallel_direct_sums[off];
                    if (fn == "SUM" && parallel_direct_has[off]) {
                        sink.direct_has[off] = 1;
                    }
                }
            }
        }
        parallel_direct_merged = true;
        return;
    }
    for (idx_t a = 0; a < na; a++) {
        auto &fn = physical.col.agg_funcs[a];
        if (fn == "AVG") {
            sink.ungrouped_sum[a] += parallel_ungrouped_sum[a];
            sink.ungrouped_count[a] += parallel_ungrouped_count[a];
        } else if (fn == "MIN") {
            if (parallel_ungrouped_has[a] &&
                (!sink.ungrouped_has[a] || parallel_ungrouped_min[a] < sink.ungrouped_min[a])) {
                sink.ungrouped_min[a] = parallel_ungrouped_min[a];
                sink.ungrouped_has[a] = 1;
            }
        } else if (fn == "MAX") {
            if (parallel_ungrouped_has[a] &&
                (!sink.ungrouped_has[a] || parallel_ungrouped_max[a] > sink.ungrouped_max[a])) {
                sink.ungrouped_max[a] = parallel_ungrouped_max[a];
                sink.ungrouped_has[a] = 1;
            }
        } else {
            sink.ungrouped_sum[a] += parallel_ungrouped_sum[a];
            if (fn == "SUM" && parallel_ungrouped_has[a]) {
                sink.ungrouped_has[a] = 1;
            }
        }
    }
    parallel_direct_merged = true;
}

void PhysicalAggJoin::BuildPipelines(Pipeline &cur, MetaPipeline &mp) {
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

string PhysicalAggJoin::GetName() const {
    return "AGGJOIN";
}

PhysicalOperator &CreatePhysicalAggJoinPlan(LogicalAggJoin &op, ClientContext &ctx, PhysicalPlanGenerator &planner) {
    D_ASSERT(op.children.size() == 2);
    auto &probe = planner.CreatePlan(*op.children[0]);
    auto &build = planner.CreatePlan(*op.children[1]);
    auto &ref = planner.Make<PhysicalAggJoin>(op.return_types, op.estimated_cardinality);
    auto &phys = ref.Cast<PhysicalAggJoin>();
    phys.children.push_back(probe);
    phys.children.push_back(build);
    phys.col = op.col;

    for (auto &g : op.group_expressions) {
        phys.group_types.push_back(g->return_type);
    }
    for (auto &e : op.agg_expressions) {
        auto &ba = e->Cast<BoundAggregateExpression>();
        if (!ba.children.empty()) {
            phys.payload_types.push_back(ba.children[0]->return_type);
        } else {
            phys.payload_types.push_back(LogicalType::BIGINT);
        }
    }
    for (auto &e : op.agg_expressions) {
        phys.owned_agg_exprs.push_back(e->Copy());
    }

    return phys;
}

} // namespace duckdb
