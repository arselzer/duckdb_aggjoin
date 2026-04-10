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
    return make_uniq<CachingOperatorState>();
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
