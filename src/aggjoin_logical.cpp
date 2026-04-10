#include "aggjoin_optimizer_shared.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"

namespace duckdb {

vector<ColumnBinding> LogicalAggJoin::GetColumnBindings() {
    vector<ColumnBinding> r;
    idx_t ng = col.group_cols.size();
    idx_t na = col.agg_funcs.size();
    for (idx_t i = 0; i < ng; i++) r.emplace_back(group_index, i);
    for (idx_t i = 0; i < na; i++) r.emplace_back(aggregate_index, i);
    return r;
}

PhysicalOperator &LogicalAggJoin::CreatePlan(ClientContext &ctx, PhysicalPlanGenerator &planner) {
    return CreatePhysicalAggJoinPlan(*this, ctx, planner);
}

void LogicalAggJoin::ResolveTypes() {
    types = return_types;
}

void LogicalAggJoin::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
    for (auto &c : children) {
        res.VisitOperator(*c);
    }
    bindings = GetColumnBindings();
}

} // namespace duckdb
