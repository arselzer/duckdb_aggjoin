#pragma once

#include "aggjoin_optimizer_shared.hpp"
#include "aggjoin_runtime.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

// A join-key side is a "bare column" if it is a column reference, possibly
// wrapped in value-reconciling CASTs, but NOT wrapped in a function or
// arithmetic expression. LOWER(x), x+1, etc. change the join semantics; treating
// them as an equi-key on the raw column joins on the wrong values. (Review bug B,
// 2026-06-15.) Shared by IsEquiJoin and the aggregate-propagation rewrite's edge detector.
inline bool IsBareColumnKey(Expression &expr) {
    auto cls = expr.GetExpressionClass();
    if (cls == ExpressionClass::BOUND_COLUMN_REF || cls == ExpressionClass::BOUND_REF) {
        return true;
    }
    if (cls == ExpressionClass::BOUND_CAST) {
        return IsBareColumnKey(*expr.Cast<BoundCastExpression>().child);
    }
    return false;
}

inline unique_ptr<BoundAggregateExpression> BindAggregateByName(ClientContext &context, const string &name,
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

inline const LogicalType &GetRewriteGroupType(const AggJoinColInfo &col, const LogicalAggregate &agg,
                                              idx_t group_idx) {
    if (group_idx < col.group_compress.size() && col.group_compress[group_idx].has_compress &&
        col.group_compress[group_idx].is_string_compress) {
        return col.group_compress[group_idx].original_type;
    }
    return agg.types[group_idx];
}

bool TryRewriteNativeMixedSidePreagg(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                     LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                     const AggJoinColInfo &col, bool need_swap, AggJoinRewriteState &state,
                                     bool has_parent);

bool TryRewriteNativeFinalBagPreagg(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                    LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                    AggJoinRewriteState &state, bool has_parent);

bool TryRewriteNativeAggPropagation(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                    LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                    AggJoinRewriteState &state, bool has_parent);

bool TryRewriteNativeBuildPreagg(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                 LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                 const AggJoinColInfo &col, idx_t build_agg_count, bool need_swap,
                                 AggJoinRewriteState &state, bool has_parent);

} // namespace duckdb
