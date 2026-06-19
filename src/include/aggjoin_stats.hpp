#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

inline bool AggJoinStatsExtractBinding(Expression &expr, ColumnBinding &binding) {
    auto cls = expr.GetExpressionClass();
    if (cls == ExpressionClass::BOUND_COLUMN_REF) {
        binding = expr.Cast<BoundColumnRefExpression>().binding;
        return true;
    }
    if (cls == ExpressionClass::BOUND_CAST) {
        return AggJoinStatsExtractBinding(*expr.Cast<BoundCastExpression>().child, binding);
    }
    return false;
}

inline bool AggJoinStatsReplaceBoundReferencesWithColumnRefs(unique_ptr<Expression> &expr,
                                                            const vector<ColumnBinding> &bindings,
                                                            const vector<LogicalType> &types) {
    if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
        auto idx = expr->Cast<BoundReferenceExpression>().index;
        if (idx >= bindings.size() || idx >= types.size()) {
            return false;
        }
        expr = make_uniq<BoundColumnRefExpression>(types[idx], bindings[idx]);
        return true;
    }
    bool ok = true;
    ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
        if (!AggJoinStatsReplaceBoundReferencesWithColumnRefs(child, bindings, types)) {
            ok = false;
        }
    });
    return ok;
}

inline bool AggJoinAtLeastRatio(idx_t value, idx_t base, idx_t numerator, idx_t denominator) {
    if (base == 0 || denominator == 0) {
        return false;
    }
    return (__int128)value * (__int128)denominator >= (__int128)base * (__int128)numerator;
}

inline bool AggJoinStatsValueToInt64(const Value &value, int64_t &out) {
    if (value.IsNull()) {
        return false;
    }
    switch (value.type().InternalType()) {
    case PhysicalType::INT8:
        out = value.GetValue<int8_t>();
        return true;
    case PhysicalType::INT16:
        out = value.GetValue<int16_t>();
        return true;
    case PhysicalType::INT32:
        out = value.GetValue<int32_t>();
        return true;
    case PhysicalType::INT64:
        out = value.GetValue<int64_t>();
        return true;
    case PhysicalType::UINT8:
        out = value.GetValue<uint8_t>();
        return true;
    case PhysicalType::UINT16:
        out = value.GetValue<uint16_t>();
        return true;
    case PhysicalType::UINT32:
        out = value.GetValue<uint32_t>();
        return true;
    default:
        return false;
    }
}

inline bool AggJoinTryGetDomainFromMinMax(int64_t min_value, int64_t max_value, idx_t &domain) {
    if (max_value < min_value) {
        return false;
    }
    __int128 range_wide = (__int128)max_value - (__int128)min_value + 1;
    if (range_wide <= 0 || range_wide > (__int128)std::numeric_limits<idx_t>::max()) {
        return false;
    }
    domain = (idx_t)range_wide;
    return domain > 0;
}

inline bool AggJoinTryGetStatsForBinding(ClientContext &context, LogicalOperator &op, ColumnBinding binding,
                                         unique_ptr<BaseStatistics> &stats) {
    LogicalOperator *cur = &op;
    while (cur) {
        if (cur->type == LogicalOperatorType::LOGICAL_GET) {
            auto &get = cur->Cast<LogicalGet>();
            if (binding.table_index != get.table_index) {
                return false;
            }
            auto &col_ids = get.GetColumnIds();
            if (binding.column_index >= col_ids.size()) {
                return false;
            }
            auto col_idx = col_ids[binding.column_index];
#if __has_include("duckdb/main/extension_callback_manager.hpp")
            if (get.function.statistics_extended) {
                TableFunctionGetStatisticsInput input(get.bind_data.get(), col_idx);
                stats = get.function.statistics_extended(context, input);
            } else if (get.function.statistics) {
                stats = get.function.statistics(context, get.bind_data.get(), col_idx.GetPrimaryIndex());
            } else {
                return false;
            }
#else
            if (get.function.statistics) {
                stats = get.function.statistics(context, get.bind_data.get(), col_idx.GetPrimaryIndex());
            } else {
                return false;
            }
#endif
            return stats && NumericStats::HasMinMax(*stats);
        }
        if (cur->type == LogicalOperatorType::LOGICAL_PROJECTION && cur->children.size() == 1) {
            auto &proj = cur->Cast<LogicalProjection>();
            if (binding.table_index != proj.table_index || binding.column_index >= proj.expressions.size()) {
                return false;
            }
            if (!AggJoinStatsExtractBinding(*proj.expressions[binding.column_index], binding)) {
                return false;
            }
            cur = cur->children[0].get();
            continue;
        }
        if (cur->children.size() == 1) {
            cur = cur->children[0].get();
            continue;
        }
        return false;
    }
    return false;
}

inline bool AggJoinTryGetStatsForGetColumn(ClientContext &context, LogicalGet &get, const ColumnIndex &col_idx,
                                           unique_ptr<BaseStatistics> &stats) {
#if __has_include("duckdb/main/extension_callback_manager.hpp")
    if (get.function.statistics_extended) {
        TableFunctionGetStatisticsInput input(get.bind_data.get(), col_idx);
        stats = get.function.statistics_extended(context, input);
    } else if (get.function.statistics) {
        stats = get.function.statistics(context, get.bind_data.get(), col_idx.GetPrimaryIndex());
    } else {
        return false;
    }
#else
    if (get.function.statistics) {
        stats = get.function.statistics(context, get.bind_data.get(), col_idx.GetPrimaryIndex());
    } else {
        return false;
    }
#endif
    return stats && NumericStats::HasMinMax(*stats);
}

inline bool AggJoinTryGetNumericMinMaxFromStats(ClientContext &context, LogicalOperator &op, ColumnBinding binding,
                                               double &min_out, double &max_out) {
    unique_ptr<BaseStatistics> stats;
    if (!AggJoinTryGetStatsForBinding(context, op, binding, stats)) {
        return false;
    }
    min_out = NumericStats::Min(*stats).GetValue<double>();
    max_out = NumericStats::Max(*stats).GetValue<double>();
    return std::isfinite(min_out) && std::isfinite(max_out) && max_out >= min_out;
}

inline bool AggJoinTryGetIntegerExpressionMinMaxFromStats(ClientContext &context, LogicalOperator &op,
                                                         Expression &expr, int64_t &min_out, int64_t &max_out);

inline bool AggJoinTryGetIntegerKeyMinMaxFromStats(ClientContext &context, LogicalOperator &op, ColumnBinding binding,
                                                  int64_t &min_out, int64_t &max_out) {
    unique_ptr<BaseStatistics> stats;
    if (AggJoinTryGetStatsForBinding(context, op, binding, stats)) {
        if (!AggJoinStatsValueToInt64(NumericStats::Min(*stats), min_out) ||
            !AggJoinStatsValueToInt64(NumericStats::Max(*stats), max_out) || max_out < min_out) {
            return false;
        }
        return true;
    }
    LogicalOperator *cur = &op;
    while (cur) {
        if (cur->type == LogicalOperatorType::LOGICAL_PROJECTION && cur->children.size() == 1) {
            auto &proj = cur->Cast<LogicalProjection>();
            if (binding.table_index == proj.table_index && binding.column_index < proj.expressions.size()) {
                auto expr = proj.expressions[binding.column_index]->Copy();
                if (!AggJoinStatsReplaceBoundReferencesWithColumnRefs(expr, cur->children[0]->GetColumnBindings(),
                                                                      cur->children[0]->types)) {
                    return false;
                }
                return AggJoinTryGetIntegerExpressionMinMaxFromStats(context, *cur->children[0], *expr, min_out,
                                                                     max_out);
            }
        }
        if (cur->children.size() != 1) {
            return false;
        }
        cur = cur->children[0].get();
    }
    return false;
}

inline bool AggJoinStatsTryGetIntegerConstant(Expression &expr, int64_t &out) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
        return AggJoinStatsTryGetIntegerConstant(*expr.Cast<BoundCastExpression>().child, out);
    }
    if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
        return false;
    }
    auto &value = expr.Cast<BoundConstantExpression>().value;
    return AggJoinStatsValueToInt64(value, out);
}

inline bool AggJoinTryGetIntegerExpressionMinMaxFromStats(ClientContext &context, LogicalOperator &op,
                                                         Expression &expr, int64_t &min_out, int64_t &max_out) {
    ColumnBinding binding;
    if (AggJoinStatsExtractBinding(expr, binding)) {
        return AggJoinTryGetIntegerKeyMinMaxFromStats(context, op, binding, min_out, max_out);
    }
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
        auto &func = expr.Cast<BoundFunctionExpression>();
        if (!expr.return_type.IsIntegral() || func.children.size() != 2) {
            return false;
        }
        bool is_add = func.function.name == "+";
        bool is_sub = func.function.name == "-";
        if (!is_add && !is_sub) {
            return false;
        }
        int64_t lhs_min = 0;
        int64_t lhs_max = 0;
        int64_t rhs_const = 0;
        if (AggJoinTryGetIntegerExpressionMinMaxFromStats(context, op, *func.children[0], lhs_min, lhs_max) &&
            AggJoinStatsTryGetIntegerConstant(*func.children[1], rhs_const)) {
            __int128 out_min = is_add ? (__int128)lhs_min + rhs_const : (__int128)lhs_min - rhs_const;
            __int128 out_max = is_add ? (__int128)lhs_max + rhs_const : (__int128)lhs_max - rhs_const;
            if (out_min < std::numeric_limits<int64_t>::min() || out_max > std::numeric_limits<int64_t>::max()) {
                return false;
            }
            min_out = (int64_t)out_min;
            max_out = (int64_t)out_max;
            return max_out >= min_out;
        }
        int64_t lhs_const = 0;
        int64_t rhs_min = 0;
        int64_t rhs_max = 0;
        if (is_add && AggJoinStatsTryGetIntegerConstant(*func.children[0], lhs_const) &&
            AggJoinTryGetIntegerExpressionMinMaxFromStats(context, op, *func.children[1], rhs_min, rhs_max)) {
            __int128 out_min = (__int128)lhs_const + rhs_min;
            __int128 out_max = (__int128)lhs_const + rhs_max;
            if (out_min < std::numeric_limits<int64_t>::min() || out_max > std::numeric_limits<int64_t>::max()) {
                return false;
            }
            min_out = (int64_t)out_min;
            max_out = (int64_t)out_max;
            return max_out >= min_out;
        }
    }
    return false;
}

inline bool AggJoinTryGetIntegerKeyDomainFromStats(ClientContext &context, LogicalOperator &op, ColumnBinding binding,
                                                  idx_t &domain) {
    int64_t min_value = 0;
    int64_t max_value = 0;
    if (!AggJoinTryGetIntegerKeyMinMaxFromStats(context, op, binding, min_value, max_value)) {
        return false;
    }
    return AggJoinTryGetDomainFromMinMax(min_value, max_value, domain);
}

inline bool AggJoinTryGetIntegerExpressionDomainFromStats(ClientContext &context, LogicalOperator &op,
                                                         Expression &expr, idx_t &domain) {
    int64_t min_value = 0;
    int64_t max_value = 0;
    if (!AggJoinTryGetIntegerExpressionMinMaxFromStats(context, op, expr, min_value, max_value)) {
        return false;
    }
    return AggJoinTryGetDomainFromMinMax(min_value, max_value, domain);
}

inline bool AggJoinTryGetCompositeKeyDomainFromStats(ClientContext &context, LogicalOperator &op,
                                                    const vector<ColumnBinding> &cols,
                                                    const vector<LogicalType> &types, idx_t &domain) {
    if (cols.empty() || cols.size() != types.size()) {
        return false;
    }
    idx_t product = 1;
    for (idx_t i = 0; i < cols.size(); i++) {
        if (!types[i].IsIntegral()) {
            return false;
        }
        idx_t col_domain = 0;
        if (!AggJoinTryGetIntegerKeyDomainFromStats(context, op, cols[i], col_domain)) {
            return false;
        }
        if (col_domain == 0 || product > std::numeric_limits<idx_t>::max() / col_domain) {
            domain = std::numeric_limits<idx_t>::max();
            return true;
        }
        product *= col_domain;
    }
    domain = product;
    return domain > 0;
}

inline bool AggJoinFilterPredicateAlwaysTrueInternal(ClientContext &context, LogicalOperator &op, Expression &expr) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
        auto &conj = expr.Cast<BoundConjunctionExpression>();
        if (expr.type != ExpressionType::CONJUNCTION_AND) {
            return false;
        }
        for (auto &child : conj.children) {
            if (!AggJoinFilterPredicateAlwaysTrueInternal(context, op, *child)) {
                return false;
            }
        }
        return true;
    }
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
        auto &op_expr = expr.Cast<BoundOperatorExpression>();
        if (expr.type != ExpressionType::OPERATOR_IS_NOT_NULL || op_expr.children.size() != 1) {
            return false;
        }
        ColumnBinding binding;
        if (!AggJoinStatsExtractBinding(*op_expr.children[0], binding)) {
            return false;
        }
        unique_ptr<BaseStatistics> stats;
        return AggJoinTryGetStatsForBinding(context, op, binding, stats) && stats && !stats->CanHaveNull();
    }
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
        auto &cmp = expr.Cast<BoundComparisonExpression>();
        int64_t lhs_min = 0;
        int64_t lhs_max = 0;
        int64_t rhs = 0;
        if (!AggJoinTryGetIntegerExpressionMinMaxFromStats(context, op, *cmp.left, lhs_min, lhs_max) ||
            !AggJoinStatsTryGetIntegerConstant(*cmp.right, rhs)) {
            return false;
        }
        switch (expr.type) {
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            return lhs_min >= rhs;
        case ExpressionType::COMPARE_GREATERTHAN:
            return lhs_min > rhs;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            return lhs_max <= rhs;
        case ExpressionType::COMPARE_LESSTHAN:
            return lhs_max < rhs;
        case ExpressionType::COMPARE_EQUAL:
            return lhs_min == rhs && lhs_max == rhs;
        default:
            return false;
        }
    }
    return false;
}

inline bool AggJoinFilterPredicateAlwaysTrue(ClientContext &context, LogicalOperator &op, Expression &expr) {
    auto normalized = expr.Copy();
    if (!AggJoinStatsReplaceBoundReferencesWithColumnRefs(normalized, op.GetColumnBindings(), op.types)) {
        return false;
    }
    return AggJoinFilterPredicateAlwaysTrueInternal(context, op, *normalized);
}

inline bool AggJoinConstantFilterAlwaysTrue(const BaseStatistics &stats, const ConstantFilter &filter) {
    int64_t constant = 0;
    if (!AggJoinStatsValueToInt64(filter.constant, constant)) {
        return false;
    }
    int64_t min_value = 0;
    int64_t max_value = 0;
    if (!AggJoinStatsValueToInt64(NumericStats::Min(stats), min_value) ||
        !AggJoinStatsValueToInt64(NumericStats::Max(stats), max_value)) {
        return false;
    }
    switch (filter.comparison_type) {
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        return min_value >= constant;
    case ExpressionType::COMPARE_GREATERTHAN:
        return min_value > constant;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return max_value <= constant;
    case ExpressionType::COMPARE_LESSTHAN:
        return max_value < constant;
    case ExpressionType::COMPARE_EQUAL:
        return min_value == constant && max_value == constant;
    default:
        return false;
    }
}

inline bool AggJoinTableFilterAlwaysTrue(ClientContext &context, LogicalGet &get, const ColumnIndex &col_idx,
                                        TableFilter &filter) {
    if (filter.filter_type == TableFilterType::CONJUNCTION_AND) {
        auto &conj = filter.Cast<ConjunctionAndFilter>();
        for (auto &child : conj.child_filters) {
            if (!AggJoinTableFilterAlwaysTrue(context, get, col_idx, *child)) {
                return false;
            }
        }
        return true;
    }
    if (filter.filter_type == TableFilterType::OPTIONAL_FILTER) {
        return true;
    }
    unique_ptr<BaseStatistics> stats;
    if (!AggJoinTryGetStatsForGetColumn(context, get, col_idx, stats)) {
        return false;
    }
    switch (filter.filter_type) {
    case TableFilterType::CONSTANT_COMPARISON:
        return AggJoinConstantFilterAlwaysTrue(*stats, filter.Cast<ConstantFilter>());
    case TableFilterType::IS_NOT_NULL:
        return !stats->CanHaveNull();
    default:
        return false;
    }
}

inline bool AggJoinTryGetNoOpFilteredGetCardinality(ClientContext &context, LogicalGet &get, idx_t &cardinality) {
    if (!get.function.cardinality) {
        return false;
    }
    for (auto &entry : get.table_filters.filters) {
        if (!AggJoinTableFilterAlwaysTrue(context, get, ColumnIndex(entry.first), *entry.second)) {
            return false;
        }
    }
    auto stats = get.function.cardinality(context, get.bind_data.get());
    if (!stats || !stats->has_estimated_cardinality || stats->estimated_cardinality == 0) {
        return false;
    }
    cardinality = stats->estimated_cardinality;
    return true;
}

inline bool AggJoinTryGetNoOpFilteredCardinality(ClientContext &context, LogicalOperator &op, idx_t &cardinality) {
    LogicalOperator *cur = &op;
    while (cur) {
        if (cur->type == LogicalOperatorType::LOGICAL_PROJECTION && cur->children.size() == 1) {
            cur = cur->children[0].get();
            continue;
        }
        if (cur->type == LogicalOperatorType::LOGICAL_FILTER && cur->children.size() == 1) {
            auto &filter = cur->Cast<LogicalFilter>();
            for (auto &expr : filter.expressions) {
                if (!AggJoinFilterPredicateAlwaysTrue(context, *cur->children[0], *expr)) {
                    return false;
                }
            }
            cur = cur->children[0].get();
            continue;
        }
        if (cur->type == LogicalOperatorType::LOGICAL_GET) {
            auto &get = cur->Cast<LogicalGet>();
            if (AggJoinTryGetNoOpFilteredGetCardinality(context, get, cardinality)) {
                return true;
            }
        }
        cardinality = cur->has_estimated_cardinality ? cur->estimated_cardinality : cur->EstimateCardinality(context);
        return cardinality > 0;
    }
    return false;
}

} // namespace duckdb
