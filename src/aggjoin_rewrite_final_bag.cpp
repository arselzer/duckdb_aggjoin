#include "aggjoin_rewrites_internal.hpp"

namespace duckdb {

namespace {

struct FinalBagAggInfo {
    string fn;
    LogicalType result_type;
    bool on_head = false;
    idx_t side_col = DConstants::INVALID_INDEX;
    idx_t sum_slot = DConstants::INVALID_INDEX;
    idx_t count_slot = DConstants::INVALID_INDEX;
    idx_t min_slot = DConstants::INVALID_INDEX;
    idx_t max_slot = DConstants::INVALID_INDEX;
};

struct FinalBagPattern {
    idx_t nested_idx = DConstants::INVALID_INDEX;
    idx_t tail_idx = DConstants::INVALID_INDEX;
    idx_t head_child_idx = DConstants::INVALID_INDEX;
    idx_t bridge_child_idx = DConstants::INVALID_INDEX;
    idx_t head_key_idx = DConstants::INVALID_INDEX;
    idx_t bridge_head_key_idx = DConstants::INVALID_INDEX;
    idx_t bridge_tail_key_idx = DConstants::INVALID_INDEX;
    idx_t tail_key_idx = DConstants::INVALID_INDEX;
    ColumnBinding head_key_binding;
    vector<ColumnBinding> head_bindings;
    vector<ColumnBinding> bridge_bindings;
    vector<ColumnBinding> tail_bindings;
    vector<LogicalType> head_types;
    vector<LogicalType> bridge_types;
    vector<LogicalType> tail_types;
};

static bool IsSingleCondInnerJoin(LogicalOperator &op) {
    if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return false;
    }
    auto &join = op.Cast<LogicalComparisonJoin>();
    if (join.join_type != JoinType::INNER || join.conditions.size() != 1) {
        return false;
    }
    return join.conditions[0].comparison == ExpressionType::COMPARE_EQUAL;
}

static bool ExtractBinding(Expression &expr, ColumnBinding &binding) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
        binding = expr.Cast<BoundColumnRefExpression>().binding;
        return true;
    }
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
        return ExtractBinding(*expr.Cast<BoundCastExpression>().child, binding);
    }
    return false;
}

static idx_t FindBindingIndex(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
    for (idx_t i = 0; i < bindings.size(); i++) {
        if (bindings[i] == binding) {
            return i;
        }
    }
    return DConstants::INVALID_INDEX;
}

static bool BindingInSet(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
    return FindBindingIndex(bindings, binding) != DConstants::INVALID_INDEX;
}

static bool MatchFinalBagPattern(LogicalComparisonJoin &top_join, FinalBagPattern &pattern) {
    if (top_join.join_type != JoinType::INNER || top_join.conditions.size() != 1 ||
        top_join.conditions[0].comparison != ExpressionType::COMPARE_EQUAL) {
        return false;
    }

    idx_t nested_idx = DConstants::INVALID_INDEX;
    idx_t tail_idx = DConstants::INVALID_INDEX;
    if (IsSingleCondInnerJoin(*top_join.children[0]) && !IsSingleCondInnerJoin(*top_join.children[1])) {
        nested_idx = 0;
        tail_idx = 1;
    } else if (!IsSingleCondInnerJoin(*top_join.children[0]) && IsSingleCondInnerJoin(*top_join.children[1])) {
        nested_idx = 1;
        tail_idx = 0;
    } else {
        return false;
    }

    auto &nested_join = top_join.children[nested_idx]->Cast<LogicalComparisonJoin>();
    auto nested_left_bindings = nested_join.children[0]->GetColumnBindings();
    auto nested_right_bindings = nested_join.children[1]->GetColumnBindings();
    auto tail_bindings = top_join.children[tail_idx]->GetColumnBindings();

    ColumnBinding top_left_binding, top_right_binding;
    if (!ExtractBinding(*top_join.conditions[0].left, top_left_binding) ||
        !ExtractBinding(*top_join.conditions[0].right, top_right_binding)) {
        return false;
    }

    ColumnBinding nested_binding, tail_binding;
    if (BindingInSet(tail_bindings, top_left_binding) && !BindingInSet(tail_bindings, top_right_binding)) {
        tail_binding = top_left_binding;
        nested_binding = top_right_binding;
    } else if (!BindingInSet(tail_bindings, top_left_binding) && BindingInSet(tail_bindings, top_right_binding)) {
        nested_binding = top_left_binding;
        tail_binding = top_right_binding;
    } else {
        return false;
    }

    idx_t bridge_child_idx = DConstants::INVALID_INDEX;
    if (BindingInSet(nested_left_bindings, nested_binding) && !BindingInSet(nested_right_bindings, nested_binding)) {
        bridge_child_idx = 0;
    } else if (!BindingInSet(nested_left_bindings, nested_binding) && BindingInSet(nested_right_bindings, nested_binding)) {
        bridge_child_idx = 1;
    } else {
        return false;
    }
    idx_t head_child_idx = 1 - bridge_child_idx;

    auto head_bindings = nested_join.children[head_child_idx]->GetColumnBindings();
    auto bridge_bindings = nested_join.children[bridge_child_idx]->GetColumnBindings();

    ColumnBinding inner_left_binding, inner_right_binding;
    if (!ExtractBinding(*nested_join.conditions[0].left, inner_left_binding) ||
        !ExtractBinding(*nested_join.conditions[0].right, inner_right_binding)) {
        return false;
    }

    ColumnBinding head_key_binding, bridge_head_key_binding;
    if (BindingInSet(head_bindings, inner_left_binding) && BindingInSet(bridge_bindings, inner_right_binding)) {
        head_key_binding = inner_left_binding;
        bridge_head_key_binding = inner_right_binding;
    } else if (BindingInSet(head_bindings, inner_right_binding) && BindingInSet(bridge_bindings, inner_left_binding)) {
        head_key_binding = inner_right_binding;
        bridge_head_key_binding = inner_left_binding;
    } else {
        return false;
    }

    auto head_key_idx = FindBindingIndex(head_bindings, head_key_binding);
    auto bridge_head_key_idx = FindBindingIndex(bridge_bindings, bridge_head_key_binding);
    auto bridge_tail_key_idx = FindBindingIndex(bridge_bindings, nested_binding);
    auto tail_key_idx = FindBindingIndex(tail_bindings, tail_binding);
    if (head_key_idx == DConstants::INVALID_INDEX || bridge_head_key_idx == DConstants::INVALID_INDEX ||
        bridge_tail_key_idx == DConstants::INVALID_INDEX || tail_key_idx == DConstants::INVALID_INDEX) {
        return false;
    }

    pattern.nested_idx = nested_idx;
    pattern.tail_idx = tail_idx;
    pattern.head_child_idx = head_child_idx;
    pattern.bridge_child_idx = bridge_child_idx;
    pattern.head_key_idx = head_key_idx;
    pattern.bridge_head_key_idx = bridge_head_key_idx;
    pattern.bridge_tail_key_idx = bridge_tail_key_idx;
    pattern.tail_key_idx = tail_key_idx;
    pattern.head_key_binding = head_key_binding;
    pattern.head_bindings = std::move(head_bindings);
    pattern.bridge_bindings = std::move(bridge_bindings);
    pattern.tail_bindings = std::move(tail_bindings);
    pattern.head_types = nested_join.children[head_child_idx]->types;
    pattern.bridge_types = nested_join.children[bridge_child_idx]->types;
    pattern.tail_types = top_join.children[tail_idx]->types;
    return true;
}

} // namespace

bool TryRewriteNativeFinalBagPreagg(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                    LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                    AggJoinRewriteState &state, bool has_parent) {
    bool grouped = agg.groups.size() == 1;
    bool ungrouped = agg.groups.empty();
    if ((!grouped && !ungrouped) || agg.expressions.empty()) {
        return false;
    }

    FinalBagPattern pattern;
    if (!MatchFinalBagPattern(join, pattern)) {
        return false;
    }

    auto get_est = [](LogicalOperator &node) -> idx_t {
        return node.has_estimated_cardinality ? node.estimated_cardinality : 0;
    };
    auto &nested_join = join.children[pattern.nested_idx]->Cast<LogicalComparisonJoin>();
    auto head_est = get_est(*nested_join.children[pattern.head_child_idx]);
    auto bridge_est = get_est(*nested_join.children[pattern.bridge_child_idx]);
    auto tail_est = get_est(*join.children[pattern.tail_idx]);
    auto group_est = agg.has_estimated_cardinality ? agg.estimated_cardinality : 0;
    if (head_est == 0 || bridge_est == 0 || tail_est == 0 || group_est == 0) {
        return false;
    }
    if (head_est < 100000 || bridge_est < 100000 || tail_est < 100000) {
        return false;
    }
    auto downstream_est = MaxValue<idx_t>(bridge_est, tail_est);
    if (ungrouped && downstream_est / 4 > head_est) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr,
                    "[AGGJOIN] native-final-bag skip: ungrouped head too small for downstream (head_est=%llu, bridge_est=%llu, tail_est=%llu)\n",
                    (unsigned long long)head_est, (unsigned long long)bridge_est,
                    (unsigned long long)tail_est);
        }
        return false;
    }

    auto child_bindings = agg_child.GetColumnBindings();
    auto top_join_bindings = join.GetColumnBindings();
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

    CompressInfo group_compress;
    if (grouped) {
        auto group_child_idx = resolve_binding(*agg.groups[0]);
        auto group_join_idx = group_child_idx == DConstants::INVALID_INDEX ? DConstants::INVALID_INDEX
                                                                           : TraceProjectionChain(agg_child, group_child_idx);
        if (group_join_idx == DConstants::INVALID_INDEX || group_join_idx >= top_join_bindings.size() ||
            top_join_bindings[group_join_idx] != pattern.head_key_binding) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr, "[AGGJOIN] native-final-bag skip: group is not head key\n");
            }
            return false;
        }
        group_compress = FindCompressInChain(agg_child, group_child_idx);
    }

    vector<FinalBagAggInfo> final_aggs;
    final_aggs.reserve(agg.expressions.size());
    bool saw_head_payload = false;
    bool saw_tail_payload = false;
    for (auto &expr : agg.expressions) {
        auto &ba = expr->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        FinalBagAggInfo info;
        info.fn = fn;
        info.result_type = ba.return_type;

        if (fn != "SUM" && fn != "COUNT" && fn != "COUNT_STAR" && fn != "AVG" && fn != "MIN" && fn != "MAX") {
            return false;
        }
        if (ba.IsDistinct() || ba.filter || ba.order_bys) {
            return false;
        }
        if (fn == "COUNT_STAR") {
            final_aggs.push_back(std::move(info));
            continue;
        }
        if (ba.children.empty()) {
            return false;
        }
        auto child_idx = resolve_binding(*ba.children[0]);
        auto join_idx = child_idx == DConstants::INVALID_INDEX ? DConstants::INVALID_INDEX
                                                               : TraceProjectionChain(agg_child, child_idx);
        if (join_idx == DConstants::INVALID_INDEX || join_idx >= top_join_bindings.size()) {
            return false;
        }
        auto &binding = top_join_bindings[join_idx];
        if (BindingInSet(pattern.bridge_bindings, binding)) {
            return false;
        }
        if (BindingInSet(pattern.head_bindings, binding)) {
            if (fn == "MIN" || fn == "MAX") {
                return false;
            }
            if ((fn == "SUM" || fn == "AVG") && !ba.children[0]->return_type.IsNumeric()) {
                return false;
            }
            info.on_head = true;
            info.side_col = FindBindingIndex(pattern.head_bindings, binding);
            saw_head_payload = true;
        } else if (BindingInSet(pattern.tail_bindings, binding)) {
            if ((fn == "SUM" || fn == "AVG") && !ba.children[0]->return_type.IsNumeric()) {
                return false;
            }
            info.on_head = false;
            info.side_col = FindBindingIndex(pattern.tail_bindings, binding);
            saw_tail_payload = true;
        } else {
            return false;
        }
        final_aggs.push_back(std::move(info));
    }

    if (!saw_head_payload || !saw_tail_payload) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] native-final-bag skip: aggregates are not split across head and tail\n");
        }
        return false;
    }

    auto head_group_index = optimizer.binder.GenerateTableIndex();
    auto head_agg_index = optimizer.binder.GenerateTableIndex();
    vector<unique_ptr<Expression>> head_aggs;
    head_aggs.push_back(BindAggregateByName(context, "count_star", {}));
    for (auto &info : final_aggs) {
        if (!info.on_head || info.fn == "COUNT_STAR") {
            continue;
        }
        auto head_ref = [&]() {
            return make_uniq<BoundColumnRefExpression>(pattern.head_types[info.side_col],
                                                       pattern.head_bindings[info.side_col]);
        };
        if (info.fn == "SUM") {
            vector<unique_ptr<Expression>> children;
            children.push_back(head_ref());
            info.sum_slot = head_aggs.size();
            head_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
        } else if (info.fn == "COUNT") {
            vector<unique_ptr<Expression>> children;
            children.push_back(head_ref());
            info.count_slot = head_aggs.size();
            head_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
        } else if (info.fn == "AVG") {
            {
                vector<unique_ptr<Expression>> children;
                children.push_back(head_ref());
                info.sum_slot = head_aggs.size();
                head_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
            {
                vector<unique_ptr<Expression>> children;
                children.push_back(head_ref());
                info.count_slot = head_aggs.size();
                head_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
            }
        }
    }
    auto head_child = std::move(nested_join.children[pattern.head_child_idx]);
    auto bridge_child = std::move(nested_join.children[pattern.bridge_child_idx]);
    auto tail_child = std::move(join.children[pattern.tail_idx]);

    auto head_preagg = make_uniq<LogicalAggregate>(head_group_index, head_agg_index, std::move(head_aggs));
    head_preagg->groups.push_back(
        make_uniq<BoundColumnRefExpression>(pattern.head_types[pattern.head_key_idx],
                                            pattern.head_bindings[pattern.head_key_idx]));
    head_preagg->children.push_back(std::move(head_child));
    head_preagg->ResolveOperatorTypes();

    auto tail_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    JoinCondition tail_cond;
    tail_cond.comparison = ExpressionType::COMPARE_EQUAL;
    tail_cond.left = make_uniq<BoundColumnRefExpression>(pattern.bridge_types[pattern.bridge_tail_key_idx],
                                                         pattern.bridge_bindings[pattern.bridge_tail_key_idx]);
    tail_cond.right = make_uniq<BoundColumnRefExpression>(pattern.tail_types[pattern.tail_key_idx],
                                                          pattern.tail_bindings[pattern.tail_key_idx]);
    if (pattern.bridge_types[pattern.bridge_tail_key_idx] != pattern.tail_types[pattern.tail_key_idx]) {
        tail_cond.right = BoundCastExpression::AddCastToType(context, std::move(tail_cond.right),
                                                             pattern.bridge_types[pattern.bridge_tail_key_idx]);
    }
    tail_join->conditions.push_back(std::move(tail_cond));
    tail_join->children.push_back(std::move(bridge_child));
    tail_join->children.push_back(std::move(tail_child));
    tail_join->ResolveOperatorTypes();

    auto tail_group_index = optimizer.binder.GenerateTableIndex();
    auto tail_agg_index = optimizer.binder.GenerateTableIndex();
    vector<unique_ptr<Expression>> tail_aggs;
    tail_aggs.push_back(BindAggregateByName(context, "count_star", {}));
    for (auto &info : final_aggs) {
        if (info.on_head || info.fn == "COUNT_STAR") {
            continue;
        }
        auto tail_ref = [&]() {
            return make_uniq<BoundColumnRefExpression>(pattern.tail_types[info.side_col],
                                                       pattern.tail_bindings[info.side_col]);
        };
        if (info.fn == "SUM") {
            vector<unique_ptr<Expression>> children;
            children.push_back(tail_ref());
            info.sum_slot = tail_aggs.size();
            tail_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
        } else if (info.fn == "COUNT") {
            vector<unique_ptr<Expression>> children;
            children.push_back(tail_ref());
            info.count_slot = tail_aggs.size();
            tail_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
        } else if (info.fn == "AVG") {
            {
                vector<unique_ptr<Expression>> children;
                children.push_back(tail_ref());
                info.sum_slot = tail_aggs.size();
                tail_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
            {
                vector<unique_ptr<Expression>> children;
                children.push_back(tail_ref());
                info.count_slot = tail_aggs.size();
                tail_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
            }
        } else if (info.fn == "MIN") {
            vector<unique_ptr<Expression>> children;
            children.push_back(tail_ref());
            info.min_slot = tail_aggs.size();
            tail_aggs.push_back(BindAggregateByName(context, "min", std::move(children)));
        } else if (info.fn == "MAX") {
            vector<unique_ptr<Expression>> children;
            children.push_back(tail_ref());
            info.max_slot = tail_aggs.size();
            tail_aggs.push_back(BindAggregateByName(context, "max", std::move(children)));
        }
    }
    auto tail_preagg = make_uniq<LogicalAggregate>(tail_group_index, tail_agg_index, std::move(tail_aggs));
    tail_preagg->groups.push_back(
        make_uniq<BoundColumnRefExpression>(pattern.bridge_types[pattern.bridge_head_key_idx],
                                            pattern.bridge_bindings[pattern.bridge_head_key_idx]));
    tail_preagg->children.push_back(std::move(tail_join));
    tail_preagg->ResolveOperatorTypes();

    auto final_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    JoinCondition final_cond;
    final_cond.comparison = ExpressionType::COMPARE_EQUAL;
    final_cond.left = make_uniq<BoundColumnRefExpression>(head_preagg->types[0], ColumnBinding(head_group_index, 0));
    final_cond.right = make_uniq<BoundColumnRefExpression>(tail_preagg->types[0], ColumnBinding(tail_group_index, 0));
    if (head_preagg->types[0] != tail_preagg->types[0]) {
        final_cond.right = BoundCastExpression::AddCastToType(context, std::move(final_cond.right),
                                                              head_preagg->types[0]);
    }
    final_join->conditions.push_back(std::move(final_cond));
    final_join->children.push_back(std::move(head_preagg));
    final_join->children.push_back(std::move(tail_preagg));
    final_join->ResolveOperatorTypes();

    auto final_join_bindings = final_join->GetColumnBindings();
    auto head_group_binding = final_join_bindings[0];
    auto head_freq_binding = final_join_bindings[1];
    auto tail_group_offset = final_join->children[0]->types.size();
    auto tail_freq_binding = final_join_bindings[tail_group_offset + 1];
    auto head_slot_binding = [&](idx_t slot) { return final_join_bindings[1 + slot]; };
    auto tail_slot_binding = [&](idx_t slot) { return final_join_bindings[tail_group_offset + 1 + slot]; };
    auto head_slot_type = [&](idx_t slot) -> const LogicalType & { return final_join->types[1 + slot]; };
    auto tail_slot_type = [&](idx_t slot) -> const LogicalType & {
        return final_join->types[tail_group_offset + 1 + slot];
    };

    unique_ptr<LogicalOperator> replacement;
    idx_t output_index = DConstants::INVALID_INDEX;
    idx_t output_base = 0;

    if (grouped) {
        auto proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> proj_exprs;
        proj_exprs.reserve(1 + final_aggs.size());
        {
            auto group_ref = make_uniq<BoundColumnRefExpression>(final_join->types[0], head_group_binding);
            auto group_type =
                (group_compress.has_compress && group_compress.is_string_compress) ? group_compress.original_type
                                                                                   : agg.types[0];
            proj_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(group_ref), group_type));
        }
        for (auto &info : final_aggs) {
            unique_ptr<Expression> final_expr;
            if (info.fn == "COUNT_STAR") {
                auto head_freq = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, head_freq_binding);
                auto tail_freq = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, tail_freq_binding);
                auto cast_head = BoundCastExpression::AddCastToType(context, std::move(head_freq), info.result_type);
                auto cast_tail = BoundCastExpression::AddCastToType(context, std::move(tail_freq), info.result_type);
                final_expr = optimizer.BindScalarFunction("*", std::move(cast_head), std::move(cast_tail));
            } else if (info.on_head) {
                if (info.fn == "AVG") {
                    auto sum_ref = make_uniq<BoundColumnRefExpression>(head_slot_type(info.sum_slot),
                                                                       head_slot_binding(info.sum_slot));
                    auto count_ref = make_uniq<BoundColumnRefExpression>(head_slot_type(info.count_slot),
                                                                         head_slot_binding(info.count_slot));
                    auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(sum_ref), info.result_type);
                    auto cast_count =
                        BoundCastExpression::AddCastToType(context, std::move(count_ref), info.result_type);
                    final_expr = optimizer.BindScalarFunction("/", std::move(cast_sum), std::move(cast_count));
                } else {
                    idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                    auto side_ref =
                        make_uniq<BoundColumnRefExpression>(head_slot_type(slot), head_slot_binding(slot));
                    auto tail_freq = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, tail_freq_binding);
                    auto cast_side = BoundCastExpression::AddCastToType(context, std::move(side_ref), info.result_type);
                    auto cast_freq = BoundCastExpression::AddCastToType(context, std::move(tail_freq), info.result_type);
                    final_expr = optimizer.BindScalarFunction("*", std::move(cast_side), std::move(cast_freq));
                }
            } else {
                if (info.fn == "AVG") {
                    auto sum_ref = make_uniq<BoundColumnRefExpression>(tail_slot_type(info.sum_slot),
                                                                       tail_slot_binding(info.sum_slot));
                    auto count_ref = make_uniq<BoundColumnRefExpression>(tail_slot_type(info.count_slot),
                                                                         tail_slot_binding(info.count_slot));
                    auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(sum_ref), info.result_type);
                    auto cast_count =
                        BoundCastExpression::AddCastToType(context, std::move(count_ref), info.result_type);
                    final_expr = optimizer.BindScalarFunction("/", std::move(cast_sum), std::move(cast_count));
                } else if (info.fn == "MIN" || info.fn == "MAX") {
                    idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                    auto side_ref =
                        make_uniq<BoundColumnRefExpression>(tail_slot_type(slot), tail_slot_binding(slot));
                    final_expr = BoundCastExpression::AddCastToType(context, std::move(side_ref), info.result_type);
                } else {
                    idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                    auto side_ref =
                        make_uniq<BoundColumnRefExpression>(tail_slot_type(slot), tail_slot_binding(slot));
                    auto head_freq = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, head_freq_binding);
                    auto cast_side = BoundCastExpression::AddCastToType(context, std::move(side_ref), info.result_type);
                    auto cast_freq = BoundCastExpression::AddCastToType(context, std::move(head_freq), info.result_type);
                    final_expr = optimizer.BindScalarFunction("*", std::move(cast_side), std::move(cast_freq));
                }
            }
            proj_exprs.push_back(std::move(final_expr));
        }

        auto proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_exprs));
        proj->children.push_back(std::move(final_join));
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
        vector<idx_t> sum_slots(final_aggs.size(), DConstants::INVALID_INDEX);
        vector<idx_t> count_slots(final_aggs.size(), DConstants::INVALID_INDEX);
        for (idx_t a = 0; a < final_aggs.size(); a++) {
            auto &info = final_aggs[a];
            auto weighted_side = [&](idx_t slot, bool side_on_head, bool use_count_slot) -> unique_ptr<Expression> {
                auto side_type = side_on_head ? head_slot_type(slot) : tail_slot_type(slot);
                auto side_binding = side_on_head ? head_slot_binding(slot) : tail_slot_binding(slot);
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_type, side_binding);
                auto cast_side =
                    BoundCastExpression::AddCastToType(context, std::move(side_ref), info.result_type);
                auto other_freq_binding = side_on_head ? tail_freq_binding : head_freq_binding;
                auto other_freq_ref = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, other_freq_binding);
                auto cast_freq =
                    BoundCastExpression::AddCastToType(context, std::move(other_freq_ref), info.result_type);
                return optimizer.BindScalarFunction("*", std::move(cast_side), std::move(cast_freq));
            };
            if (info.fn == "COUNT_STAR") {
                auto head_freq = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, head_freq_binding);
                auto tail_freq = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, tail_freq_binding);
                auto cast_head = BoundCastExpression::AddCastToType(context, std::move(head_freq), info.result_type);
                auto cast_tail = BoundCastExpression::AddCastToType(context, std::move(tail_freq), info.result_type);
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(cast_head), std::move(cast_tail)));
            } else if (info.fn == "AVG") {
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(weighted_side(info.sum_slot, info.on_head, false));
                count_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(weighted_side(info.count_slot, info.on_head, true));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                auto side_ref = make_uniq<BoundColumnRefExpression>(tail_slot_type(slot), tail_slot_binding(slot));
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(side_ref), info.result_type));
            } else {
                idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                sum_slots[a] = contrib_exprs.size();
                contrib_exprs.push_back(weighted_side(slot, info.on_head, info.fn == "COUNT"));
            }
        }
        auto contrib_proj = make_uniq<LogicalProjection>(contrib_index, std::move(contrib_exprs));
        contrib_proj->children.push_back(std::move(final_join));
        contrib_proj->ResolveOperatorTypes();

        auto final_agg_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_aggs_exprs;
        for (idx_t a = 0; a < final_aggs.size(); a++) {
            auto &info = final_aggs[a];
            if (info.fn == "AVG") {
                for (auto slot : {sum_slots[a], count_slots[a]}) {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[slot], ColumnBinding(contrib_index, slot)));
                    final_aggs_exprs.push_back(BindAggregateByName(context, "sum", std::move(children)));
                }
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[sum_slots[a]],
                                                                       ColumnBinding(contrib_index, sum_slots[a])));
                final_aggs_exprs.push_back(BindAggregateByName(context, StringUtil::Lower(info.fn), std::move(children)));
            } else {
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[sum_slots[a]],
                                                                       ColumnBinding(contrib_index, sum_slots[a])));
                final_aggs_exprs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
        }
        auto final_agg = make_uniq<LogicalAggregate>(DConstants::INVALID_INDEX, final_agg_index, std::move(final_aggs_exprs));
        final_agg->children.push_back(std::move(contrib_proj));
        final_agg->ResolveOperatorTypes();

        auto final_proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_exprs;
        final_exprs.reserve(final_aggs.size());
        for (idx_t a = 0; a < final_aggs.size(); a++) {
            auto &info = final_aggs[a];
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
        if (grouped) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.group_index, 0), ColumnBinding(output_index, 0));
        }
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.aggregate_index, a),
                                                    ColumnBinding(output_index, output_base + a));
        }
    }
    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] planner rewrite: native final-bag preagg (groups=%zu, aggs=%zu, head_est=%llu, bridge_est=%llu, tail_est=%llu, group_est=%llu)\n",
                agg.groups.size(), agg.expressions.size(), (unsigned long long)head_est,
                (unsigned long long)bridge_est, (unsigned long long)tail_est, (unsigned long long)group_est);
    }
    op = std::move(replacement);
    return true;
}

} // namespace duckdb
