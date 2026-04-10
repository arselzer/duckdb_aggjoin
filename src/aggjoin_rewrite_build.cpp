#include "aggjoin_rewrites_internal.hpp"

namespace duckdb {
bool TryRewriteNativeBuildPreagg(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
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
    auto key_count = col.probe_key_cols.size();
    bool grouped_by_join_key = agg.groups.size() == 1 && key_count == 1;
    bool ungrouped = agg.groups.empty();
    bool grouped_by_join_key_subset = false;
    if (!ungrouped && agg.groups.size() == 1 && key_count == 2 && col.group_cols.size() == 1 &&
        col.group_cols[0] == col.probe_key_cols[0]) {
        grouped_by_join_key_subset = true;
    }
    if (join.conditions.size() != key_count || (!grouped_by_join_key && !ungrouped && !grouped_by_join_key_subset)) {
        return false;
    }
    if ((!ungrouped && col.group_cols.size() != agg.groups.size()) || col.probe_key_cols.size() != col.build_key_cols.size() ||
        key_count == 0) {
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
    bool probe_key_is_varlen = false;
    if (key_count == 1) {
        auto &probe_side = *(need_swap ? join.children[1] : join.children[0]);
        auto key_idx = col.probe_key_cols[0];
        if (key_idx < probe_side.types.size()) {
            auto key_id = probe_side.types[key_idx].id();
            probe_key_is_varlen = key_id == LogicalTypeId::VARCHAR || key_id == LogicalTypeId::BLOB;
        }
    }
    bool mid_fanout_varlen_probe_shape = !payload_on_build && grouped_by_join_key && key_count == 1 &&
                                         probe_key_is_varlen &&
                                         probe_est >= 100000 && build_est >= 100000 &&
                                         probe_est <= build_est * 4 && build_est <= probe_est * 4 &&
                                         group_est >= 20000 && group_est <= 100000;
    if (!large_balanced_shape && !mid_fanout_varlen_probe_shape) {
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

    auto &payload_key_idxs = payload_on_build ? col.build_key_cols : col.probe_key_cols;
    auto &count_key_idxs = payload_on_build ? col.probe_key_cols : col.build_key_cols;
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
    for (auto key_idx : count_key_idxs) {
        count_preagg->groups.push_back(make_uniq<BoundColumnRefExpression>(
            count_types[key_idx], count_side_bindings[key_idx]));
    }
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
    for (auto key_idx : payload_key_idxs) {
        build_preagg->groups.push_back(make_uniq<BoundColumnRefExpression>(
            payload_types[key_idx], payload_side_bindings[key_idx]));
    }
    build_preagg->children.push_back(std::move(payload_child));
    build_preagg->ResolveOperatorTypes();

    auto native_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    for (idx_t i = 0; i < key_count; i++) {
        JoinCondition cond;
        cond.comparison = ExpressionType::COMPARE_EQUAL;
        cond.left = make_uniq<BoundColumnRefExpression>(count_preagg->types[i], ColumnBinding(count_group_index, i));
        cond.right = make_uniq<BoundColumnRefExpression>(build_preagg->types[i], ColumnBinding(build_group_index, i));
        if (count_preagg->types[i] != build_preagg->types[i]) {
            cond.right = BoundCastExpression::AddCastToType(context, std::move(cond.right), count_preagg->types[i]);
        }
        native_join->conditions.push_back(std::move(cond));
    }
    native_join->children.push_back(std::move(count_preagg));
    native_join->children.push_back(std::move(build_preagg));
    native_join->ResolveOperatorTypes();

    auto native_join_bindings = native_join->GetColumnBindings();
    auto probe_count_binding = native_join_bindings[key_count];
    auto build_group_offset = native_join->children[0]->types.size();
    auto build_base = build_group_offset + key_count;

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
            proj_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(group_expr),
                                                                    GetRewriteGroupType(col, agg, 0)));
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
    } else if (grouped_by_join_key_subset) {
        auto contrib_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> contrib_exprs;
        contrib_exprs.push_back(make_uniq<BoundColumnRefExpression>(native_join->types[0], native_join_bindings[0]));
        idx_t contrib_value_base = 1;
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
                sum_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(num_mul), std::move(cast_sum)));

                auto den_mul = build_probe_multiplier();
                auto build_count =
                    make_uniq<BoundColumnRefExpression>(native_join->types[build_base + info.count_slot],
                                                        native_join_bindings[build_base + info.count_slot]);
                auto cast_count = BoundCastExpression::AddCastToType(context, std::move(build_count), result_type);
                count_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(den_mul), std::move(cast_count)));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                auto build_expr = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + slot],
                                                                      native_join_bindings[build_base + slot]);
                sum_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(build_expr), result_type));
            } else {
                auto mul = build_probe_multiplier();
                idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                auto build_expr = make_uniq<BoundColumnRefExpression>(native_join->types[build_base + slot],
                                                                      native_join_bindings[build_base + slot]);
                auto cast_value = BoundCastExpression::AddCastToType(context, std::move(build_expr), result_type);
                sum_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul), std::move(cast_value)));
            }
        }
        auto contrib_proj = make_uniq<LogicalProjection>(contrib_index, std::move(contrib_exprs));
        contrib_proj->children.push_back(std::move(native_join));
        contrib_proj->ResolveOperatorTypes();

        auto final_group_index = optimizer.binder.GenerateTableIndex();
        auto final_agg_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_aggs;
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            auto &info = native_aggs[a];
            auto fn = info.fn;
            if (fn == "AVG") {
                for (auto slot : {sum_slots[a], count_slots[a]}) {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[contrib_value_base + slot],
                                                                           ColumnBinding(contrib_index, contrib_value_base + slot)));
                    final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
                }
            } else if (fn == "MIN" || fn == "MAX") {
                auto slot = sum_slots[a];
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[contrib_value_base + slot],
                                                                       ColumnBinding(contrib_index, contrib_value_base + slot)));
                final_aggs.push_back(BindAggregateByName(context, StringUtil::Lower(fn), std::move(children)));
            } else {
                auto slot = sum_slots[a];
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[contrib_value_base + slot],
                                                                       ColumnBinding(contrib_index, contrib_value_base + slot)));
                final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
        }
        auto final_agg = make_uniq<LogicalAggregate>(final_group_index, final_agg_index, std::move(final_aggs));
        final_agg->groups.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[0], ColumnBinding(contrib_index, 0)));
        final_agg->children.push_back(std::move(contrib_proj));
        final_agg->ResolveOperatorTypes();

        auto final_bindings = final_agg->GetColumnBindings();
        auto final_proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_exprs;
        final_exprs.reserve(1 + agg.expressions.size());
        final_exprs.push_back(BoundCastExpression::AddCastToType(
            context, make_uniq<BoundColumnRefExpression>(final_agg->types[0], final_bindings[0]),
            GetRewriteGroupType(col, agg, 0)));
        idx_t final_agg_base = 1;
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            auto &info = native_aggs[a];
            auto result_type = info.result_type;
            unique_ptr<Expression> final_expr;
            if (info.fn == "AVG") {
                auto num_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[final_agg_base + sum_slots[a]],
                                                                   final_bindings[final_agg_base + sum_slots[a]]);
                auto den_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[final_agg_base + count_slots[a]],
                                                                   final_bindings[final_agg_base + count_slots[a]]);
                auto cast_num = BoundCastExpression::AddCastToType(context, std::move(num_ref), result_type);
                auto cast_den = BoundCastExpression::AddCastToType(context, std::move(den_ref), result_type);
                final_expr = optimizer.BindScalarFunction("/", std::move(cast_num), std::move(cast_den));
            } else {
                auto sum_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[final_agg_base + sum_slots[a]],
                                                                   final_bindings[final_agg_base + sum_slots[a]]);
                final_expr = BoundCastExpression::AddCastToType(context, std::move(sum_ref), result_type);
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
        output_base = 1;
        replacement = std::move(final_proj);
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
        if (grouped_by_join_key || grouped_by_join_key_subset) {
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

} // namespace duckdb
