#include "aggjoin_rewrites_internal.hpp"

namespace duckdb {
bool TryRewriteNativeMixedSidePreagg(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                     LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                     const AggJoinColInfo &col, bool need_swap, AggJoinRewriteState &state,
                                     bool has_parent) {
    struct MixedAggInfo {
        string fn;
        LogicalType result_type;
        bool on_build = false;
        idx_t side_col = DConstants::INVALID_INDEX;
        idx_t sum_slot = DConstants::INVALID_INDEX;
        idx_t count_slot = DConstants::INVALID_INDEX;
        idx_t min_slot = DConstants::INVALID_INDEX;
        idx_t max_slot = DConstants::INVALID_INDEX;
    };

    bool grouped_by_join_key = agg.groups.size() == col.probe_key_cols.size() && !agg.groups.empty();
    bool ungrouped = agg.groups.empty();
    bool grouped_by_join_key_subset = !ungrouped && agg.groups.size() < col.probe_key_cols.size();
    if (!grouped_by_join_key && !ungrouped && !grouped_by_join_key_subset) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: groups do not match join key envelope\n");
        return false;
    }
    if (col.probe_key_cols.empty() || col.build_key_cols.empty() ||
        col.probe_key_cols.size() != col.build_key_cols.size()) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: key extraction mismatch\n");
        return false;
    }
    vector<idx_t> group_key_positions;
    if (!ungrouped) {
        if (col.group_cols.size() != agg.groups.size()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: group count mismatch\n");
            return false;
        }
        idx_t prev_pos = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < col.group_cols.size(); i++) {
            idx_t matched = DConstants::INVALID_INDEX;
            for (idx_t k = 0; k < col.probe_key_cols.size(); k++) {
                if (col.group_cols[i] == col.probe_key_cols[k]) {
                    matched = k;
                    break;
                }
            }
            if (matched == DConstants::INVALID_INDEX || (prev_pos != DConstants::INVALID_INDEX && matched <= prev_pos)) {
                if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: group cols are not an ordered subset of probe keys\n");
                return false;
            }
            group_key_positions.push_back(matched);
            prev_pos = matched;
        }
    }
    auto key_count = col.probe_key_cols.size();
    if (grouped_by_join_key_subset && !(key_count == 2 && group_key_positions.size() == 1)) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: grouped subset envelope too wide\n");
        return false;
    }
    if (join.conditions.size() != key_count) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: join condition count != key count\n");
        return false;
    }

    bool saw_payload_on_build = false;
    bool saw_payload_on_probe = false;
    for (idx_t a = 0; a < agg.expressions.size(); a++) {
        auto &ba = agg.expressions[a]->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        if (fn == "COUNT_STAR") {
            saw_payload_on_probe = true;
            saw_payload_on_build = true;
            continue;
        }
        if (a >= col.agg_on_build.size()) {
            return false;
        }
        if (col.agg_on_build[a]) saw_payload_on_build = true;
        else saw_payload_on_probe = true;
    }
    if (!saw_payload_on_build || !saw_payload_on_probe) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: aggregates are not on both sides\n");
        return false;
    }

    auto get_est = [](LogicalOperator &node) -> idx_t {
        return node.has_estimated_cardinality ? node.estimated_cardinality : 0;
    };
    auto probe_est = get_est(*(need_swap ? join.children[1] : join.children[0]));
    auto build_est = get_est(*(need_swap ? join.children[0] : join.children[1]));
    auto group_est = agg.has_estimated_cardinality ? agg.estimated_cardinality : 0;
    if (probe_est == 0 || build_est == 0 || group_est == 0) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: missing estimates\n");
        return false;
    }

    auto join_bindings = join.GetColumnBindings();
    auto child_bindings = agg_child.GetColumnBindings();
    auto &probe_child_ref = *(need_swap ? join.children[1] : join.children[0]);
    auto &build_child_ref = *(need_swap ? join.children[0] : join.children[1]);
    auto probe_payload_bindings = probe_child_ref.GetColumnBindings();
    auto build_payload_bindings = build_child_ref.GetColumnBindings();

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

    vector<MixedAggInfo> mixed_aggs;
    mixed_aggs.reserve(agg.expressions.size());
    bool probe_only_nonnumeric_minmax = true;
    bool probe_only_linear_numeric = true;
    bool probe_only_linear_or_nonnumeric_minmax = true;
    bool build_only_nonnumeric_minmax = true;
    bool build_only_linear_numeric = true;
    bool build_only_linear_or_nonnumeric_minmax = true;
    bool saw_probe_nonnumeric_minmax = false;
    bool saw_probe_linear_numeric = false;
    bool saw_build_nonnumeric_minmax = false;
    bool saw_build_linear_numeric = false;
    for (idx_t a = 0; a < agg.expressions.size(); a++) {
        auto &ba = agg.expressions[a]->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        MixedAggInfo info;
        info.fn = fn;
        info.result_type = ba.return_type;
        if (fn != "SUM" && fn != "COUNT" && fn != "COUNT_STAR" && fn != "AVG" && fn != "MIN" && fn != "MAX") {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: unsupported agg fn %s\n", fn.c_str());
            return false;
        }
        if (ba.IsDistinct() || ba.filter || ba.order_bys) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: distinct/filter/order agg\n");
            return false;
        }
        if (fn == "COUNT_STAR") {
            mixed_aggs.push_back(std::move(info));
            continue;
        }
        if (ba.children.empty()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: empty agg children\n");
            return false;
        }
        if ((fn == "SUM" || fn == "AVG") && !ba.children[0]->return_type.IsNumeric()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: nonnumeric SUM/AVG input\n");
            return false;
        }
        if (a >= col.agg_on_build.size()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: agg_on_build size mismatch\n");
            return false;
        }
        info.on_build = col.agg_on_build[a];
        auto child_idx = resolve_binding(*ba.children[0]);
        auto join_idx = child_idx == DConstants::INVALID_INDEX ? DConstants::INVALID_INDEX
                                                               : TraceProjectionChain(agg_child, child_idx);
        if (join_idx == DConstants::INVALID_INDEX || join_idx >= join_bindings.size()) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: failed to trace agg binding\n");
            return false;
        }
        auto &payload_bindings = info.on_build ? build_payload_bindings : probe_payload_bindings;
        idx_t payload_idx = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < payload_bindings.size(); i++) {
            if (payload_bindings[i] == join_bindings[join_idx]) {
                payload_idx = i;
                break;
            }
        }
        if (payload_idx == DConstants::INVALID_INDEX) {
            if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: failed to map payload binding to side child\n");
            return false;
        }
        info.side_col = payload_idx;
        bool is_linear_numeric = fn == "SUM" || fn == "COUNT" || fn == "COUNT_STAR" || fn == "AVG";
        bool is_nonnumeric_minmax = (fn == "MIN" || fn == "MAX") && !ba.children[0]->return_type.IsNumeric();
        if (info.on_build) {
            if (is_linear_numeric) {
                saw_build_linear_numeric = true;
            } else if (!is_nonnumeric_minmax) {
                build_only_linear_or_nonnumeric_minmax = false;
            }
            if (!(fn == "MIN" || fn == "MAX")) {
                build_only_nonnumeric_minmax = false;
            } else if (ba.children[0]->return_type.IsNumeric()) {
                build_only_nonnumeric_minmax = false;
            } else {
                saw_build_nonnumeric_minmax = true;
            }
            if (!(fn == "SUM" || fn == "COUNT" || fn == "COUNT_STAR" || fn == "AVG")) {
                build_only_linear_numeric = false;
            }
        } else {
            if (is_linear_numeric) {
                saw_probe_linear_numeric = true;
            } else if (!is_nonnumeric_minmax) {
                probe_only_linear_or_nonnumeric_minmax = false;
            }
            if (!(fn == "SUM" || fn == "COUNT" || fn == "COUNT_STAR" || fn == "AVG")) {
                probe_only_linear_numeric = false;
            }
            if (!(fn == "MIN" || fn == "MAX")) {
                probe_only_nonnumeric_minmax = false;
            } else if (ba.children[0]->return_type.IsNumeric()) {
                probe_only_nonnumeric_minmax = false;
            } else {
                saw_probe_nonnumeric_minmax = true;
            }
        }
        mixed_aggs.push_back(std::move(info));
    }

    bool large_balanced_shape = probe_est >= 250000 && build_est >= 250000 &&
                                probe_est <= build_est * 4 && build_est <= probe_est * 4;
    bool probe_heavy_single_key = grouped_by_join_key && key_count == 1 &&
                                  probe_est >= 500000 && build_est >= 50000 &&
                                  probe_est >= build_est * 8;
    bool asymmetric_composite_probe_nonnumeric = key_count > 1 &&
                                                 (grouped_by_join_key || grouped_by_join_key_subset || ungrouped) &&
                                                 probe_est >= 100000 && build_est >= 100000 &&
                                                 probe_only_nonnumeric_minmax && saw_probe_nonnumeric_minmax &&
                                                 build_only_linear_numeric;
    bool asymmetric_composite_build_nonnumeric = key_count > 1 &&
                                                 (grouped_by_join_key || grouped_by_join_key_subset || ungrouped) &&
                                                 probe_est >= 100000 && build_est >= 100000 &&
                                                 build_only_nonnumeric_minmax && saw_build_nonnumeric_minmax &&
                                                 probe_only_linear_numeric;
    bool asymmetric_composite_probe_mixed = key_count > 1 &&
                                            (grouped_by_join_key || grouped_by_join_key_subset || ungrouped) &&
                                            probe_est >= 100000 && build_est >= 100000 &&
                                            probe_only_linear_or_nonnumeric_minmax &&
                                            build_only_linear_or_nonnumeric_minmax &&
                                            saw_probe_nonnumeric_minmax && saw_build_linear_numeric;
    bool asymmetric_composite_build_mixed = key_count > 1 &&
                                            (grouped_by_join_key || grouped_by_join_key_subset || ungrouped) &&
                                            probe_est >= 100000 && build_est >= 100000 &&
                                            probe_only_linear_or_nonnumeric_minmax &&
                                            build_only_linear_or_nonnumeric_minmax &&
                                            saw_build_nonnumeric_minmax && saw_probe_linear_numeric;
    if (!large_balanced_shape && !probe_heavy_single_key && !asymmetric_composite_probe_nonnumeric &&
        !asymmetric_composite_build_nonnumeric && !asymmetric_composite_probe_mixed &&
        !asymmetric_composite_build_mixed) {
        if (AggJoinTraceEnabled()) fprintf(stderr, "[AGGJOIN] native-mixed-side skip: outside balanced envelope\n");
        return false;
    }

    auto probe_child = need_swap ? std::move(join.children[1]) : std::move(join.children[0]);
    auto build_child = need_swap ? std::move(join.children[0]) : std::move(join.children[1]);
    auto probe_bindings = probe_child->GetColumnBindings();
    auto build_bindings = build_child->GetColumnBindings();
    auto &probe_types = probe_child->types;
    auto &build_types = build_child->types;

    auto make_side_preagg = [&](unique_ptr<LogicalOperator> child, const vector<ColumnBinding> &bindings,
                                const vector<LogicalType> &types, const vector<idx_t> &key_idxs, bool on_build_side,
                                idx_t &group_index_out, idx_t &agg_index_out) -> unique_ptr<LogicalAggregate> {
        group_index_out = optimizer.binder.GenerateTableIndex();
        agg_index_out = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> side_aggs;
        side_aggs.push_back(BindAggregateByName(context, "count_star", {}));
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            if (info.fn == "COUNT_STAR" || info.on_build != on_build_side) {
                continue;
            }
            auto build_child_ref_expr = [&](idx_t col_idx) {
                return make_uniq<BoundColumnRefExpression>(types[col_idx], bindings[col_idx]);
            };
            if (info.fn == "SUM") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.sum_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            } else if (info.fn == "COUNT") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.count_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
            } else if (info.fn == "MIN") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.min_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "min", std::move(children)));
            } else if (info.fn == "MAX") {
                vector<unique_ptr<Expression>> children;
                children.push_back(build_child_ref_expr(info.side_col));
                info.max_slot = side_aggs.size();
                side_aggs.push_back(BindAggregateByName(context, "max", std::move(children)));
            } else if (info.fn == "AVG") {
                {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(build_child_ref_expr(info.side_col));
                    info.sum_slot = side_aggs.size();
                    side_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
                }
                {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(build_child_ref_expr(info.side_col));
                    info.count_slot = side_aggs.size();
                    side_aggs.push_back(BindAggregateByName(context, "count", std::move(children)));
                }
            }
        }
        auto side_preagg = make_uniq<LogicalAggregate>(group_index_out, agg_index_out, std::move(side_aggs));
        for (auto key_idx : key_idxs) {
            side_preagg->groups.push_back(make_uniq<BoundColumnRefExpression>(types[key_idx], bindings[key_idx]));
        }
        side_preagg->children.push_back(std::move(child));
        side_preagg->ResolveOperatorTypes();
        return side_preagg;
    };

    idx_t probe_group_index, probe_aggregate_index;
    auto probe_preagg = make_side_preagg(std::move(probe_child), probe_bindings, probe_types, col.probe_key_cols,
                                         false, probe_group_index, probe_aggregate_index);
    idx_t build_group_index, build_aggregate_index;
    auto build_preagg = make_side_preagg(std::move(build_child), build_bindings, build_types, col.build_key_cols,
                                         true, build_group_index, build_aggregate_index);

    auto native_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    for (idx_t i = 0; i < key_count; i++) {
        JoinCondition cond;
        cond.comparison = ExpressionType::COMPARE_EQUAL;
        cond.left = make_uniq<BoundColumnRefExpression>(probe_preagg->types[i], ColumnBinding(probe_group_index, i));
        cond.right = make_uniq<BoundColumnRefExpression>(build_preagg->types[i], ColumnBinding(build_group_index, i));
        if (probe_preagg->types[i] != build_preagg->types[i]) {
            cond.right = BoundCastExpression::AddCastToType(context, std::move(cond.right), probe_preagg->types[i]);
        }
        native_join->conditions.push_back(std::move(cond));
    }
    native_join->children.push_back(std::move(probe_preagg));
    native_join->children.push_back(std::move(build_preagg));
    native_join->ResolveOperatorTypes();

    auto native_join_bindings = native_join->GetColumnBindings();
    auto probe_count_binding = native_join_bindings[key_count];
    auto build_group_offset = native_join->children[0]->types.size();
    auto build_count_binding = native_join_bindings[build_group_offset + key_count];
    idx_t probe_extra_base = key_count;
    idx_t build_extra_base = build_group_offset + key_count;

    auto other_count_ref = [&](MixedAggInfo &info) -> unique_ptr<Expression> {
        auto binding = info.on_build ? probe_count_binding : build_count_binding;
        auto ref = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, binding);
        return BoundCastExpression::AddCastToType(context, std::move(ref), info.result_type);
    };
    auto side_slot_binding = [&](MixedAggInfo &info, idx_t slot) -> ColumnBinding {
        auto idx = (info.on_build ? build_extra_base : probe_extra_base) + slot;
        return native_join_bindings[idx];
    };
    auto side_slot_type = [&](MixedAggInfo &info, idx_t slot) -> const LogicalType & {
        auto idx = (info.on_build ? build_extra_base : probe_extra_base) + slot;
        return native_join->types[idx];
    };

    unique_ptr<LogicalOperator> replacement;
    idx_t output_index = DConstants::INVALID_INDEX;
    idx_t output_base = 0;

    if (grouped_by_join_key) {
        auto proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> proj_exprs;
        proj_exprs.reserve(key_count + agg.expressions.size());
        for (idx_t i = 0; i < key_count; i++) {
            auto group_expr = make_uniq<BoundColumnRefExpression>(native_join->types[i], native_join_bindings[i]);
            proj_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(group_expr),
                                                                    GetRewriteGroupType(col, agg, i)));
        }
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            auto result_type = info.result_type;

            unique_ptr<Expression> final_expr;
            if (info.fn == "COUNT_STAR") {
                auto left = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, probe_count_binding);
                auto right = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, build_count_binding);
                auto cast_left = BoundCastExpression::AddCastToType(context, std::move(left), result_type);
                auto cast_right = BoundCastExpression::AddCastToType(context, std::move(right), result_type);
                final_expr = optimizer.BindScalarFunction("*", std::move(cast_left), std::move(cast_right));
            } else if (info.fn == "AVG") {
                auto sum_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.sum_slot),
                                                                   side_slot_binding(info, info.sum_slot));
                auto count_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.count_slot),
                                                                     side_slot_binding(info, info.count_slot));
                auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(sum_ref), result_type);
                auto cast_count = BoundCastExpression::AddCastToType(context, std::move(count_ref), result_type);
                final_expr = optimizer.BindScalarFunction("/", std::move(cast_sum), std::move(cast_count));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                final_expr = BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type);
            } else {
                idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                auto multiplier = other_count_ref(info);
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                auto cast_side = BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type);
                final_expr = optimizer.BindScalarFunction("*", std::move(multiplier), std::move(cast_side));
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
        output_base = key_count;
        replacement = std::move(proj);
    } else {
        auto contrib_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> contrib_exprs;
        if (grouped_by_join_key_subset) {
            for (idx_t g = 0; g < group_key_positions.size(); g++) {
                auto key_pos = group_key_positions[g];
                auto group_ref = make_uniq<BoundColumnRefExpression>(native_join->types[key_pos], native_join_bindings[key_pos]);
                contrib_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(group_ref), agg.types[g]));
            }
        }
        idx_t contrib_value_base = contrib_exprs.size();
        vector<idx_t> sum_slots(mixed_aggs.size(), DConstants::INVALID_INDEX);
        vector<idx_t> count_slots(mixed_aggs.size(), DConstants::INVALID_INDEX);
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            auto result_type = info.result_type;
            if (info.fn == "COUNT_STAR") {
                auto left = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, probe_count_binding);
                auto right = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, build_count_binding);
                auto cast_left = BoundCastExpression::AddCastToType(context, std::move(left), result_type);
                auto cast_right = BoundCastExpression::AddCastToType(context, std::move(right), result_type);
                sum_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(cast_left), std::move(cast_right)));
            } else if (info.fn == "AVG") {
                auto mul_num = other_count_ref(info);
                auto sum_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.sum_slot),
                                                                   side_slot_binding(info, info.sum_slot));
                auto cast_sum = BoundCastExpression::AddCastToType(context, std::move(sum_ref), result_type);
                sum_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul_num), std::move(cast_sum)));

                auto mul_den = other_count_ref(info);
                auto count_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, info.count_slot),
                                                                     side_slot_binding(info, info.count_slot));
                auto cast_count = BoundCastExpression::AddCastToType(context, std::move(count_ref), result_type);
                count_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul_den), std::move(cast_count)));
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                idx_t slot = info.fn == "MIN" ? info.min_slot : info.max_slot;
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                sum_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type));
            } else {
                idx_t slot = info.fn == "SUM" ? info.sum_slot : info.count_slot;
                auto mul = other_count_ref(info);
                auto side_ref = make_uniq<BoundColumnRefExpression>(side_slot_type(info, slot),
                                                                    side_slot_binding(info, slot));
                auto cast_side = BoundCastExpression::AddCastToType(context, std::move(side_ref), result_type);
                sum_slots[a] = contrib_exprs.size() - contrib_value_base;
                contrib_exprs.push_back(optimizer.BindScalarFunction("*", std::move(mul), std::move(cast_side)));
            }
        }
        auto contrib_proj = make_uniq<LogicalProjection>(contrib_index, std::move(contrib_exprs));
        contrib_proj->children.push_back(std::move(native_join));
        contrib_proj->ResolveOperatorTypes();

        auto final_agg_index = optimizer.binder.GenerateTableIndex();
        idx_t final_group_index = grouped_by_join_key_subset ? optimizer.binder.GenerateTableIndex() : DConstants::INVALID_INDEX;
        vector<unique_ptr<Expression>> final_aggs;
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            if (info.fn == "AVG") {
                for (auto slot : {sum_slots[a], count_slots[a]}) {
                    vector<unique_ptr<Expression>> children;
                    children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[contrib_value_base + slot],
                                                                           ColumnBinding(contrib_index, contrib_value_base + slot)));
                    final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
                }
            } else if (info.fn == "MIN" || info.fn == "MAX") {
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[contrib_value_base + sum_slots[a]],
                                                                       ColumnBinding(contrib_index, contrib_value_base + sum_slots[a])));
                final_aggs.push_back(BindAggregateByName(context, StringUtil::Lower(info.fn), std::move(children)));
            } else {
                vector<unique_ptr<Expression>> children;
                children.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[contrib_value_base + sum_slots[a]],
                                                                       ColumnBinding(contrib_index, contrib_value_base + sum_slots[a])));
                final_aggs.push_back(BindAggregateByName(context, "sum", std::move(children)));
            }
        }
        auto final_agg = make_uniq<LogicalAggregate>(final_group_index, final_agg_index, std::move(final_aggs));
        if (grouped_by_join_key_subset) {
            for (idx_t g = 0; g < group_key_positions.size(); g++) {
                final_agg->groups.push_back(make_uniq<BoundColumnRefExpression>(contrib_proj->types[g], ColumnBinding(contrib_index, g)));
            }
        }
        final_agg->children.push_back(std::move(contrib_proj));
        final_agg->ResolveOperatorTypes();

        auto final_bindings = final_agg->GetColumnBindings();
        auto final_proj_index = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> final_exprs;
        final_exprs.reserve((grouped_by_join_key_subset ? group_key_positions.size() : 0) + mixed_aggs.size());
        if (grouped_by_join_key_subset) {
            for (idx_t g = 0; g < group_key_positions.size(); g++) {
                auto group_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[g], final_bindings[g]);
                final_exprs.push_back(BoundCastExpression::AddCastToType(context, std::move(group_ref),
                                                                        GetRewriteGroupType(col, agg, g)));
            }
        }
        idx_t final_agg_base = grouped_by_join_key_subset ? group_key_positions.size() : 0;
        for (idx_t a = 0; a < mixed_aggs.size(); a++) {
            auto &info = mixed_aggs[a];
            unique_ptr<Expression> final_expr;
            if (info.fn == "AVG") {
                auto num_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[final_agg_base + sum_slots[a]],
                                                                   final_bindings[final_agg_base + sum_slots[a]]);
                auto den_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[final_agg_base + count_slots[a]],
                                                                   final_bindings[final_agg_base + count_slots[a]]);
                auto cast_num = BoundCastExpression::AddCastToType(context, std::move(num_ref), info.result_type);
                auto cast_den = BoundCastExpression::AddCastToType(context, std::move(den_ref), info.result_type);
                final_expr = optimizer.BindScalarFunction("/", std::move(cast_num), std::move(cast_den));
            } else {
                auto sum_ref = make_uniq<BoundColumnRefExpression>(final_agg->types[final_agg_base + sum_slots[a]],
                                                                   final_bindings[final_agg_base + sum_slots[a]]);
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
        output_base = grouped_by_join_key_subset ? group_key_positions.size() : 0;
        replacement = std::move(final_proj);
    }

    if (has_parent) {
        for (idx_t a = 0; a < agg.expressions.size(); a++) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.aggregate_index, a),
                                                    ColumnBinding(output_index, output_base + a));
        }
        if (grouped_by_join_key || grouped_by_join_key_subset) {
            auto group_count = grouped_by_join_key ? key_count : group_key_positions.size();
            for (idx_t i = 0; i < group_count; i++) {
                state.replacement_bindings.emplace_back(ColumnBinding(agg.group_index, i),
                                                        ColumnBinding(output_index, i));
            }
        }
    }
    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] planner rewrite: native mixed-side preagg (join_conds=%zu, groups=%zu, aggs=%zu, probe_est=%llu, build_est=%llu, group_est=%llu)\n",
                join.conditions.size(), agg.groups.size(), agg.expressions.size(), (unsigned long long)probe_est,
                (unsigned long long)build_est, (unsigned long long)group_est);
    }
    op = std::move(replacement);
    return true;
}


} // namespace duckdb
