#include "aggjoin_optimizer_shared.hpp"
#include "aggjoin_rewrites_internal.hpp"
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
// Optimizer: post-optimize with Projection chain tracing
// ============================================================

static bool IsEquiJoin(LogicalOperator &op) {
    if(op.type!=LogicalOperatorType::LOGICAL_COMPARISON_JOIN) return false;
    auto &j=op.Cast<LogicalComparisonJoin>();
    if(j.join_type!=JoinType::INNER) return false;
    for(auto &c:j.conditions) if(c.comparison!=ExpressionType::COMPARE_EQUAL) return false;
    return !j.conditions.empty();
}

static bool IsAggregate(LogicalOperator &op) {
    if(op.type!=LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) return false;
    auto &a=op.Cast<LogicalAggregate>();
    
    // Ungrouped aggregates (no GROUP BY) are supported — single result row
    for(auto &e:a.expressions) {
        if(e->type!=ExpressionType::BOUND_AGGREGATE) { return false; }
        auto &ba = e->Cast<BoundAggregateExpression>();
        auto fn=StringUtil::Upper(ba.function.name);
        if(fn!="SUM"&&fn!="MIN"&&fn!="MAX"&&fn!="COUNT"&&fn!="COUNT_STAR"&&fn!="AVG") return false;
        // SUM/AVG still require the current numeric fast path. MIN/MAX can be
        // admitted here and later lowered back to native if they stay on the
        // old Value-heavy execution path.
        if (fn != "COUNT" && fn != "COUNT_STAR" && fn != "MIN" && fn != "MAX") {
            auto &ret_type = ba.return_type;
            auto phys = ret_type.InternalType();
            if (phys != PhysicalType::DOUBLE && phys != PhysicalType::FLOAT &&
                phys != PhysicalType::INT32 && phys != PhysicalType::INT64 &&
                phys != PhysicalType::INT16 && phys != PhysicalType::INT8) {
                return false; // HUGEINT, DECIMAL, VARCHAR, DATE — can't beat native
            }
        }
        if ((fn == "SUM" || fn == "AVG") &&
            !ba.children.empty() && !ba.children[0]->return_type.IsNumeric()) {
            return false; // Non-numeric aggregate input — bail to native
        }
    }
    return true;
}

// Find Join through Projection chain, return the Join and the Aggregate's child
static LogicalComparisonJoin *FindJoin(LogicalOperator &op) {
    if(IsEquiJoin(op)) return &op.Cast<LogicalComparisonJoin>();
    if(op.type==LogicalOperatorType::LOGICAL_PROJECTION && op.children.size()==1)
        return FindJoin(*op.children[0]);
    return nullptr;
}

void WalkAndReplace(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                    AggJoinRewriteState &state, bool has_parent) {
    for (auto &c : op->children) {
        WalkAndReplace(context, optimizer, c, state, true);
    }
    if(!IsAggregate(*op)||op->children.size()!=1) return;
    auto *join = FindJoin(*op->children[0]);
    if(!join) { return; }
    

    auto &agg = op->Cast<LogicalAggregate>();
    auto &agg_child = *op->children[0]; // Projection chain above Join

    if (TryRewriteNativeFinalBagPreagg(context, optimizer, op, agg, *join, agg_child, state, has_parent)) {
        return;
    }

    // ── Planner gating: bail on shapes where AGGJOIN is unlikely to beat native ──
    // Disable at compile time with -DAGGJOIN_NO_PLANNER_GATE for testing/benchmarking.
    // When disabled, logs what WOULD have been gated for accuracy analysis.
    {
        const char *gate_reason = nullptr;
        if (join->conditions.size() >= 4) gate_reason = "4+ join conditions";
        else if (agg.expressions.empty()) gate_reason = "no aggregate functions";
        else if (agg.groups.size() > 4) gate_reason = "4+ GROUP BY columns";
        if (gate_reason) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr, "[AGGJOIN] planner gate would bail: %s (join_conds=%zu, aggs=%zu, groups=%zu)\n",
                        gate_reason, join->conditions.size(), agg.expressions.size(), agg.groups.size());
            }
#ifndef AGGJOIN_NO_PLANNER_GATE
            return;
#else
            fprintf(stderr, "[AGGJOIN] planner gate would bail: %s (join_conds=%zu, aggs=%zu, groups=%zu)\n",
                    gate_reason, join->conditions.size(), agg.expressions.size(), agg.groups.size());
#endif
        }
    }

    // Trace each aggregate group/input index through Projection chain → Join output index
    // The Join's output is PRUNED — it only includes columns actually needed.
    // We need to map Join output positions to actual scan column indices.
    auto join_bindings = join->GetColumnBindings();
    auto probe_bindings = join->children[0]->GetColumnBindings();
    auto build_bindings = join->children[1]->GetColumnBindings();
    auto probe_cols = join->children[0]->types.size();

    // Map a join output position to (is_probe, scan_col_index)
    auto resolveJoinCol = [&](idx_t join_pos) -> std::pair<bool, idx_t> {
        if (join_pos >= join_bindings.size()) return {false, DConstants::INVALID_INDEX};
        auto &b = join_bindings[join_pos];
        for (idx_t i = 0; i < probe_bindings.size(); i++) {
            if (probe_bindings[i] == b) return {true, i}; // Probe side, scan index i
        }
        for (idx_t i = 0; i < build_bindings.size(); i++) {
            if (build_bindings[i] == b) return {false, i}; // Build side, scan index i
        }
        return {false, DConstants::INVALID_INDEX};
    };

    AggJoinColInfo col;
    col.probe_col_count = probe_cols;

    // Resolve BoundColumnRefExpression to physical position in the child's output.
    // At post-optimize, group/aggregate expressions are BoundColumnRefExpression
    // with (table_index, column_index). We need to find the POSITION in the child
    // operator's GetColumnBindings() that matches this binding.
    auto child_bindings = agg_child.GetColumnBindings();
    auto resolveBinding = [&](Expression &e) -> idx_t {
        if (e.GetExpressionClass() == ExpressionClass::BOUND_REF) {
            return e.Cast<BoundReferenceExpression>().index;
        }
        if (e.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
            auto &binding = e.Cast<BoundColumnRefExpression>().binding;
            for (idx_t i = 0; i < child_bindings.size(); i++) {
                if (child_bindings[i] == binding) return i;
            }
        }
        return DConstants::INVALID_INDEX;
    };

    // First pass: resolve all group and agg columns to (is_probe, scan_idx) pairs.
    // Then determine if we need to swap children.
    struct ResolvedCol { bool is_probe; idx_t scan_idx; CompressInfo compress; };
    vector<ResolvedCol> resolved_groups, resolved_aggs;

    for(idx_t gi = 0; gi < agg.groups.size(); gi++) {
        auto &g = agg.groups[gi];
        auto agg_idx = resolveBinding(*g);
        if (agg_idx == DConstants::INVALID_INDEX) { return; }
        auto ci = FindCompressInChain(agg_child, agg_idx);
        auto join_idx = TraceProjectionChain(agg_child, agg_idx);
        if(join_idx==DConstants::INVALID_INDEX) return;
        if (join_idx >= join_bindings.size()) return;
        auto &b = join_bindings[join_idx];
        bool is_p = false; idx_t si = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < probe_bindings.size(); i++) {
            if (probe_bindings[i] == b) { is_p = true; si = i; break; }
        }
        if (!is_p) {
            for (idx_t i = 0; i < build_bindings.size(); i++) {
                if (build_bindings[i] == b) { si = i; break; }
            }
        }
        if (si == DConstants::INVALID_INDEX) { return; }
        resolved_groups.push_back({is_p, si, ci});
       
    }

    for(auto &e : agg.expressions) {
        auto &ba = e->Cast<BoundAggregateExpression>();
        auto fn = StringUtil::Upper(ba.function.name);
        // Normalize COUNT_STAR → COUNT for uniform handling
        if (fn == "COUNT_STAR") fn = "COUNT";
        col.agg_funcs.push_back(fn);
        bool is_numeric = true;
        if (!ba.children.empty() && !ba.children[0]->return_type.IsNumeric()) {
            is_numeric = false; // VARCHAR, DATE, etc.
        }
        col.agg_is_numeric.push_back(is_numeric);
        idx_t agg_child_idx = DConstants::INVALID_INDEX;
        if (!ba.children.empty()) {
            agg_child_idx = resolveBinding(*ba.children[0]);
        }
        if (agg_child_idx == DConstants::INVALID_INDEX) {
            resolved_aggs.push_back({true, DConstants::INVALID_INDEX, {}}); // COUNT(*)
            continue;
        }
        auto join_idx = TraceProjectionChain(agg_child, agg_child_idx);
        if (join_idx == DConstants::INVALID_INDEX || join_idx >= join_bindings.size()) {
            resolved_aggs.push_back({true, DConstants::INVALID_INDEX, {}});
            continue;
        }
        auto &b = join_bindings[join_idx];
        bool is_p = false; idx_t si = DConstants::INVALID_INDEX;
        for (idx_t i = 0; i < probe_bindings.size(); i++) {
            if (probe_bindings[i] == b) { is_p = true; si = i; break; }
        }
        if (!is_p) {
            for (idx_t i = 0; i < build_bindings.size(); i++) {
                if (build_bindings[i] == b) { si = i; break; }
            }
        }
        resolved_aggs.push_back({is_p, si, {}});
    }

    // Determine if ALL group columns are on the build side.
    // If so, swap probe/build so groups are accessible from ExecuteInternal.
    // Empty groups (ungrouped aggregate) → treat as "all on probe" (no swap needed)
    bool all_groups_build = !resolved_groups.empty();
    bool all_groups_probe = true; // Empty groups trivially satisfied
    for (auto &rg : resolved_groups) {
        if (rg.is_probe) all_groups_build = false;
        else all_groups_probe = false;
    }
    bool need_swap = all_groups_build;
    // With build-side aggregate access, we CAN swap even when agg inputs are on probe
    // (they become build-side after swap, handled via build-side accumulation in Sink).
    // Bail if groups span both sides.
    if (!all_groups_probe && !all_groups_build) { return; }

    if (need_swap) {
        // Swap bindings and probe_cols
        std::swap(probe_bindings, build_bindings);
        probe_cols = probe_bindings.size(); // Not types.size() — use binding count
        col.probe_col_count = probe_cols;
        // Flip is_probe for all resolved columns
        for (auto &rg : resolved_groups) rg.is_probe = true;
        for (auto &ra : resolved_aggs) {
            if (ra.scan_idx != DConstants::INVALID_INDEX) ra.is_probe = !ra.is_probe;
        }
    }

    // Bail if any group is still on build side after potential swap
    for (idx_t i = 0; i < resolved_groups.size(); i++) {
        auto &rg = resolved_groups[i];
        if (!rg.is_probe || rg.scan_idx == DConstants::INVALID_INDEX) { return; }
    }
    // Populate col from resolved columns.
    // Build-side aggregate inputs are now supported — accumulated during Sink.
    for (auto &rg : resolved_groups) {
        col.group_compress.push_back(rg.compress);
        col.group_cols.push_back(rg.scan_idx);
    }
    // Count build-side aggregates for build-side storage sizing.
    idx_t build_agg_count = 0;
    for (auto &ra : resolved_aggs) {
        if (ra.scan_idx != DConstants::INVALID_INDEX && !ra.is_probe) build_agg_count++;
    }

    bool has_nonnumeric_minmax = false;
    {
        idx_t ra_idx = 0;
        for (auto &e : agg.expressions) {
            auto &ba = e->Cast<BoundAggregateExpression>();
            auto fn = StringUtil::Upper(ba.function.name);
            if (ra_idx < resolved_aggs.size()) {
                auto &ra = resolved_aggs[ra_idx];
                if (ra.scan_idx != DConstants::INVALID_INDEX && (fn == "MIN" || fn == "MAX")) {
                    if (!ba.children.empty() && !ba.children[0]->return_type.IsNumeric()) {
                        has_nonnumeric_minmax = true;
                        break;
                    }
                }
            }
            ra_idx++;
        }
    }

    for (auto &ra : resolved_aggs) {
        bool on_build = (ra.scan_idx != DConstants::INVALID_INDEX && !ra.is_probe);
        col.agg_on_build.push_back(on_build);
        if (ra.scan_idx == DConstants::INVALID_INDEX) {
            col.agg_input_cols.push_back(DConstants::INVALID_INDEX);
            col.build_agg_cols.push_back(DConstants::INVALID_INDEX);
        } else if (on_build) {
            col.agg_input_cols.push_back(DConstants::INVALID_INDEX); // Not in probe scan
            col.build_agg_cols.push_back(ra.scan_idx); // Build-side scan index
        } else {
            col.agg_input_cols.push_back(ra.scan_idx);
            col.build_agg_cols.push_back(DConstants::INVALID_INDEX);
        }
    }

    // Extract join key column indices from join conditions.
    // Left = original probe child, Right = original build child.
    std::function<idx_t(Expression &)> getIdx = [&](Expression &e) -> idx_t {
        if (e.GetExpressionClass() == ExpressionClass::BOUND_REF)
            return e.Cast<BoundReferenceExpression>().index;
        if (e.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF)
            return e.Cast<BoundColumnRefExpression>().binding.column_index;
        if (e.GetExpressionClass() == ExpressionClass::BOUND_CAST)
            return getIdx(*e.Cast<BoundCastExpression>().child);
        if (e.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            for (auto &child : e.Cast<BoundFunctionExpression>().children) {
                auto r = getIdx(*child);
                if (r != DConstants::INVALID_INDEX) return r;
            }
        }
        return DConstants::INVALID_INDEX;
    };
    for(auto &cond : join->conditions) {
        auto li = getIdx(*cond.left), ri = getIdx(*cond.right);
        if (need_swap) {
            // After swap: original left (probe) is now build, right is now probe
            if (ri != DConstants::INVALID_INDEX) col.probe_key_cols.push_back(ri);
            if (li != DConstants::INVALID_INDEX) col.build_key_cols.push_back(li);
        } else {
            if (li != DConstants::INVALID_INDEX) col.probe_key_cols.push_back(li);
            if (ri != DConstants::INVALID_INDEX) col.build_key_cols.push_back(ri);
        }
    }

    // Bail if key columns couldn't be extracted
    if (col.probe_key_cols.empty() || col.build_key_cols.empty()) return;
    if (col.probe_key_cols.size() != col.build_key_cols.size()) return;

    if (TryRewriteNativeMixedSidePreagg(context, optimizer, op, agg, *join, agg_child, col, need_swap,
                                        state, has_parent)) {
        return;
    }

    if (TryRewriteNativeBuildPreagg(context, optimizer, op, agg, *join, agg_child, col, build_agg_count, need_swap,
                                    state, has_parent)) {
        return;
    }

    if (has_nonnumeric_minmax) {
        return;
    }

    // ── Benchmark-informed planner gate: avoid weak key shapes ──
    {
        auto &probe_types = need_swap ? join->children[1]->types : join->children[0]->types;
        auto &build_types = need_swap ? join->children[0]->types : join->children[1]->types;
        auto get_est = [](LogicalOperator &node) -> idx_t {
            return node.has_estimated_cardinality ? node.estimated_cardinality : 0;
        };
        auto probe_est = get_est(*(need_swap ? join->children[1] : join->children[0]));
        auto build_est = get_est(*(need_swap ? join->children[0] : join->children[1]));
        auto group_est = agg.has_estimated_cardinality ? agg.estimated_cardinality : 0;
        col.probe_estimate = probe_est;
        col.build_estimate = build_est;
        col.group_estimate = group_est;
        auto is_varlen_key_type = [&](const LogicalType &type) {
            auto id = type.id();
            return id == LogicalTypeId::VARCHAR || id == LogicalTypeId::BLOB;
        };
        bool has_varlen_key = false;
        bool has_non_integral_key = false;
        for (auto idx : col.probe_key_cols) {
            if (idx >= probe_types.size()) continue;
            has_varlen_key |= is_varlen_key_type(probe_types[idx]);
            has_non_integral_key |= !probe_types[idx].IsIntegral();
        }
        for (auto idx : col.build_key_cols) {
            if (idx >= build_types.size()) continue;
            has_varlen_key |= is_varlen_key_type(build_types[idx]);
            has_non_integral_key |= !build_types[idx].IsIntegral();
        }
        for (auto idx : col.group_cols) {
            if (idx >= probe_types.size()) continue;
            has_varlen_key |= is_varlen_key_type(probe_types[idx]);
            has_non_integral_key |= !probe_types[idx].IsIntegral();
        }
        bool composite_shape = (join->conditions.size() > 1 || agg.groups.size() > 1);
        bool group_matches_join_key = (col.group_cols.size() == col.probe_key_cols.size());
        if (group_matches_join_key) {
            for (idx_t i = 0; i < col.group_cols.size(); i++) {
                if (col.group_cols[i] != col.probe_key_cols[i]) {
                    group_matches_join_key = false;
                    break;
                }
            }
        }
        bool has_count_or_avg = false;
        for (auto &fn : col.agg_funcs) {
            if (fn == "COUNT" || fn == "AVG") {
                has_count_or_avg = true;
                break;
            }
        }
        bool direct_like_shape = !has_varlen_key && !has_non_integral_key &&
                                 col.probe_key_cols.size() == 1 &&
                                 (col.group_cols.empty() || group_matches_join_key);
        bool large_inputs = probe_est >= 100000 && build_est >= 100000;
        bool huge_inputs = probe_est >= 500000 && build_est >= 500000;
        bool group_known = group_est > 0;
        bool low_probe_fanout = group_known && probe_est <= group_est * 2;
        bool low_build_fanout = group_known && build_est <= group_est * 2;
        bool low_fanout_shape = low_probe_fanout && low_build_fanout;
        bool has_build_aggs = build_agg_count > 0;
        bool simple_varlen_hash_shape = false;
        if (has_varlen_key && !composite_shape && !has_build_aggs && col.probe_key_cols.size() == 1 &&
            (col.group_cols.empty() || group_matches_join_key)) {
            simple_varlen_hash_shape = true;
            for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
                auto &fn = col.agg_funcs[a];
                bool numeric_ok = (a >= col.agg_is_numeric.size() || col.agg_is_numeric[a]);
                if (fn == "COUNT") continue;
                if ((fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX") && numeric_ok) continue;
                simple_varlen_hash_shape = false;
                break;
            }
            if (simple_varlen_hash_shape && !col.group_cols.empty() && group_known &&
                probe_est >= 100000 && group_est >= probe_est / 4) {
                simple_varlen_hash_shape = false;
            }
        }
        bool build_rollup = has_build_aggs && !group_matches_join_key;
        bool heavy_build_aggs = build_agg_count >= 3;
        bool asym_build_heavy = has_build_aggs && group_matches_join_key &&
                                build_est >= probe_est * 8 && build_est >= 1000000;
        if (AggJoinTraceEnabled() && has_build_aggs) {
            fprintf(stderr,
                    "[AGGJOIN] gate build-agg shape: probe_est=%llu build_est=%llu group_est=%llu group_matches_join_key=%d build_agg_count=%llu asym_build_heavy=%d\n",
                    (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est,
                    group_matches_join_key ? 1 : 0, (unsigned long long)build_agg_count, asym_build_heavy ? 1 : 0);
        }
        bool composite_rollup = join->conditions.size() > 1 && !group_matches_join_key;
        bool native_ht_friendly = composite_shape && !has_varlen_key && !has_count_or_avg &&
                                  build_agg_count == 0 && join->conditions.size() <= 2;
        const char *gate_reason = nullptr;
        if (has_varlen_key && !simple_varlen_hash_shape) gate_reason = "variable-width join/group key";
        else if (asym_build_heavy)
            gate_reason = "build-heavy aggregate shape better handled natively";
        else if (build_rollup && large_inputs && (build_agg_count >= 2 || group_est == 0 || group_est >= 256))
            gate_reason = "build-side rollup outside fast path";
        else if (has_build_aggs && heavy_build_aggs && large_inputs && !direct_like_shape)
            gate_reason = "build-side aggregate fanout outside direct path";
        else if (has_build_aggs && large_inputs && group_matches_join_key &&
                 build_agg_count >= 4 && has_count_or_avg)
            gate_reason = "build-side aggregate mix outside direct fast path";
        else if (!has_build_aggs && !composite_shape && has_non_integral_key &&
                 !simple_varlen_hash_shape &&
                 group_matches_join_key && large_inputs)
            gate_reason = "non-integral single-key shape outside direct path";
        else if (!has_build_aggs && !has_count_or_avg && !composite_shape &&
                 large_inputs && low_fanout_shape)
            gate_reason = "low estimated fanout";
        else if (!direct_like_shape && composite_shape && has_non_integral_key && large_inputs)
            gate_reason = "non-integral composite key";
        else if (!direct_like_shape && composite_rollup && huge_inputs && (group_est == 0 || group_est >= 512))
            gate_reason = "estimated expensive composite rollup";
        else if (!direct_like_shape && composite_shape && !native_ht_friendly && large_inputs)
            gate_reason = "composite shape outside native-ht fast path";
        if (gate_reason) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr,
                        "[AGGJOIN] planner cost gate would bail: %s (join_conds=%zu, groups=%zu, build_aggs=%zu, probe_est=%llu, build_est=%llu, group_est=%llu)\n",
                        gate_reason, join->conditions.size(), agg.groups.size(), build_agg_count,
                        (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est);
            }
#ifndef AGGJOIN_NO_PLANNER_GATE
            return;
#else
            fprintf(stderr,
                    "[AGGJOIN] planner cost gate would bail: %s (join_conds=%zu, groups=%zu, build_aggs=%zu, probe_est=%llu, build_est=%llu, group_est=%llu)\n",
                    gate_reason, join->conditions.size(), agg.groups.size(), build_agg_count,
                    (unsigned long long)probe_est, (unsigned long long)build_est, (unsigned long long)group_est);
#endif
        }
    }

    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] planner fired: join_conds=%zu groups=%zu aggs=%zu build_aggs=%zu need_swap=%d\n",
                join->conditions.size(), agg.groups.size(), agg.expressions.size(), build_agg_count,
                need_swap ? 1 : 0);
    }

    // Create LogicalAggJoin
    auto aj = make_uniq<LogicalAggJoin>();
    // Build return types: group columns use compressed types (if compression exists)
    // so that the decompress Projection above us can correctly restore original types.
    // Aggregate columns keep their original types (SUM output is DOUBLE/BIGINT, not compressed).
    {
        vector<LogicalType> ret_types;
        idx_t ng = col.group_compress.size();
        for (idx_t g = 0; g < ng; g++) {
            if (col.group_compress[g].has_compress && !col.group_compress[g].is_string_compress) {
                ret_types.push_back(col.group_compress[g].compressed_type);
            } else if (col.group_compress[g].has_compress && col.group_compress[g].is_string_compress) {
                ret_types.push_back(col.group_compress[g].original_type);
            } else {
                ret_types.push_back(agg.types[g]);
            }
        }
        // Aggregate output types (unchanged)
        for (idx_t a = ng; a < agg.types.size(); a++) {
            ret_types.push_back(agg.types[a]);
        }
        aj->return_types = std::move(ret_types);
    }
    aj->estimated_cardinality = agg.estimated_cardinality;
    aj->group_index = agg.group_index;
    aj->aggregate_index = agg.aggregate_index;
    aj->col = std::move(col);
    // Store expressions for native HT creation
    for (auto &e : agg.expressions) {
        aj->agg_expressions.push_back(e->Copy());
    }
    for (auto &g : agg.groups) {
        aj->group_expressions.push_back(g->Copy());
    }
    // Take Join's children — swap if needed so probe side has GROUP BY columns
    if (need_swap) {
        aj->children.push_back(std::move(join->children[1])); // old build → new probe
        aj->children.push_back(std::move(join->children[0])); // old probe → new build
    } else {
        aj->children = std::move(join->children);
    }


    op = std::move(aj);
}

// Second pass: strip string-decompress projections above AggJoin.
// Narrow VARCHAR support keeps raw strings inside AggJoin, so a parent
// __internal_decompress_string(#i) must become a passthrough reference.
// Integral decompress projections remain untouched because AggJoin still emits
// compressed integral keys for that path.
void StripDecompressProjections(unique_ptr<LogicalOperator> &op) {
    for (auto &child : op->children) {
        StripDecompressProjections(child);
    }

    if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) return;
    if (op->children.size() != 1) return;
    auto &proj = op->Cast<LogicalProjection>();
    bool child_is_aggjoin = op->children[0]->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
    bool has_string_decompress = false;
    for (auto &expr : proj.expressions) {
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            auto &func = expr->Cast<BoundFunctionExpression>();
            if (func.function.name.find("decompress_string") != string::npos) {
                if (child_is_aggjoin) {
                    has_string_decompress = true;
                    continue;
                }
                idx_t ref_idx = DConstants::INVALID_INDEX;
                for (auto &child : func.children) {
                    if (child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
                        ref_idx = child->Cast<BoundReferenceExpression>().index;
                        break;
                    }
                    if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                        ref_idx = child->Cast<BoundColumnRefExpression>().binding.column_index;
                        break;
                    }
                }
                if (ref_idx != DConstants::INVALID_INDEX && ref_idx < op->children[0]->types.size() &&
                    op->children[0]->types[ref_idx] == func.return_type) {
                    has_string_decompress = true;
                }
            }
        }
    }

    if (!has_string_decompress) return;

    for (idx_t i = 0; i < proj.expressions.size(); i++) {
        auto &expr = proj.expressions[i];
        if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            auto &func = expr->Cast<BoundFunctionExpression>();
            if (func.function.name.find("decompress_string") != string::npos) {
                if (child_is_aggjoin) {
                    for (auto &child : func.children) {
                        if (child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
                            auto ref_idx = child->Cast<BoundReferenceExpression>().index;
                            proj.expressions[i] = make_uniq<BoundReferenceExpression>(func.return_type, ref_idx);
                            break;
                        }
                        if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                            auto &binding = child->Cast<BoundColumnRefExpression>().binding;
                            proj.expressions[i] = make_uniq<BoundReferenceExpression>(func.return_type, binding.column_index);
                            break;
                        }
                    }
                    continue;
                }
                idx_t ref_idx = DConstants::INVALID_INDEX;
                for (auto &child : func.children) {
                    if (child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
                        ref_idx = child->Cast<BoundReferenceExpression>().index;
                        break;
                    }
                    if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                        ref_idx = child->Cast<BoundColumnRefExpression>().binding.column_index;
                        break;
                    }
                }
                if (ref_idx != DConstants::INVALID_INDEX && ref_idx < op->children[0]->types.size() &&
                    op->children[0]->types[ref_idx] == func.return_type) {
                    proj.expressions[i] = make_uniq<BoundReferenceExpression>(func.return_type, ref_idx);
                }
            }
        }
    }

    proj.types.clear();
    for (auto &expr : proj.expressions) {
        proj.types.push_back(expr->return_type);
    }
}


} // namespace duckdb
