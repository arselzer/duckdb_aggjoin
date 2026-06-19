#include "aggjoin_optimizer_shared.hpp"
#include "aggjoin_rewrites_internal.hpp"
#include "aggjoin_runtime.hpp"
#include "aggjoin_stats.hpp"
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

// IsBareColumnKey lives in aggjoin_rewrites_internal.hpp (shared with the
// aggregate-propagation rewrite). (Review bug B, 2026-06-15.)

static bool IsEquiJoin(LogicalOperator &op) {
    if(op.type!=LogicalOperatorType::LOGICAL_COMPARISON_JOIN) return false;
    auto &j=op.Cast<LogicalComparisonJoin>();
    if(j.join_type!=JoinType::INNER) return false;
    for(auto &c:j.conditions) {
        if(c.comparison!=ExpressionType::COMPARE_EQUAL) return false;
        // Both sides must be bare columns — reject function/arithmetic-wrapped
        // keys (LOWER(x)=LOWER(y), x+1=y) which the runtime would otherwise join
        // on the raw column, silently dropping the function. (Review bug B.)
        if(!IsBareColumnKey(*c.left) || !IsBareColumnKey(*c.right)) return false;
    }
    return !j.conditions.empty();
}

static bool IsInnerEqualityJoin(LogicalOperator &op) {
    if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return false;
    }
    auto &j = op.Cast<LogicalComparisonJoin>();
    if (j.join_type != JoinType::INNER) {
        return false;
    }
    for (auto &c : j.conditions) {
        if (c.comparison != ExpressionType::COMPARE_EQUAL) {
            return false;
        }
    }
    return !j.conditions.empty();
}

static bool ExtractBinding(Expression &expr, ColumnBinding &binding) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
        binding = expr.Cast<BoundColumnRefExpression>().binding;
        return true;
    }
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
        return ExtractBinding(*expr.Cast<BoundCastExpression>().child, binding);
    }
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
        for (auto &child : expr.Cast<BoundFunctionExpression>().children) {
            if (ExtractBinding(*child, binding)) {
                return true;
            }
        }
    }
    return false;
}

static bool IsDirectPlannableInteger(PhysicalType type) {
    switch (type) {
    case PhysicalType::INT8:
    case PhysicalType::INT16:
    case PhysicalType::INT32:
    case PhysicalType::INT64:
    case PhysicalType::UINT8:
    case PhysicalType::UINT16:
    case PhysicalType::UINT32:
        return true;
    default:
        return false;
    }
}

static bool IsDoubleExactMinMaxPayload(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
        return true;
    default:
        return false;
    }
}

static bool IsBuildAgg(const AggJoinColInfo &col, idx_t agg_idx) {
    return col.agg_on_build.size() > agg_idx && col.agg_on_build[agg_idx];
}

static idx_t DirectBuildAggBytesPerKey(const AggJoinColInfo &col) {
    idx_t sum_slots = 0;
    idx_t count_slots = 0;
    idx_t min_slots = 0;
    idx_t max_slots = 0;
    idx_t has_slots = 0;
    for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
        if (!IsBuildAgg(col, a)) {
            continue;
        }
        auto &f = col.agg_funcs[a];
        if (f == "SUM") {
            sum_slots++;
            has_slots++;
        } else if (f == "AVG") {
            sum_slots++;
            count_slots++;
        } else if (f == "COUNT") {
            count_slots++;
        } else if (f == "MIN") {
            min_slots++;
            has_slots++;
        } else if (f == "MAX") {
            max_slots++;
            has_slots++;
        }
    }
    return sizeof(double) * (sum_slots + count_slots + min_slots + max_slots) +
           sizeof(uint8_t) * has_slots;
}

static idx_t DirectAggBytesPerKey(const AggJoinColInfo &col) {
    idx_t accum_slots = 0;
    idx_t avg_slots = 0;
    idx_t min_slots = 0;
    idx_t max_slots = 0;
    idx_t has_slots = 0;
    for (auto &f : col.agg_funcs) {
        if (f == "SUM") {
            accum_slots++;
            has_slots++;
        } else if (f == "AVG") {
            accum_slots++;
            avg_slots++;
        } else if (f == "COUNT") {
            accum_slots++;
        } else if (f == "MIN") {
            min_slots++;
            has_slots++;
        } else if (f == "MAX") {
            max_slots++;
            has_slots++;
        }
    }
    return sizeof(double) * (accum_slots + avg_slots + min_slots + max_slots) +
           sizeof(uint8_t) * has_slots;
}

static bool IsAggregate(LogicalOperator &op) {
    if(op.type!=LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) return false;
    auto &a=op.Cast<LogicalAggregate>();

    // ROLLUP / CUBE / GROUPING SETS lower to a single LogicalAggregate with
    // multiple grouping sets; the fused operator only computes the primary
    // grouping and would silently drop the other levels. GROUPING() functions
    // likewise have no fused equivalent. Bail to native execution.
    if (a.grouping_sets.size() > 1 || !a.grouping_functions.empty()) return false;

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

static LogicalComparisonJoin *FindInnerEqualityJoin(LogicalOperator &op) {
    if (IsInnerEqualityJoin(op)) {
        return &op.Cast<LogicalComparisonJoin>();
    }
    if (op.type == LogicalOperatorType::LOGICAL_PROJECTION && op.children.size() == 1) {
        return FindInnerEqualityJoin(*op.children[0]);
    }
    return nullptr;
}

// A correlated subquery is decorrelated into DELIM_JOIN / DELIM_GET (and, before
// decorrelation, DEPENDENT_JOIN) nodes: a DELIM_GET replays the OUTER query's
// tuples into the inner plan. An aggregate matched over such a subtree depends on
// that correlation, which neither the fused PhysicalAggJoin operator nor the
// native logical rewrites model -- firing computes the aggregate over the wrong (outer-
// independent) relation and silently drops the correlation (TPC-H q02 dropped all
// rows; q17 inflated ~100x because the subquery AVG was wrong, so the outer
// l_quantity < AVG filter passed everything). Decline every aggjoin rewrite when
// any such node is in the aggregate's input subtree.
static bool SubtreeHasDelim(LogicalOperator &op) {
    switch (op.type) {
    case LogicalOperatorType::LOGICAL_DELIM_JOIN:
    case LogicalOperatorType::LOGICAL_DELIM_GET:
    case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        return true;
    default:
        break;
    }
    for (auto &c : op.children) {
        if (SubtreeHasDelim(*c)) {
            return true;
        }
    }
    return false;
}

void WalkAndReplace(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                    AggJoinRewriteState &state, bool has_parent) {
    for (auto &c : op->children) {
        WalkAndReplace(context, optimizer, c, state, true);
    }

    // Correlated-subquery decorrelation guard (see SubtreeHasDelim): an aggregate
    // whose input subtree contains a DELIM/dependent join replays outer tuples that
    // none of the aggjoin rewrites model -- decline all of them for this aggregate.
    if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && op->children.size() == 1 &&
        SubtreeHasDelim(*op->children[0])) {
        return;
    }

    // The agg-propagation logical rewrite lowers entirely to native operators and has complete
    // internal gating, including an EXACT-HUGEINT path for integer SUM (whose
    // HUGEINT result the shared IsAggregate gate below excludes for the Value-heavy
    // operator paths). Try it FIRST, before that gate, so integer SUM is not
    // blocked; it bails cleanly on every shape it does not own.
    if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && op->children.size() == 1) {
        if (auto *cc_join = FindInnerEqualityJoin(*op->children[0])) {
            auto &cc_agg = op->Cast<LogicalAggregate>();
            auto &cc_child = *op->children[0];
            if (AggJoinLogicalRewritesEnabled() &&
                TryRewriteNativeAggPropagation(context, optimizer, op, cc_agg, *cc_join, cc_child, state, has_parent)) {
                return;
            }
        }
    }

    if(!IsAggregate(*op)||op->children.size()!=1) return;
    auto *join = FindJoin(*op->children[0]);
    if(!join) { return; }


    auto &agg = op->Cast<LogicalAggregate>();
    auto &agg_child = *op->children[0]; // Projection chain above Join

    // final-bag covers the SUM/MIN/MAX/AVG split-payload shapes the agg-propagation
    // logical rewrite declines (agg-propagation was already attempted above).
    if (AggJoinLogicalRewritesEnabled() &&
        TryRewriteNativeFinalBagPreagg(context, optimizer, op, agg, *join, agg_child, state, has_parent)) {
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

    // Resolve group/aggregate expressions to their position in the agg_child's
    // output bindings. See ResolveChildBinding in aggjoin_optimizer_shared.hpp.
    auto child_bindings = agg_child.GetColumnBindings();
    auto resolveBinding = [&](Expression &e) -> idx_t {
        return ResolveChildBinding(e, child_bindings);
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
        auto join_idx = TraceProjectionChainPassthrough(agg_child, agg_idx, true);
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
        // The fused operator can only handle a single direct column ref per
        // aggregate. Bail on DISTINCT/FILTER/ORDER BY and on multi-arg or
        // expression-wrapped inputs (e.g. SUM(a*b), SUM(CAST(x AS DOUBLE))) —
        // otherwise the runtime silently treats them as no-input aggregates
        // and produces 0. Native handles these correctly.
        if (ba.IsDistinct() || ba.filter || ba.order_bys) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr, "[AGGJOIN] bail: aggregate %s has DISTINCT/FILTER/ORDER BY\n", fn.c_str());
            }
            return;
        }
        if (ba.children.size() > 1) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr, "[AGGJOIN] bail: aggregate %s has %zu arguments (only 0 or 1 supported)\n",
                        fn.c_str(), ba.children.size());
            }
            return;
        }
        if (!ba.children.empty()) {
            auto cls = ba.children[0]->GetExpressionClass();
            if (cls != ExpressionClass::BOUND_REF && cls != ExpressionClass::BOUND_COLUMN_REF) {
                if (AggJoinTraceEnabled()) {
                    fprintf(stderr,
                            "[AGGJOIN] bail: aggregate %s input is not a direct column ref\n",
                            fn.c_str());
                }
                return;
            }
        }
        col.agg_funcs.push_back(fn);
        bool is_numeric = true;
        if (!ba.children.empty() && !ba.children[0]->return_type.IsNumeric()) {
            is_numeric = false; // VARCHAR, DATE, etc.
        }
        col.agg_is_numeric.push_back(is_numeric);
        if (ba.children.empty()) {
            // True COUNT(*): no input column, runtime counts rows.
            resolved_aggs.push_back({true, DConstants::INVALID_INDEX, {}});
            continue;
        }
        auto agg_child_idx = resolveBinding(*ba.children[0]);
        if (agg_child_idx == DConstants::INVALID_INDEX) {
            // Column ref didn't resolve through the agg's child bindings.
            // The runtime can't read it, so bail rather than silently produce 0.
            if (AggJoinTraceEnabled()) {
                fprintf(stderr,
                        "[AGGJOIN] bail: aggregate %s input column did not resolve\n", fn.c_str());
            }
            return;
        }
        auto join_idx = TraceProjectionChainPassthrough(agg_child, agg_child_idx);
        if (join_idx == DConstants::INVALID_INDEX || join_idx >= join_bindings.size()) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr,
                        "[AGGJOIN] bail: aggregate %s input did not trace through projection chain\n",
                        fn.c_str());
            }
            return;
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

    bool has_unsupported_minmax = false;
    {
        idx_t ra_idx = 0;
        for (auto &e : agg.expressions) {
            auto &ba = e->Cast<BoundAggregateExpression>();
            auto fn = StringUtil::Upper(ba.function.name);
            if (ra_idx < resolved_aggs.size()) {
                auto &ra = resolved_aggs[ra_idx];
                if (ra.scan_idx != DConstants::INVALID_INDEX && (fn == "MIN" || fn == "MAX")) {
                    if (ba.children.empty() ||
                        !IsDoubleExactMinMaxPayload(ba.children[0]->return_type)) {
                        has_unsupported_minmax = true;
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

    // Extract join key column indices from join conditions by resolving the
    // child output binding, not binding.column_index. The latter is only safe
    // for base-table children and breaks as soon as a join child is itself a
    // nested join/project subtree.
    auto left_child_bindings = join->children[0]->GetColumnBindings();
    auto right_child_bindings = join->children[1]->GetColumnBindings();
    auto find_binding_idx = [&](const vector<ColumnBinding> &bindings, const ColumnBinding &binding) -> idx_t {
        for (idx_t i = 0; i < bindings.size(); i++) {
            if (bindings[i] == binding) {
                return i;
            }
        }
        return DConstants::INVALID_INDEX;
    };
    auto resolve_join_child_idx = [&](Expression &expr, const vector<ColumnBinding> &child_bindings) -> idx_t {
        if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
            auto idx = expr.Cast<BoundReferenceExpression>().index;
            return idx < child_bindings.size() ? idx : DConstants::INVALID_INDEX;
        }
        ColumnBinding binding;
        return ExtractBinding(expr, binding) ? find_binding_idx(child_bindings, binding)
                                             : DConstants::INVALID_INDEX;
    };
    auto &left_child_types = join->children[0]->types;
    auto &right_child_types = join->children[1]->types;
    for(auto &cond : join->conditions) {
        auto li = resolve_join_child_idx(*cond.left, left_child_bindings);
        auto ri = resolve_join_child_idx(*cond.right, right_child_bindings);
        if (li == DConstants::INVALID_INDEX || ri == DConstants::INVALID_INDEX) return;
        // Bug C (2026-06-15): a UINT64 join key paired with a different-width or
        // signed integer key can hold values (> INT64_MAX) that are
        // unrepresentable in the signed-offset arithmetic the direct/segmented
        // runtime uses — such a value wraps negative and can collide with a real
        // signed build-key slot. Bail mismatched-integer-type joins to native.
        if (li < left_child_types.size() && ri < right_child_types.size()) {
            auto lp = left_child_types[li].InternalType();
            auto rp = right_child_types[ri].InternalType();
            if ((lp == PhysicalType::UINT64 || rp == PhysicalType::UINT64) && lp != rp) {
                return;
            }
        }
        if (need_swap) {
            // After swap: original left (probe) is now build, right is now probe
            col.probe_key_cols.push_back(ri);
            col.build_key_cols.push_back(li);
        } else {
            col.probe_key_cols.push_back(li);
            col.build_key_cols.push_back(ri);
        }
    }

    // Bail if key columns couldn't be extracted
    if (col.probe_key_cols.empty() || col.build_key_cols.empty()) return;
    if (col.probe_key_cols.size() != col.build_key_cols.size()) return;

    if (AggJoinLogicalRewritesEnabled() &&
        TryRewriteNativeMixedSidePreagg(context, optimizer, op, agg, *join, agg_child, col, need_swap,
                                        state, has_parent)) {
        return;
    }

    if (AggJoinLogicalRewritesEnabled() &&
        TryRewriteNativeBuildPreagg(context, optimizer, op, agg, *join, agg_child, col, build_agg_count, need_swap,
                                    state, has_parent)) {
        return;
    }

    if (has_unsupported_minmax) {
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
        auto join_est = join->has_estimated_cardinality ? join->estimated_cardinality : 0;
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
        bool stats_planned_direct_borderline_fanout = false;
        bool stats_planned_direct_build_fanout = false;
        bool stats_planned_direct_build_strong_fanout = false;
        bool stats_planned_direct_high_fanout = false;
        idx_t stats_build_key_domain = 0;
        idx_t stats_probe_key_domain = 0;
        if (direct_like_shape && !composite_shape && !has_unsupported_minmax) {
            auto &probe_child = *(need_swap ? join->children[1] : join->children[0]);
            auto &build_child = *(need_swap ? join->children[0] : join->children[1]);
            auto probe_key_idx = col.probe_key_cols[0];
            auto build_key_idx = col.build_key_cols[0];
            if (probe_key_idx < probe_types.size() && build_key_idx < build_types.size() &&
                IsDirectPlannableInteger(probe_types[probe_key_idx].InternalType()) &&
                IsDirectPlannableInteger(build_types[build_key_idx].InternalType())) {
                auto probe_plan_bindings = probe_child.GetColumnBindings();
                auto build_plan_bindings = build_child.GetColumnBindings();
                int64_t build_key_min = 0;
                int64_t build_key_max = 0;
                if (probe_key_idx < probe_plan_bindings.size() &&
                    build_key_idx < build_plan_bindings.size() &&
                    AggJoinTryGetIntegerKeyMinMaxFromStats(context, build_child,
                                                           build_plan_bindings[build_key_idx],
                                                           build_key_min, build_key_max)) {
                    idx_t planned_range = 0;
                    if (AggJoinTryGetDomainFromMinMax(build_key_min, build_key_max, planned_range)) {
                        stats_build_key_domain = planned_range;
                        int64_t probe_key_min = 0;
                        int64_t probe_key_max = 0;
                        if (AggJoinTryGetIntegerKeyMinMaxFromStats(context, probe_child,
                                                                   probe_plan_bindings[probe_key_idx],
                                                                   probe_key_min, probe_key_max) &&
                            AggJoinTryGetDomainFromMinMax(probe_key_min, probe_key_max, stats_probe_key_domain)) {
                            bool probe_range_covered = probe_key_min >= build_key_min && probe_key_max <= build_key_max;
                            bool build_density_at_least_1_5x =
                                AggJoinAtLeastRatio(build_est, stats_build_key_domain, 3, 2);
                            bool build_density_at_least_3x =
                                AggJoinAtLeastRatio(build_est, stats_build_key_domain, 3, 1);
                            bool build_density_at_least_4x =
                                AggJoinAtLeastRatio(build_est, stats_build_key_domain, 4, 1);
                            bool build_density_at_least_32x =
                                AggJoinAtLeastRatio(build_est, stats_build_key_domain, 32, 1);
                            stats_planned_direct_borderline_fanout =
                                probe_range_covered && build_density_at_least_1_5x;
                            stats_planned_direct_build_fanout =
                                probe_range_covered && build_density_at_least_3x;
                            stats_planned_direct_build_strong_fanout =
                                probe_range_covered && build_density_at_least_32x;
                            stats_planned_direct_high_fanout =
                                probe_range_covered && build_density_at_least_4x;
                            if (AggJoinTraceEnabled()) {
                                fprintf(stderr,
                                        "[AGGJOIN] planned direct stats: build_domain=%llu probe_domain=%llu probe_covered=%d build_density_1_5x=%d build_density_3x=%d build_density_4x=%d build_density_32x=%d\n",
                                        (unsigned long long)stats_build_key_domain,
                                        (unsigned long long)stats_probe_key_domain,
                                        probe_range_covered ? 1 : 0,
                                        build_density_at_least_1_5x ? 1 : 0,
                                        build_density_at_least_3x ? 1 : 0,
                                        build_density_at_least_4x ? 1 : 0,
                                        build_density_at_least_32x ? 1 : 0);
                            }
                        }
                        auto na_total = (idx_t)col.agg_funcs.size();
                        bool would_have_minmax = false;
                        bool would_have_avg = false;
                        bool has_build_aggs_est = build_agg_count > 0;
                        for (idx_t a = 0; a < na_total; a++) {
                            if (col.agg_funcs[a] == "MIN" || col.agg_funcs[a] == "MAX") would_have_minmax = true;
                            if (col.agg_funcs[a] == "AVG") would_have_avg = true;
                        }
                        idx_t bytes_per_key = sizeof(idx_t) +
                                             DirectAggBytesPerKey(col) +
                                             DirectBuildAggBytesPerKey(col);
                        idx_t max_working_set = (col.group_cols.empty() || group_matches_join_key)
                                                    ? 32 * 1024 * 1024
                                                    : 16 * 1024 * 1024;
                        if (group_matches_join_key && !col.group_cols.empty() && na_total == 1 &&
                            !would_have_minmax && !would_have_avg && !has_build_aggs_est) {
                            max_working_set = 48 * 1024 * 1024;
                        }
                        idx_t direct_limit = bytes_per_key > 0 ? max_working_set / bytes_per_key : 2000000;
                        if (direct_limit < 100000) direct_limit = 100000;
                        if (direct_limit > 8000000) direct_limit = 8000000;
                        if (planned_range <= direct_limit) {
                            col.planned_direct_mode = true;
                            col.planned_direct_key_min = build_key_min;
                            col.planned_direct_key_range = planned_range;
                            if (AggJoinTraceEnabled()) {
                                fprintf(stderr,
                                        "[AGGJOIN] planned direct build: key_min=%lld range=%llu limit=%llu\n",
                                        (long long)build_key_min,
                                        (unsigned long long)planned_range,
                                        (unsigned long long)direct_limit);
                            }
                        }
                    }
                }
            }
        }
        bool large_inputs = probe_est >= 100000 && build_est >= 100000;
        bool huge_inputs = probe_est >= 500000 && build_est >= 500000;
        bool group_known = group_est > 0;
        bool join_known = join_est > 0;
        bool low_probe_fanout = group_known && probe_est <= group_est * 2;
        bool low_build_fanout = group_known && build_est <= group_est * 2;
        bool low_fanout_shape = low_probe_fanout && low_build_fanout;
        bool has_build_aggs = build_agg_count > 0;
        bool planned_direct_parallel_shape = col.planned_direct_mode && direct_like_shape &&
                                             !composite_shape && !has_unsupported_minmax;
        if (planned_direct_parallel_shape) {
            for (idx_t a = 0; a < col.agg_funcs.size(); a++) {
                auto &fn = col.agg_funcs[a];
                bool on_build = col.agg_on_build.size() > a && col.agg_on_build[a];
                if (fn == "COUNT") {
                    continue;
                }
                if (fn != "AVG" && fn != "SUM" && fn != "MIN" && fn != "MAX") {
                    planned_direct_parallel_shape = false;
                    break;
                }
                auto &ba = agg.expressions[a]->Cast<BoundAggregateExpression>();
                if (ba.children.empty()) {
                    planned_direct_parallel_shape = false;
                    break;
                }
                auto payload_type = ba.children[0]->return_type.InternalType();
                bool payload_ok = payload_type == PhysicalType::DOUBLE || payload_type == PhysicalType::FLOAT;
                if (on_build) {
                    payload_ok = payload_ok || payload_type == PhysicalType::INT8 ||
                                 payload_type == PhysicalType::INT16 || payload_type == PhysicalType::INT32 ||
                                 payload_type == PhysicalType::INT64 || payload_type == PhysicalType::UINT8 ||
                                 payload_type == PhysicalType::UINT16 || payload_type == PhysicalType::UINT32 ||
                                 payload_type == PhysicalType::UINT64;
                }
                if (!payload_ok) {
                    planned_direct_parallel_shape = false;
                    break;
                }
            }
        }
        bool planned_direct_sparse_grouped = false;
        if (planned_direct_parallel_shape && !col.group_cols.empty()) {
            idx_t bytes_per_key = DirectAggBytesPerKey(col) + sizeof(uint8_t);
            __int128 local_bytes = (__int128)col.planned_direct_key_range * (__int128)bytes_per_key;
            planned_direct_sparse_grouped = local_bytes > (__int128)16 * 1024 * 1024;
        }
        bool planned_direct_borderline_fanout = planned_direct_parallel_shape && join_known && probe_est > 0 &&
                                                join_est >= probe_est &&
                                                join_est - probe_est >= probe_est / 4;
        planned_direct_borderline_fanout =
            planned_direct_borderline_fanout ||
            (planned_direct_parallel_shape && stats_planned_direct_borderline_fanout);
        bool planned_direct_build_side_fanout = planned_direct_parallel_shape && has_build_aggs &&
                                                join_known && probe_est > 0 &&
                                                AggJoinAtLeastRatio(join_est, probe_est, 3, 1);
        planned_direct_build_side_fanout =
            planned_direct_build_side_fanout ||
            (planned_direct_parallel_shape && has_build_aggs && stats_planned_direct_build_fanout);
        bool planned_direct_build_side_strong_fanout = planned_direct_parallel_shape && has_build_aggs &&
                                                       join_known && probe_est > 0 &&
                                                       AggJoinAtLeastRatio(join_est, probe_est, 32, 1);
        planned_direct_build_side_strong_fanout =
            planned_direct_build_side_strong_fanout ||
            (planned_direct_parallel_shape && has_build_aggs && stats_planned_direct_build_strong_fanout);
        bool planned_direct_build_side_required_fanout =
            col.group_cols.empty() ? planned_direct_build_side_strong_fanout : planned_direct_build_side_fanout;
        bool planned_direct_sparse_high_fanout = planned_direct_sparse_grouped && join_known && probe_est > 0 &&
                                                 join_est >= probe_est * 4;
        planned_direct_sparse_high_fanout =
            planned_direct_sparse_high_fanout ||
            (planned_direct_sparse_grouped && stats_planned_direct_high_fanout);
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
        bool join_close_to_probe = join_known && probe_est > 0 &&
                                   (join_est <= probe_est || join_est - probe_est <= probe_est);
        bool build_close_to_groups = group_known && build_est > 0 &&
                                     (build_est <= group_est || build_est - group_est <= group_est / 2);
        bool low_fanout_join_key_aggregate = !has_build_aggs && direct_like_shape && !composite_shape &&
                                             large_inputs && join_close_to_probe &&
                                             !col.group_cols.empty() && group_matches_join_key &&
                                             build_close_to_groups;
        bool low_fanout_ungrouped_payload = !has_build_aggs && direct_like_shape && !composite_shape &&
                                             large_inputs && join_close_to_probe &&
                                             col.group_cols.empty() && !has_count_or_avg;
        bool estimated_sparse_join = !join_known || join_est <= probe_est + (probe_est / 2);
        const char *gate_reason = nullptr;
        if (has_varlen_key && !simple_varlen_hash_shape) gate_reason = "variable-width join/group key";
        else if (asym_build_heavy)
            gate_reason = "build-heavy aggregate shape better handled natively";
        else if (build_rollup && !planned_direct_parallel_shape && large_inputs &&
                 (build_agg_count >= 2 || group_est == 0 || group_est >= 256))
            gate_reason = "build-side rollup outside fast path";
        else if (has_build_aggs && heavy_build_aggs && large_inputs && !direct_like_shape)
            gate_reason = "build-side aggregate fanout outside direct path";
        else if (has_build_aggs && !planned_direct_parallel_shape && large_inputs && group_matches_join_key &&
                 build_agg_count >= 4 && has_count_or_avg)
            gate_reason = "build-side aggregate mix outside direct fast path";
        else if (has_build_aggs && planned_direct_parallel_shape && large_inputs &&
                 !planned_direct_build_side_required_fanout)
            gate_reason = "build-side planned-direct needs fanout";
        else if (!has_build_aggs && !composite_shape && has_non_integral_key &&
                 !simple_varlen_hash_shape &&
                 group_matches_join_key && large_inputs)
            gate_reason = "non-integral single-key shape outside direct path";
        else if (planned_direct_sparse_grouped && !planned_direct_sparse_high_fanout)
            gate_reason = "wide planned-direct grouped shape needs high fanout";
        else if (!planned_direct_borderline_fanout && low_fanout_join_key_aggregate)
            gate_reason = "low-fanout join-key aggregate";
        else if (!planned_direct_borderline_fanout && low_fanout_ungrouped_payload)
            gate_reason = "low-fanout ungrouped payload aggregate";
        else if (!planned_direct_borderline_fanout && !has_build_aggs && !has_count_or_avg && !composite_shape &&
                 large_inputs && low_fanout_shape && estimated_sparse_join)
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
                        "[AGGJOIN] planner cost gate would bail: %s (join_conds=%zu, groups=%zu, build_aggs=%zu, probe_est=%llu, build_est=%llu, join_est=%llu, group_est=%llu)\n",
                        gate_reason, join->conditions.size(), agg.groups.size(), build_agg_count,
                        (unsigned long long)probe_est, (unsigned long long)build_est,
                        (unsigned long long)join_est, (unsigned long long)group_est);
            }
#ifndef AGGJOIN_NO_PLANNER_GATE
            return;
#else
            fprintf(stderr,
                    "[AGGJOIN] planner cost gate would bail: %s (join_conds=%zu, groups=%zu, build_aggs=%zu, probe_est=%llu, build_est=%llu, join_est=%llu, group_est=%llu)\n",
                    gate_reason, join->conditions.size(), agg.groups.size(), build_agg_count,
                    (unsigned long long)probe_est, (unsigned long long)build_est,
                    (unsigned long long)join_est, (unsigned long long)group_est);
#endif
        }
    }

    if (AggJoinTraceEnabled()) {
        fprintf(stderr,
                "[AGGJOIN] planner fired: join_conds=%zu groups=%zu aggs=%zu build_aggs=%zu need_swap=%d\n",
                join->conditions.size(), agg.groups.size(), agg.expressions.size(), build_agg_count,
                need_swap ? 1 : 0);
    }

    // The fused operator is the riskier, custom-execution path — let a deployment
    // turn it off independently of the native-lowering logical rewrites.
    if (!AggJoinOperatorEnabled()) {
        return;
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

    SetAggJoinLastRewrite("fused");
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
