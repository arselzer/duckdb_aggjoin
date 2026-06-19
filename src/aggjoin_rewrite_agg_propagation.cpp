// Aggregate-propagation Yannakakis-style rewrite.
//
// Lowers `SELECT COUNT(*) FROM R0, R1, ..., R(n-1) WHERE <acyclic tree of
// INNER equi-joins>` into a native frequency-propagation logical rewrite
// (HASH_GROUP_BY -> HASH_JOIN -> HASH_GROUP_BY -> ...), so the intermediate join
// product is never materialised. Each tree edge may be a single equality or a
// composite vector of equality predicates connecting the same pair of leaves.
//
// Correctness sketch (the GYO/Yannakakis foundation): for an acyclic join the
// join tree has the running-intersection property. Pre-aggregating each relation
// by its interface key and propagating per-key multiplicity as a DOUBLE `_freq`
// (leaf _freq = COUNT(*) per key; parent _freq = SUM(parent rows' child _freq))
// yields, at the root, one row per root tuple whose _freq equals how many full-
// join rows it represents. SUM of root _freq therefore equals COUNT(*) of the
// full join, without materialising it. Empty/dangling inputs -> 0 via COALESCE.
//
// Scope: COUNT/SUM/MIN/MAX/AVG/COUNT(col)/set-safe/VAR-style aggregates over an
// acyclic join tree (>= 3 leaves) whose edges are bare columns or deterministic
// leaf-local key expressions. Anything outside that shape is treated as an
// OPAQUE LEAF (safe: its multiplicity is captured by COUNT(*) over the leaf subtree).
//
// Review/design context: docs/ (2026-06-15 critical review + Yannakakis design).

#include "aggjoin_rewrites_internal.hpp"
#include "aggjoin_stats.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

namespace {

// Peel value-reconciling CASTs to the underlying column binding. Returns false
// for anything that is not a (possibly cast-wrapped) bare column reference.
bool ExtractBareBinding(Expression &expr, ColumnBinding &binding) {
    auto cls = expr.GetExpressionClass();
    if (cls == ExpressionClass::BOUND_COLUMN_REF) {
        binding = expr.Cast<BoundColumnRefExpression>().binding;
        return true;
    }
    if (cls == ExpressionClass::BOUND_CAST) {
        return ExtractBareBinding(*expr.Cast<BoundCastExpression>().child, binding);
    }
    return false;
}

struct AggPropLeaf {
    unique_ptr<LogicalOperator> *slot = nullptr; // owning slot inside the join tree
    vector<ColumnBinding> bindings;
    vector<LogicalType> types;
    idx_t estimated_cardinality = 0;
};

struct AggPropEdge {
    idx_t leaf_a = 0;
    idx_t leaf_b = 0;
    vector<ColumnBinding> cols_a;
    vector<ColumnBinding> cols_b;
    vector<LogicalType> types_a;
    vector<LogicalType> types_b;
};

struct RawJoinPredicate {
    unique_ptr<Expression> left;
    unique_ptr<Expression> right;
};

struct LoweredJoinPredicate {
    ColumnBinding left;
    ColumnBinding right;
};

struct LeafComputedKey {
    unique_ptr<Expression> expr;
    LogicalType type;
};

struct LeafProjectionInfo {
    bool projected = false;
    vector<ColumnBinding> old_bindings;
    vector<ColumnBinding> new_bindings;
    vector<ColumnBinding> computed_bindings;
};

struct JoinKeyRequest {
    idx_t leaf = DConstants::INVALID_INDEX;
    bool computed = false;
    ColumnBinding binding;
    idx_t computed_idx = DConstants::INVALID_INDEX;
};

// A user aggregate to propagate through frequency propagation. `home_leaf` is the leaf
// whose base relation holds the input column (INVALID for COUNT(*)). The
// frequency-weighted value is carried up the tree as an `_agg` column (plus an
// `_agg_cnt` column for AVG) and combined at the root -- see FoldNode threading.
struct AggSpec {
    string fn;                          // SUM / MIN / MAX / AVG / COUNT / COUNT_STAR
    bool is_count_star = false;
    idx_t home_leaf = DConstants::INVALID_INDEX;
    ColumnBinding col_b;                // input column (in the home leaf)
    LogicalType col_t;
    LogicalType result_type;            // the user aggregate's return type
};

// The running state of an aggregate's `_agg` column in a node's CTE output.
struct AggState {
    bool present = false;               // is `_agg` live in this node's output?
    ColumnBinding agg_b;                // sum / min / max / count value; for VAR: Σ(x·f)
    LogicalType agg_t;
    bool has_cnt = false;               // AVG/VAR carry a denominator: n = Σ(f over non-null)
    ColumnBinding cnt_b;
    LogicalType cnt_t;
    bool has_sxx = false;               // VAR/STDDEV carry a third column: Σ(x²·f)
    ColumnBinding sxx_b;
    LogicalType sxx_t;
};

// VAR_POP / VAR_SAMP / VARIANCE / STDDEV_POP / STDDEV_SAMP / STDDEV: computed via
// EXACT HUGEINT moments (n·Σx² − (Σx)² is an exact non-negative integer for integer
// x; the cancellation that plagues the DOUBLE moment form vanishes). Three
// sub-aggregates Σ(x²·f), Σ(x·f), n=Σ(f over non-null x) thread up; the root forms
// the variance. The result is DETERMINISTIC and the correctly-rounded true value --
// strictly better than native var_pop, which is thread-dependent (parallel Welford).
bool IsVarFamily(const string &fn) {
    return fn == "VAR_POP" || fn == "VAR_SAMP" || fn == "VARIANCE" || fn == "STDDEV_POP" ||
           fn == "STDDEV_SAMP" || fn == "STDDEV";
}
bool IsVarSample(const string &fn) {
    return fn == "VAR_SAMP" || fn == "VARIANCE" || fn == "STDDEV_SAMP" || fn == "STDDEV";
}
bool IsStddev(const string &fn) {
    return fn == "STDDEV_POP" || fn == "STDDEV_SAMP" || fn == "STDDEV";
}

bool BindingsContain(const vector<ColumnBinding> &v, const ColumnBinding &x) {
    for (auto &e : v) {
        if (e == x) {
            return true;
        }
    }
    return false;
}

idx_t FindLeafForBinding(const vector<AggPropLeaf> &leaves, idx_t lo, idx_t hi, const ColumnBinding &b) {
    for (idx_t i = lo; i < hi; i++) {
        if (BindingsContain(leaves[i].bindings, b)) {
            return i;
        }
    }
    return DConstants::INVALID_INDEX;
}

bool TypeForBinding(const AggPropLeaf &leaf, const ColumnBinding &b, LogicalType &out) {
    for (idx_t i = 0; i < leaf.bindings.size(); i++) {
        if (leaf.bindings[i] == b) {
            out = leaf.types[i];
            return true;
        }
    }
    return false;
}

// Best-effort max(|min|,|max|) for column `b` on `leaf`, read from the base
// table's column statistics (zonemaps).  Returns -1.0 when unavailable — no
// resolvable LogicalGet (a projection/join in the way), no statistics callback,
// or no min/max — so callers keep the conservative type-worst-case behaviour.
// Mirrors DuckDB's own usage (statistics_propagation/propagate_get.cpp):
//   get.function.statistics(context, get.bind_data.get(), col_ids[i].GetPrimaryIndex())
// Used to safely admit wide-INTEGER VAR/STDDEV: the exact HUGEINT moment
// `n·Σx² − (Σx)²` overflows only if the ACTUAL values are large, which stats
// can rule out (a BIGINT column whose values fit in 32 bits is just as safe).
double ColumnMaxAbsFromStats(ClientContext &context, const AggPropLeaf &leaf, const ColumnBinding &b) {
    if (!leaf.slot || !*leaf.slot) {
        return -1.0; // slot already moved out / empty
    }
    double lo = 0;
    double hi = 0;
    if (!AggJoinTryGetNumericMinMaxFromStats(context, **leaf.slot, b, lo, hi)) {
        return -1.0;
    }
    return std::max(std::fabs(lo), std::fabs(hi));
}

// The accumulator type an aggregate's value threads through the fold in -- chosen
// so the logical rewrite matches native EXACTLY, never rounding through a DOUBLE it
// shouldn't:
//   COUNT/COUNT(col)          -> HUGEINT  (exact integer multiplicity sum)
//   SUM(integer, any width)   -> HUGEINT  (DuckDB SUM(int) returns HUGEINT)
//   SUM(DECIMAL(p,s))         -> DECIMAL(38,s)  (DuckDB SUM(decimal) returns DECIMAL(38,s))
//   SUM(FLOAT/DOUBLE)         -> DOUBLE   (inherently floating)
//   AVG(integer)              -> HUGEINT  (exact numerator+denominator, /DOUBLE at root)
//   AVG(DECIMAL/FLOAT/DOUBLE) -> DOUBLE   (native AVG already returns DOUBLE)
// Returns DOUBLE for MIN/MAX (unused -- they pass the native column through).
LogicalType AggAccumType(const string &fn, const LogicalType &col_t) {
    if (fn == "COUNT") {
        return LogicalType::HUGEINT;
    }
    if (fn == "SUM") {
        if (col_t.IsIntegral()) {
            return LogicalType::HUGEINT;
        }
        if (col_t.id() == LogicalTypeId::DECIMAL) {
            return LogicalType::DECIMAL(38, DecimalType::GetScale(col_t)); // DECIMAL(38,s)
        }
        return LogicalType::DOUBLE; // FLOAT / DOUBLE
    }
    // AVG and VAR/STDDEV accumulate in HUGEINT for integer columns. VAR is gated to
    // <= 32-bit integer, so its three moments (Sum x, Sum x*x, n) stay exact HUGEINT.
    if (fn == "AVG" || IsVarFamily(fn)) {
        return col_t.IsIntegral() ? LogicalType::HUGEINT : LogicalType::DOUBLE;
    }
    return LogicalType::DOUBLE; // set-safe (MIN/MAX/BOOL/BIT) -- unused
}

// Set-safe aggregates are idempotent under multiplicity (repeating a value freq
// times does not change the result), so the freq-fold passes them straight through
// with NO _freq weighting -- exactly like MIN/MAX. BIT_XOR is deliberately NOT here:
// it cancels on even repeats (x ^ x = 0), so it depends on multiplicity parity and
// is NOT freq-safe. (SUM/COUNT/AVG are multiplicity-sensitive and freq-weighted.)
bool IsSetSafe(const string &fn) {
    return fn == "MIN" || fn == "MAX" || fn == "BOOL_AND" || fn == "BOOL_OR" || fn == "BIT_AND" ||
           fn == "BIT_OR";
}

void CollectExpressionBindings(Expression &expr, vector<ColumnBinding> &bindings) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
        auto binding = expr.Cast<BoundColumnRefExpression>().binding;
        if (!BindingsContain(bindings, binding)) {
            bindings.push_back(binding);
        }
    }
    ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { CollectExpressionBindings(child, bindings); });
}

bool ExpressionContainsNullConstant(Expression &expr) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
        expr.Cast<BoundConstantExpression>().value.IsNull()) {
        return true;
    }
    bool contains_null = false;
    ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
        if (ExpressionContainsNullConstant(child)) {
            contains_null = true;
        }
    });
    return contains_null;
}

bool IsSupportedJoinKeyExpression(Expression &expr) {
    ColumnBinding dummy;
    if (ExtractBareBinding(expr, dummy)) {
        return true;
    }
    return !ExpressionContainsNullConstant(expr) && !expr.IsVolatile() && !expr.HasSubquery() &&
           !expr.HasParameter() && !expr.IsAggregate() && !expr.IsWindow();
}

bool ReplaceBoundReferencesWithColumnRefs(unique_ptr<Expression> &expr, const vector<ColumnBinding> &bindings,
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
        if (!ReplaceBoundReferencesWithColumnRefs(child, bindings, types)) {
            ok = false;
        }
    });
    return ok;
}

void AddOpaqueLeaf(ClientContext &context, unique_ptr<LogicalOperator> &slot, vector<AggPropLeaf> &leaves) {
    auto &node = *slot;
    AggPropLeaf leaf;
    leaf.slot = &slot;
    leaf.bindings = node.GetColumnBindings();
    leaf.types = node.types;
    leaf.estimated_cardinality =
        node.has_estimated_cardinality ? node.estimated_cardinality : node.EstimateCardinality(context);
    idx_t no_op_filtered_cardinality = 0;
    if (AggJoinTryGetNoOpFilteredCardinality(context, node, no_op_filtered_cardinality)) {
        leaf.estimated_cardinality = std::max(leaf.estimated_cardinality, no_op_filtered_cardinality);
    }
    leaves.push_back(std::move(leaf));
}

bool IsFlattenableJoin(LogicalOperator &node, vector<RawJoinPredicate> &predicates) {
    if (node.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        auto &j = node.Cast<LogicalComparisonJoin>();
        if (j.join_type == JoinType::INNER && !j.conditions.empty()) {
            auto left_bindings = j.children[0]->GetColumnBindings();
            auto right_bindings = j.children[1]->GetColumnBindings();
            auto &left_types = j.children[0]->types;
            auto &right_types = j.children[1]->types;
            vector<RawJoinPredicate> local;
            for (auto &cond : j.conditions) {
                RawJoinPredicate pred;
                pred.left = cond.left->Copy();
                pred.right = cond.right->Copy();
                bool left_refs_ok = ReplaceBoundReferencesWithColumnRefs(pred.left, left_bindings, left_types);
                bool right_refs_ok = ReplaceBoundReferencesWithColumnRefs(pred.right, right_bindings, right_types);
                bool left_supported = left_refs_ok && IsSupportedJoinKeyExpression(*pred.left);
                bool right_supported = right_refs_ok && IsSupportedJoinKeyExpression(*pred.right);
                if (cond.comparison != ExpressionType::COMPARE_EQUAL || !left_supported || !right_supported) {
                    return false;
                }
                local.push_back(std::move(pred));
            }
            for (auto &pred : local) {
                predicates.push_back(std::move(pred));
            }
            return true;
        }
    }
    if (node.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT && node.children.size() == 2) {
        return true;
    }
    return false;
}

// Recursively classify the join subtree owned by `slot` into leaves plus a flat
// list of bare equality predicates. Edges are intentionally resolved only after
// all leaves are known, so a valid star/tree is recognized even when DuckDB's
// binary join tree groups several unrelated leaves on one side of a later join.
// Unsupported join nodes become opaque leaves; their multiplicity is then
// captured by the fold just like a base scan.
void CollectJoinGraph(ClientContext &context, unique_ptr<LogicalOperator> &slot, vector<AggPropLeaf> &leaves,
                      vector<RawJoinPredicate> &predicates) {
    auto &node = *slot;
    idx_t predicate_base = predicates.size();
    if (IsFlattenableJoin(node, predicates)) {
        idx_t leaf_base = leaves.size();
        for (auto &child : node.children) {
            CollectJoinGraph(context, child, leaves, predicates);
        }
        if (leaves.size() > leaf_base) {
            return;
        }
        predicates.resize(predicate_base);
    }
    AddOpaqueLeaf(context, slot, leaves);
}

bool ResolveJoinKeyRequest(Expression &expr, const vector<AggPropLeaf> &leaves,
                           vector<vector<LeafComputedKey>> &computed_keys, JoinKeyRequest &request) {
    ColumnBinding bare;
    if (ExtractBareBinding(expr, bare)) {
        idx_t leaf = FindLeafForBinding(leaves, 0, leaves.size(), bare);
        if (leaf == DConstants::INVALID_INDEX) {
            return false;
        }
        request.leaf = leaf;
        request.computed = false;
        request.binding = bare;
        return true;
    }

    vector<ColumnBinding> referenced;
    CollectExpressionBindings(expr, referenced);
    if (referenced.empty()) {
        return false; // constants do not connect two leaves
    }
    idx_t leaf = DConstants::INVALID_INDEX;
    for (auto &binding : referenced) {
        idx_t binding_leaf = FindLeafForBinding(leaves, 0, leaves.size(), binding);
        if (binding_leaf == DConstants::INVALID_INDEX) {
            return false;
        }
        if (leaf == DConstants::INVALID_INDEX) {
            leaf = binding_leaf;
        } else if (leaf != binding_leaf) {
            return false; // expression spans multiple leaves
        }
    }

    request.leaf = leaf;
    request.computed = true;
    for (idx_t i = 0; i < computed_keys[leaf].size(); i++) {
        if (Expression::Equals(*computed_keys[leaf][i].expr, expr)) {
            request.computed_idx = i;
            return true;
        }
    }
    request.computed_idx = computed_keys[leaf].size();
    computed_keys[leaf].push_back({expr.Copy(), expr.return_type});
    return true;
}

bool RewriteBindingThroughProjection(const vector<LeafProjectionInfo> &projection_info, ColumnBinding &binding) {
    for (auto &info : projection_info) {
        if (!info.projected) {
            continue;
        }
        for (idx_t i = 0; i < info.old_bindings.size(); i++) {
            if (info.old_bindings[i] == binding) {
                binding = info.new_bindings[i];
                return true;
            }
        }
    }
    return false;
}

ColumnBinding RequestBinding(const JoinKeyRequest &request, const vector<LeafProjectionInfo> &projection_info) {
    if (request.computed) {
        return projection_info[request.leaf].computed_bindings[request.computed_idx];
    }
    auto binding = request.binding;
    RewriteBindingThroughProjection(projection_info, binding);
    return binding;
}

bool BindingIsDerivedProjectionKey(LogicalOperator &op, const ColumnBinding &binding) {
    if (op.type == LogicalOperatorType::LOGICAL_PROJECTION && op.children.size() == 1) {
        auto &proj = op.Cast<LogicalProjection>();
        if (binding.table_index == proj.table_index && binding.column_index < proj.expressions.size()) {
            auto &expr = *proj.expressions[binding.column_index];
            ColumnBinding child_binding;
            if (ExtractBareBinding(expr, child_binding)) {
                return BindingIsDerivedProjectionKey(*proj.children[0], child_binding);
            }
            if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
                auto child_bindings = proj.children[0]->GetColumnBindings();
                auto idx = expr.Cast<BoundReferenceExpression>().index;
                if (idx < child_bindings.size()) {
                    return BindingIsDerivedProjectionKey(*proj.children[0], child_bindings[idx]);
                }
            }
            return true;
        }
    }
    if (op.children.size() == 1 && BindingsContain(op.children[0]->GetColumnBindings(), binding)) {
        return BindingIsDerivedProjectionKey(*op.children[0], binding);
    }
    return false;
}

void ApplyComputedKeyProjections(ClientContext &context, Optimizer &optimizer, vector<AggPropLeaf> &leaves,
                                 vector<vector<LeafComputedKey>> &computed_keys,
                                 vector<LeafProjectionInfo> &projection_info) {
    auto colref = [](const LogicalType &t, const ColumnBinding &b) {
        return make_uniq<BoundColumnRefExpression>(t, b);
    };
    projection_info.assign(leaves.size(), LeafProjectionInfo());
    for (idx_t leaf_idx = 0; leaf_idx < leaves.size(); leaf_idx++) {
        auto &keys = computed_keys[leaf_idx];
        if (keys.empty()) {
            continue;
        }
        auto &leaf = leaves[leaf_idx];
        auto table_index = optimizer.binder.GenerateTableIndex();
        auto old_bindings = leaf.bindings;
        auto old_types = leaf.types;
        vector<unique_ptr<Expression>> expressions;
        expressions.reserve(old_bindings.size() + keys.size());
        for (idx_t i = 0; i < old_bindings.size(); i++) {
            expressions.push_back(colref(old_types[i], old_bindings[i]));
        }
        for (auto &key : keys) {
            expressions.push_back(std::move(key.expr));
        }

        auto proj = make_uniq<LogicalProjection>(table_index, std::move(expressions));
        proj->children.push_back(std::move(*leaf.slot));
        proj->ResolveOperatorTypes();

        LeafProjectionInfo info;
        info.projected = true;
        info.old_bindings = std::move(old_bindings);
        for (idx_t i = 0; i < old_types.size(); i++) {
            info.new_bindings.push_back(ColumnBinding(table_index, i));
        }
        for (idx_t i = 0; i < keys.size(); i++) {
            info.computed_bindings.push_back(ColumnBinding(table_index, old_types.size() + i));
        }

        vector<ColumnBinding> new_bindings;
        new_bindings.reserve(proj->types.size());
        for (idx_t i = 0; i < proj->types.size(); i++) {
            new_bindings.push_back(ColumnBinding(table_index, i));
        }
        leaf.bindings = std::move(new_bindings);
        leaf.types = proj->types;
        *leaf.slot = std::move(proj);
        projection_info[leaf_idx] = std::move(info);
        (void)context;
    }
}

struct ComputedKeyProjectionRollback {
    explicit ComputedKeyProjectionRollback(vector<AggPropLeaf> &leaves, vector<LeafProjectionInfo> &projection_info)
        : leaves(leaves), projection_info(projection_info) {
    }

    ~ComputedKeyProjectionRollback() {
        if (!active) {
            return;
        }
        for (idx_t i = 0; i < leaves.size() && i < projection_info.size(); i++) {
            if (!projection_info[i].projected || !leaves[i].slot || !*leaves[i].slot ||
                (*leaves[i].slot)->type != LogicalOperatorType::LOGICAL_PROJECTION ||
                (*leaves[i].slot)->children.size() != 1) {
                continue;
            }
            *leaves[i].slot = std::move((*leaves[i].slot)->children[0]);
        }
    }

    void Commit() {
        active = false;
    }

    vector<AggPropLeaf> &leaves;
    vector<LeafProjectionInfo> &projection_info;
    bool active = true;
};

bool LowerJoinPredicates(ClientContext &context, Optimizer &optimizer, vector<AggPropLeaf> &leaves,
                         vector<RawJoinPredicate> &raw_predicates, vector<ColumnBinding> &group_bindings,
                         vector<AggSpec> &aggs, vector<LoweredJoinPredicate> &predicates,
                         vector<LeafProjectionInfo> &projection_info) {
    vector<vector<LeafComputedKey>> computed_keys(leaves.size());
    struct PendingPredicate {
        JoinKeyRequest left;
        JoinKeyRequest right;
    };
    vector<PendingPredicate> pending;
    pending.reserve(raw_predicates.size());
    for (auto &raw : raw_predicates) {
        PendingPredicate pred;
        if (!ResolveJoinKeyRequest(*raw.left, leaves, computed_keys, pred.left) ||
            !ResolveJoinKeyRequest(*raw.right, leaves, computed_keys, pred.right) ||
            pred.left.leaf == pred.right.leaf) {
            return false;
        }
        pending.push_back(pred);
    }

    ApplyComputedKeyProjections(context, optimizer, leaves, computed_keys, projection_info);
    for (auto &binding : group_bindings) {
        RewriteBindingThroughProjection(projection_info, binding);
    }
    for (auto &agg : aggs) {
        if (!agg.is_count_star) {
            RewriteBindingThroughProjection(projection_info, agg.col_b);
        }
    }

    predicates.clear();
    predicates.reserve(pending.size());
    for (auto &pred : pending) {
        predicates.push_back({RequestBinding(pred.left, projection_info), RequestBinding(pred.right, projection_info)});
    }
    return !predicates.empty();
}

bool BuildEdgesFromPredicates(const vector<AggPropLeaf> &leaves, const vector<LoweredJoinPredicate> &predicates,
                              vector<AggPropEdge> &edges) {
    edges.clear();
    for (auto &pred : predicates) {
        idx_t left_leaf = FindLeafForBinding(leaves, 0, leaves.size(), pred.left);
        idx_t right_leaf = FindLeafForBinding(leaves, 0, leaves.size(), pred.right);
        if (left_leaf == DConstants::INVALID_INDEX || right_leaf == DConstants::INVALID_INDEX ||
            left_leaf == right_leaf) {
            return false;
        }
        AggPropEdge *edge = nullptr;
        for (auto &candidate : edges) {
            if ((candidate.leaf_a == left_leaf && candidate.leaf_b == right_leaf) ||
                (candidate.leaf_a == right_leaf && candidate.leaf_b == left_leaf)) {
                edge = &candidate;
                break;
            }
        }
        if (!edge) {
            AggPropEdge new_edge;
            new_edge.leaf_a = left_leaf;
            new_edge.leaf_b = right_leaf;
            edges.push_back(std::move(new_edge));
            edge = &edges.back();
        }
        ColumnBinding col_a;
        ColumnBinding col_b;
        if (edge->leaf_a == left_leaf) {
            col_a = pred.left;
            col_b = pred.right;
        } else {
            col_a = pred.right;
            col_b = pred.left;
        }
        LogicalType type_a;
        LogicalType type_b;
        if (!TypeForBinding(leaves[edge->leaf_a], col_a, type_a) ||
            !TypeForBinding(leaves[edge->leaf_b], col_b, type_b)) {
            return false;
        }
        edge->cols_a.push_back(col_a);
        edge->cols_b.push_back(col_b);
        edge->types_a.push_back(std::move(type_a));
        edge->types_b.push_back(std::move(type_b));
    }
    return !edges.empty();
}

bool EstimateJoinTreeCardinalityFromStats(ClientContext &context, const vector<AggPropLeaf> &leaves,
                                          const vector<AggPropEdge> &edges, idx_t &estimated) {
    long double result = 1.0;
    for (auto &leaf : leaves) {
        if (leaf.estimated_cardinality == 0) {
            return false;
        }
        result *= static_cast<long double>(leaf.estimated_cardinality);
    }
    for (auto &edge : edges) {
        idx_t domain_a = 0;
        idx_t domain_b = 0;
        auto *leaf_a_op = leaves[edge.leaf_a].slot ? leaves[edge.leaf_a].slot->get() : nullptr;
        auto *leaf_b_op = leaves[edge.leaf_b].slot ? leaves[edge.leaf_b].slot->get() : nullptr;
        if (!leaf_a_op || !leaf_b_op ||
            !AggJoinTryGetCompositeKeyDomainFromStats(context, *leaf_a_op, edge.cols_a, edge.types_a, domain_a) ||
            !AggJoinTryGetCompositeKeyDomainFromStats(context, *leaf_b_op, edge.cols_b, edge.types_b, domain_b)) {
            return false;
        }
        auto denom = std::max(domain_a, domain_b);
        if (denom == 0) {
            return false;
        }
        result /= static_cast<long double>(denom);
    }
    if (result <= 0.0) {
        return false;
    }
    if (result >= static_cast<long double>(std::numeric_limits<idx_t>::max())) {
        estimated = std::numeric_limits<idx_t>::max();
    } else {
        estimated = static_cast<idx_t>(result);
    }
    return estimated > 0;
}

// Per-leaf adjacency for the general acyclic join tree. Each edge records this
// leaf's join-key vector and the neighbour's join-key vector (raw leaf bindings/types).
struct TreeAdjEdge {
    idx_t nbr;
    vector<ColumnBinding> my_cols;
    vector<LogicalType> my_types;
    vector<ColumnBinding> nbr_cols;
    vector<LogicalType> nbr_types;
};

// Build per-leaf adjacency. Returns false unless the equi-join graph is a
// connected TREE (exactly m-1 edges AND all leaves reachable) -- the
// alpha-acyclicity test: a cycle has >= m edges, a disconnected graph fewer
// reachable nodes. Stars and branching trees ARE accepted (any tree topology),
// unlike the former path-only check; only cycles and disconnected graphs bail.
bool BuildAdjacency(const vector<AggPropLeaf> &leaves, const vector<AggPropEdge> &edges,
                    vector<vector<TreeAdjEdge>> &adj) {
    idx_t m = leaves.size();
    if (edges.size() != m - 1) {
        return false; // a tree has exactly m-1 edges
    }
    adj.assign(m, vector<TreeAdjEdge>());
    for (auto &e : edges) {
        adj[e.leaf_a].push_back({e.leaf_b, e.cols_a, e.types_a, e.cols_b, e.types_b});
        adj[e.leaf_b].push_back({e.leaf_a, e.cols_b, e.types_b, e.cols_a, e.types_a});
    }
    // With m-1 edges, connected <=> acyclic. DFS from node 0.
    vector<bool> seen(m, false);
    vector<idx_t> stack;
    stack.push_back(0);
    seen[0] = true;
    idx_t reached = 0;
    while (!stack.empty()) {
        idx_t n = stack.back();
        stack.pop_back();
        reached++;
        for (auto &e : adj[n]) {
            if (!seen[e.nbr]) {
                seen[e.nbr] = true;
                stack.push_back(e.nbr);
            }
        }
    }
    return reached == m;
}

// Reconcile two join-key sides to their common super-type (widening), never
// narrowing. (Bug D discipline.) Returns false if no common type exists.
bool WidenKeys(ClientContext &context, JoinCondition &cond, const LogicalType &lt, const LogicalType &rt) {
    if (lt == rt) {
        return true;
    }
    LogicalType super;
    if (!LogicalType::TryGetMaxLogicalType(context, lt, rt, super)) {
        return false;
    }
    if (lt != super) {
        cond.left = BoundCastExpression::AddCastToType(context, std::move(cond.left), super);
    }
    if (rt != super) {
        cond.right = BoundCastExpression::AddCastToType(context, std::move(cond.right), super);
    }
    return true;
}

// Recursively fold the subtree rooted at `node` (the edge to `parent` excluded)
// into a single CTE. The output CTE exposes the node's column toward `parent`
// (when parent is valid), any requested `extra_keep` GROUP BY columns that live in
// this subtree, and a HUGEINT `_freq`. A node with k children produces k stacked
// aggregates: each fold JOINs one child CTE, then `SUM(node_freq * child_freq)`
// GROUP BY the still-live columns (persistent GROUP BY columns + not-yet-folded
// child join columns). This is the general-tree generalisation of the linear
// path fold. Leaf subtrees are moved out of leaves[].slot. Returns false on
// construction failure.
//
// out_keep_b/out_keep_t are the persistent columns in order: [up_col (if parent
// valid)] followed by the GROUP BY columns present in this subtree, in global
// extra_keep order. out_keep_extra_pos is INVALID for up_col and otherwise points
// back into extra_keep.
bool FoldNode(ClientContext &context, Optimizer &optimizer, const vector<vector<TreeAdjEdge>> &adj,
              vector<AggPropLeaf> &leaves, idx_t node, idx_t parent, const vector<AggSpec> &aggs,
              const vector<std::pair<ColumnBinding, LogicalType>> &extra_keep, unique_ptr<LogicalOperator> &out_cte,
              vector<ColumnBinding> &out_keep_b, vector<LogicalType> &out_keep_t,
              vector<idx_t> &out_keep_extra_pos, ColumnBinding &out_freq_b, LogicalType &out_freq_t,
              vector<AggState> &out_states) {
    // ── expression-building helpers ──
    // Two arithmetic modes thread through the fold: DOUBLE (castd/muld) for
    // floating SUM/AVG, and EXACT HUGEINT (casth/mulh) for integer multiplicities
    // (_freq) and integer SUM/COUNT -- a DOUBLE accumulator silently rounds once
    // the running value exceeds 2^53, which a SUM over a multiplicity blowup hits
    // easily, so integer payloads must stay exact.
    auto colref = [](const LogicalType &t, const ColumnBinding &b) {
        return make_uniq<BoundColumnRefExpression>(t, b);
    };
    auto castt = [&](unique_ptr<Expression> e, const LogicalType &t) {
        return BoundCastExpression::AddCastToType(context, std::move(e), t);
    };
    auto castd = [&](unique_ptr<Expression> e) { return castt(std::move(e), LogicalType::DOUBLE); };
    auto casth = [&](unique_ptr<Expression> e) { return castt(std::move(e), LogicalType::HUGEINT); };
    auto muld = [&](unique_ptr<Expression> x, unique_ptr<Expression> y) {
        return optimizer.BindScalarFunction("*", castd(std::move(x)), castd(std::move(y)));
    };
    auto mulh = [&](unique_ptr<Expression> x, unique_ptr<Expression> y) {
        return optimizer.BindScalarFunction("*", casth(std::move(x)), casth(std::move(y)));
    };
    auto aggof = [&](const string &name, unique_ptr<Expression> e) {
        vector<unique_ptr<Expression>> ch;
        ch.push_back(std::move(e));
        return BindAggregateByName(context, name, std::move(ch));
    };
    auto zero_of = [&](const LogicalType &t) -> unique_ptr<Expression> {
        return t.id() == LogicalTypeId::DOUBLE ? make_uniq<BoundConstantExpression>(Value::DOUBLE(0.0))
                                               : make_uniq<BoundConstantExpression>(Value::HUGEINT(0));
    };
    auto one_of = [&](const LogicalType &t) -> unique_ptr<Expression> {
        return t.id() == LogicalTypeId::DOUBLE ? make_uniq<BoundConstantExpression>(Value::DOUBLE(1.0))
                                               : make_uniq<BoundConstantExpression>(Value::HUGEINT(1));
    };
    // CASE WHEN col IS NOT NULL THEN <then_e> ELSE 0, typed `t` (DOUBLE or HUGEINT).
    auto case_notnull_t = [&](const LogicalType &col_t, const ColumnBinding &col_b, unique_ptr<Expression> then_e,
                              const LogicalType &t) {
        auto isnn = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
        isnn->children.push_back(colref(col_t, col_b));
        auto ce = make_uniq<BoundCaseExpression>(t);
        BoundCaseCheck chk;
        chk.when_expr = std::move(isnn);
        chk.then_expr = std::move(then_e);
        ce->case_checks.push_back(std::move(chk));
        ce->else_expr = zero_of(t);
        return ce;
    };

    bool have_up = false;
    vector<ColumnBinding> up_cols;
    vector<LogicalType> up_types;
    vector<const TreeAdjEdge *> children;
    for (auto &e : adj[node]) {
        if (parent != DConstants::INVALID_INDEX && e.nbr == parent) {
            up_cols = e.my_cols;
            up_types = e.my_types;
            have_up = true;
        } else {
            children.push_back(&e);
        }
    }

    unique_ptr<LogicalOperator> rel = std::move(*leaves[node].slot);
    out_states.assign(aggs.size(), AggState());

    struct Keep {
        bool transient_child;
        idx_t child_pos;
        idx_t extra_pos;
        ColumnBinding cur_b;
        LogicalType cur_t;
    };
    vector<Keep> keep;
    if (have_up) {
        for (idx_t i = 0; i < up_cols.size(); i++) {
            keep.push_back({false, 0, DConstants::INVALID_INDEX, up_cols[i], up_types[i]});
        }
    }
    for (idx_t i = 0; i < extra_keep.size(); i++) {
        auto &ek = extra_keep[i];
        if (BindingsContain(leaves[node].bindings, ek.first)) {
            keep.push_back({false, 0, i, ek.first, ek.second});
        }
    }
    auto emit_keep_outputs = [&](const vector<Keep> &items) -> bool {
        out_keep_b.clear();
        out_keep_t.clear();
        out_keep_extra_pos.clear();
        if (have_up) {
            idx_t found_up = 0;
            for (auto &k : items) {
                if (!k.transient_child && k.extra_pos == DConstants::INVALID_INDEX) {
                    out_keep_b.push_back(k.cur_b);
                    out_keep_t.push_back(k.cur_t);
                    out_keep_extra_pos.push_back(DConstants::INVALID_INDEX);
                    found_up++;
                    if (found_up == up_cols.size()) {
                        break;
                    }
                }
            }
            if (found_up != up_cols.size()) {
                return false;
            }
        }
        for (idx_t pos = 0; pos < extra_keep.size(); pos++) {
            for (auto &k : items) {
                if (k.extra_pos == pos) {
                    out_keep_b.push_back(k.cur_b);
                    out_keep_t.push_back(k.cur_t);
                    out_keep_extra_pos.push_back(pos);
                    break;
                }
            }
        }
        return true;
    };

    // ── Leaf (no children): COUNT(*) = frequency, plus initialise any aggregate
    // homed here (its local SUM/MIN/MAX/COUNT/AVG over the leaf rows). ──
    if (children.empty()) {
        auto g = optimizer.binder.GenerateTableIndex();
        auto a = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> aggexprs;
        aggexprs.push_back(BindAggregateByName(context, "count_star", {})); // slot 0 = freq
        vector<std::pair<idx_t, idx_t>> agg_slots(aggs.size(), {DConstants::INVALID_INDEX, DConstants::INVALID_INDEX});
        vector<idx_t> sxx_slots(aggs.size(), DConstants::INVALID_INDEX); // VAR's Sum(x*x) sub-agg
        for (idx_t j = 0; j < aggs.size(); j++) {
            if (aggs[j].is_count_star || aggs[j].home_leaf != node) {
                continue;
            }
            const string &fn = aggs[j].fn;
            // The aggregate accumulates in `at` (HUGEINT for integer SUM/COUNT/AVG,
            // DECIMAL(38,s) for decimal SUM, DOUBLE for floating SUM/AVG). For AVG,
            // BOTH the numerator and the denominator accumulate in `at`; the final
            // root division casts them to DOUBLE -- reproducing native
            // AVG(integer) = (double)int128_sum/(double)count exactly (above 2^53 too).
            LogicalType at = AggAccumType(fn, aggs[j].col_t);
            if (IsSetSafe(fn)) {
                // MIN/MAX/BOOL_AND/BOOL_OR/BIT_AND/BIT_OR: idempotent under multiplicity,
                // so the local per-key aggregate passes straight up with no _freq weight.
                agg_slots[j].first = aggexprs.size();
                aggexprs.push_back(aggof(StringUtil::Lower(fn), colref(aggs[j].col_t, aggs[j].col_b)));
            } else if (fn == "SUM") {
                agg_slots[j].first = aggexprs.size();
                aggexprs.push_back(aggof("sum", castt(colref(aggs[j].col_t, aggs[j].col_b), at)));
            } else if (fn == "COUNT") {
                agg_slots[j].first = aggexprs.size();
                aggexprs.push_back(aggof("sum", case_notnull_t(aggs[j].col_t, aggs[j].col_b, one_of(at), at)));
            } else if (fn == "AVG") {
                agg_slots[j].first = aggexprs.size();
                aggexprs.push_back(aggof("sum", castt(colref(aggs[j].col_t, aggs[j].col_b), at)));
                agg_slots[j].second = aggexprs.size();
                aggexprs.push_back(aggof("sum", case_notnull_t(aggs[j].col_t, aggs[j].col_b, one_of(at), at)));
            } else if (IsVarFamily(fn)) {
                // Three HUGEINT moments at this leaf: Σx (first), n = non-null count
                // (second), Σx² (sxx). Freq weighting is applied by the parent fold.
                agg_slots[j].first = aggexprs.size();
                aggexprs.push_back(aggof("sum", castt(colref(aggs[j].col_t, aggs[j].col_b), at)));
                agg_slots[j].second = aggexprs.size();
                aggexprs.push_back(aggof("sum", case_notnull_t(aggs[j].col_t, aggs[j].col_b, one_of(at), at)));
                sxx_slots[j] = aggexprs.size();
                aggexprs.push_back(
                    aggof("sum", mulh(colref(aggs[j].col_t, aggs[j].col_b), colref(aggs[j].col_t, aggs[j].col_b))));
            }
        }
        auto preagg = make_uniq<LogicalAggregate>(g, a, std::move(aggexprs));
        for (auto &k : keep) {
            preagg->groups.push_back(colref(k.cur_t, k.cur_b));
        }
        preagg->children.push_back(std::move(rel));
        preagg->ResolveOperatorTypes();
        idx_t ng = keep.size();
        for (idx_t i = 0; i < ng; i++) {
            keep[i].cur_b = ColumnBinding(g, i);
            keep[i].cur_t = preagg->types[i];
        }
        out_freq_b = ColumnBinding(a, 0);
        out_freq_t = preagg->types[ng]; // count_star
        for (idx_t j = 0; j < aggs.size(); j++) {
            if (agg_slots[j].first == DConstants::INVALID_INDEX) {
                continue;
            }
            out_states[j].present = true;
            out_states[j].agg_b = ColumnBinding(a, agg_slots[j].first);
            out_states[j].agg_t = preagg->types[ng + agg_slots[j].first];
            if (agg_slots[j].second != DConstants::INVALID_INDEX) {
                out_states[j].has_cnt = true;
                out_states[j].cnt_b = ColumnBinding(a, agg_slots[j].second);
                out_states[j].cnt_t = preagg->types[ng + agg_slots[j].second];
            }
            if (sxx_slots[j] != DConstants::INVALID_INDEX) {
                out_states[j].has_sxx = true;
                out_states[j].sxx_b = ColumnBinding(a, sxx_slots[j]);
                out_states[j].sxx_t = preagg->types[ng + sxx_slots[j]];
            }
        }
        out_cte = std::move(preagg);
        return emit_keep_outputs(keep);
    }

    for (idx_t ci = 0; ci < children.size(); ci++) {
        for (idx_t k = 0; k < children[ci]->my_cols.size(); k++) {
            keep.push_back(
                {true, ci, DConstants::INVALID_INDEX, children[ci]->my_cols[k], children[ci]->my_types[k]});
        }
    }

    bool has_freq = false;
    ColumnBinding cur_freq_b;
    LogicalType cur_freq_t;
    vector<AggState> states(aggs.size()); // accumulated aggregate state at this node
    for (idx_t ci = 0; ci < children.size(); ci++) {
        unique_ptr<LogicalOperator> child_cte;
        vector<ColumnBinding> c_keep_b;
        vector<LogicalType> c_keep_t;
        vector<idx_t> c_keep_extra_pos;
        ColumnBinding c_freq_b;
        LogicalType c_freq_t;
        vector<AggState> child_states;
        if (!FoldNode(context, optimizer, adj, leaves, children[ci]->nbr, node, aggs, extra_keep, child_cte, c_keep_b,
                      c_keep_t, c_keep_extra_pos, c_freq_b, c_freq_t, child_states)) {
            return false;
        }
        idx_t key_count = children[ci]->my_cols.size();
        if (c_keep_b.size() < key_count || c_keep_extra_pos.size() != c_keep_b.size()) {
            return false;
        }
        for (idx_t k = 0; k < key_count; k++) {
            if (c_keep_extra_pos[k] != DConstants::INVALID_INDEX) {
                return false;
            }
        }
        vector<idx_t> kpos;
        for (idx_t k = 0; k < keep.size(); k++) {
            if (keep[k].transient_child && keep[k].child_pos == ci) {
                kpos.push_back(k);
            }
        }
        if (kpos.size() != key_count) {
            return false;
        }
        auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
        for (idx_t k = 0; k < key_count; k++) {
            JoinCondition cond;
            cond.comparison = ExpressionType::COMPARE_EQUAL;
            cond.left = colref(keep[kpos[k]].cur_t, keep[kpos[k]].cur_b);
            cond.right = colref(c_keep_t[k], c_keep_b[k]);
            if (!WidenKeys(context, cond, keep[kpos[k]].cur_t, c_keep_t[k])) {
                return false;
            }
            join->conditions.push_back(std::move(cond));
        }
        join->children.push_back(std::move(rel));
        join->children.push_back(std::move(child_cte));
        join->ResolveOperatorTypes();

        vector<Keep> joined_keep = keep;
        for (idx_t i = key_count; i < c_keep_b.size(); i++) {
            if (c_keep_extra_pos[i] == DConstants::INVALID_INDEX) {
                return false;
            }
            joined_keep.push_back({false, 0, c_keep_extra_pos[i], c_keep_b[i], c_keep_t[i]});
        }

        auto g = optimizer.binder.GenerateTableIndex();
        auto a = optimizer.binder.GenerateTableIndex();
        vector<unique_ptr<Expression>> aggexprs;
        {
            // freq is the running multiplicity (an integer); accumulate it EXACTLY
            // in HUGEINT. The COUNT-only MVP used DOUBLE here, which rounds above
            // 2^53 -- harmless for plausible row COUNTS but wrong for SUM payloads.
            unique_ptr<Expression> freq_expr = has_freq
                                                   ? mulh(colref(cur_freq_t, cur_freq_b), colref(c_freq_t, c_freq_b))
                                                   : casth(colref(c_freq_t, c_freq_b));
            aggexprs.push_back(aggof("sum", std::move(freq_expr))); // slot 0 = freq (HUGEINT)
        }
        // Per-aggregate sub-aggregates (the _agg threading).
        vector<AggState> new_states(aggs.size());
        vector<std::pair<idx_t, idx_t>> agg_slots(aggs.size(), {DConstants::INVALID_INDEX, DConstants::INVALID_INDEX});
        vector<idx_t> sxx_slots(aggs.size(), DConstants::INVALID_INDEX); // VAR's Σx² sub-agg
        for (idx_t j = 0; j < aggs.size(); j++) {
            if (aggs[j].is_count_star) {
                continue; // COUNT(*) reads the frequency directly
            }
            const string &fn = aggs[j].fn;
            bool setsafe = IsSetSafe(fn); // MIN/MAX/BOOL_AND/BOOL_OR/BIT_AND/BIT_OR: pass-through, no freq weight
            bool is_avg = (fn == "AVG");
            bool is_var = IsVarFamily(fn);
            bool varlike = is_avg || is_var; // carry a denominator n (and, for VAR, Σx²)
            // The aggregate value propagates in `at` (HUGEINT integer / DECIMAL(38,s)
            // / DOUBLE). cast_w casts to `at`; mul_w multiplies value*freq in `at`.
            // For HUGEINT/DOUBLE both operands cast to `at`; for DECIMAL only the
            // value casts -- DuckDB resolves DECIMAL(38,s) * <integer freq> ->
            // DECIMAL(38,s) exactly (scaling the freq into the decimal would be wrong).
            LogicalType at = AggAccumType(fn, aggs[j].col_t);
            bool dec_acc = (at.id() == LogicalTypeId::DECIMAL);
            auto cast_w = [&](unique_ptr<Expression> e) { return castt(std::move(e), at); };
            auto mul_w = [&](unique_ptr<Expression> val, unique_ptr<Expression> freq) {
                if (dec_acc) {
                    return optimizer.BindScalarFunction("*", castt(std::move(val), at), std::move(freq));
                }
                return optimizer.BindScalarFunction("*", castt(std::move(val), at), castt(std::move(freq), at));
            };
            bool child_present = child_states[j].present;
            bool node_present = states[j].present;
            bool homed_here = (aggs[j].home_leaf == node);
            // value expr + denominator (for AVG) + the per-edge frequency multiplier.
            unique_ptr<Expression> val, cnt, sxx;
            unique_ptr<Expression> mul_freq; // multiply value by this, or null = no multiply
            if (child_present) {
                // on-path: bring child._agg up, weighted by this node's own freq.
                val = colref(child_states[j].agg_t, child_states[j].agg_b);
                if (varlike && child_states[j].has_cnt) {
                    cnt = colref(child_states[j].cnt_t, child_states[j].cnt_b);
                }
                if (is_var && child_states[j].has_sxx) {
                    sxx = colref(child_states[j].sxx_t, child_states[j].sxx_b);
                }
                if (has_freq && !setsafe) {
                    mul_freq = colref(cur_freq_t, cur_freq_b);
                }
            } else if (node_present) {
                // off-path: this child only adds multiplicity to an already-present agg.
                val = colref(states[j].agg_t, states[j].agg_b);
                if (varlike && states[j].has_cnt) {
                    cnt = colref(states[j].cnt_t, states[j].cnt_b);
                }
                if (is_var && states[j].has_sxx) {
                    sxx = colref(states[j].sxx_t, states[j].sxx_b);
                }
                if (!setsafe) {
                    mul_freq = colref(c_freq_t, c_freq_b);
                }
            } else if (homed_here) {
                // initialise from the base column, weighted by this child's freq.
                if (setsafe) {
                    val = colref(aggs[j].col_t, aggs[j].col_b);
                } else if (fn == "SUM") {
                    val = mul_w(colref(aggs[j].col_t, aggs[j].col_b), colref(c_freq_t, c_freq_b));
                } else if (fn == "COUNT") {
                    val = case_notnull_t(aggs[j].col_t, aggs[j].col_b, casth(colref(c_freq_t, c_freq_b)),
                                         LogicalType::HUGEINT);
                } else { // AVG / VAR -- numerator Σx (val), denominator n (cnt), [Σx² (sxx) for VAR]
                    val = mul_w(colref(aggs[j].col_t, aggs[j].col_b), colref(c_freq_t, c_freq_b));
                    cnt = case_notnull_t(aggs[j].col_t, aggs[j].col_b, cast_w(colref(c_freq_t, c_freq_b)), at);
                    if (is_var) {
                        sxx = mul_w(mulh(colref(aggs[j].col_t, aggs[j].col_b), colref(aggs[j].col_t, aggs[j].col_b)),
                                    colref(c_freq_t, c_freq_b));
                    }
                }
            } else {
                continue; // homed in a not-yet-folded child
            }
            // Apply the multiplier (for non-init sum-like cases) and emit sub-agg(s).
            if (setsafe) {
                agg_slots[j].first = aggexprs.size();
                aggexprs.push_back(aggof(StringUtil::Lower(fn), std::move(val)));
            } else {
                unique_ptr<Expression> term = mul_freq ? mul_w(std::move(val), std::move(mul_freq)) : cast_w(std::move(val));
                agg_slots[j].first = aggexprs.size();
                aggexprs.push_back(aggof("sum", std::move(term)));
                if (varlike) {
                    // denominator n: cnt is already built; weight it identically to
                    // the numerator (same exact-or-DOUBLE mode).
                    unique_ptr<Expression> cterm;
                    if (cnt) {
                        cterm = (child_present && has_freq)
                                    ? mul_w(std::move(cnt), colref(cur_freq_t, cur_freq_b))
                                    : (node_present ? mul_w(std::move(cnt), colref(c_freq_t, c_freq_b))
                                                    : cast_w(std::move(cnt)));
                    } else {
                        cterm = zero_of(at);
                    }
                    agg_slots[j].second = aggexprs.size();
                    aggexprs.push_back(aggof("sum", std::move(cterm)));
                }
                if (is_var) {
                    // Σx² sub-agg: weight identically to the numerator.
                    unique_ptr<Expression> sterm;
                    if (sxx) {
                        sterm = (child_present && has_freq)
                                    ? mul_w(std::move(sxx), colref(cur_freq_t, cur_freq_b))
                                    : (node_present ? mul_w(std::move(sxx), colref(c_freq_t, c_freq_b))
                                                    : cast_w(std::move(sxx)));
                    } else {
                        sterm = zero_of(at);
                    }
                    sxx_slots[j] = aggexprs.size();
                    aggexprs.push_back(aggof("sum", std::move(sterm)));
                }
            }
        }

        auto preagg = make_uniq<LogicalAggregate>(g, a, std::move(aggexprs));
        vector<idx_t> surviving;
        for (idx_t k = 0; k < joined_keep.size(); k++) {
            bool folded_key = false;
            for (auto pos : kpos) {
                if (k == pos) {
                    folded_key = true;
                    break;
                }
            }
            if (!folded_key) {
                preagg->groups.push_back(colref(joined_keep[k].cur_t, joined_keep[k].cur_b));
                surviving.push_back(k);
            }
        }
        preagg->children.push_back(std::move(join));
        preagg->ResolveOperatorTypes();

        idx_t ng = surviving.size();
        vector<Keep> nk;
        for (idx_t pos = 0; pos < ng; pos++) {
            Keep e = joined_keep[surviving[pos]];
            e.cur_b = ColumnBinding(g, pos);
            e.cur_t = preagg->types[pos];
            nk.push_back(e);
        }
        keep = std::move(nk);
        cur_freq_b = ColumnBinding(a, 0);
        cur_freq_t = preagg->types[ng];
        has_freq = true;
        for (idx_t j = 0; j < aggs.size(); j++) {
            if (agg_slots[j].first == DConstants::INVALID_INDEX) {
                continue; // unchanged this fold (still not present, or count_star)
            }
            new_states[j].present = true;
            new_states[j].agg_b = ColumnBinding(a, agg_slots[j].first);
            new_states[j].agg_t = preagg->types[ng + agg_slots[j].first];
            if (agg_slots[j].second != DConstants::INVALID_INDEX) {
                new_states[j].has_cnt = true;
                new_states[j].cnt_b = ColumnBinding(a, agg_slots[j].second);
                new_states[j].cnt_t = preagg->types[ng + agg_slots[j].second];
            }
            if (sxx_slots[j] != DConstants::INVALID_INDEX) {
                new_states[j].has_sxx = true;
                new_states[j].sxx_b = ColumnBinding(a, sxx_slots[j]);
                new_states[j].sxx_t = preagg->types[ng + sxx_slots[j]];
            }
        }
        states = std::move(new_states);
        rel = std::move(preagg);
    }

    out_freq_b = cur_freq_b;
    out_freq_t = cur_freq_t;
    out_states = std::move(states);
    out_cte = std::move(rel);
    return emit_keep_outputs(keep);
}

// Descend single-child LOGICAL_PROJECTION nodes from `slot` to the underlying
// COMPARISON_JOIN (grouped aggregates put a projection chain between the
// aggregate and the join). When `binding` is non-null it is mapped down through
// each projection to the join-output binding space; it must be a bare passthrough
// column ref at every level, except that an integer CompressedMaterialization
// wrapper (__internal_compress_integral) is peeled to its raw column and its
// offset recorded in *out_compress (so the logical rewrite can re-emit the compressed
// value the parent decompress projection expects). Returns the join's owning
// slot, or nullptr if the chain isn't proj*->join or the binding can't be traced
// (computed column, string compress, unextractable offset).
unique_ptr<LogicalOperator> *DescendMappingBindings(unique_ptr<LogicalOperator> *slot,
                                                    const vector<ColumnBinding *> &bindings,
                                                    const vector<CompressInfo *> &compresses) {
    while ((*slot)->type == LogicalOperatorType::LOGICAL_PROJECTION && (*slot)->children.size() == 1) {
        auto &proj = (*slot)->Cast<LogicalProjection>();
        for (idx_t bi = 0; bi < bindings.size(); bi++) {
            ColumnBinding *binding = bindings[bi];
            if (!binding || binding->table_index != proj.table_index) {
                continue;
            }
            idx_t col = binding->column_index;
            if (col >= proj.expressions.size()) {
                return nullptr;
            }
            auto &expr = proj.expressions[col];
            CompressInfo ci = ExtractCompressInfo(**slot, col);
            if (ci.has_compress) {
                if (ci.is_string_compress) {
                    return nullptr; // string compress not handled by the MVP
                }
                if (compresses[bi] && !compresses[bi]->has_compress) {
                    *compresses[bi] = ci; // outermost integer compress wins
                }
                ColumnBinding inner;
                auto &func = expr->Cast<BoundFunctionExpression>();
                if (func.children.empty() || !ExtractBareBinding(*func.children[0], inner)) {
                    return nullptr;
                }
                *binding = inner;
            } else {
                ColumnBinding mapped;
                if (!ExtractBareBinding(*expr, mapped)) {
                    return nullptr;
                }
                *binding = mapped;
            }
        }
        slot = &(*slot)->children[0];
    }
    if ((*slot)->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return nullptr;
    }
    return slot;
}

} // namespace

bool TryRewriteNativeAggPropagation(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                    LogicalAggregate &agg, LogicalComparisonJoin &join, LogicalOperator &agg_child,
                                    AggJoinRewriteState &state, bool has_parent) {
    // ── Gate: aggregates limited to SUM/MIN/MAX/AVG/COUNT(col)/COUNT(*),
    // ungrouped OR GROUP BY one-or-more bare columns that all live on the SAME tree
    // node (the guard). GROUP BY columns spanning different nodes bail (later stage). ──
    if (agg.grouping_sets.size() > 1 || !agg.grouping_functions.empty()) {
        return false;
    }
    bool grouped_query = !agg.groups.empty();
    vector<ColumnBinding> group_bindings;
    if (grouped_query) {
        for (auto &g : agg.groups) {
            ColumnBinding gb;
            if (!ExtractBareBinding(*g, gb)) {
                return false; // GROUP BY a function/expression -> bail
            }
            group_bindings.push_back(gb);
        }
    }
    if (agg.expressions.empty()) {
        return false;
    }
    vector<AggSpec> aggs;
    for (auto &e : agg.expressions) {
        if (e->type != ExpressionType::BOUND_AGGREGATE) {
            return false;
        }
        auto &ba = e->Cast<BoundAggregateExpression>();
        if (ba.IsDistinct() || ba.filter || ba.order_bys) {
            return false;
        }
        string fn = StringUtil::Upper(ba.function.name);
        AggSpec spec;
        spec.result_type = ba.return_type;
        if (fn == "COUNT_STAR") {
            spec.fn = "COUNT_STAR";
            spec.is_count_star = true;
            aggs.push_back(std::move(spec));
            continue;
        }
        // SUM(integer) binds to plain "sum" with a HUGEINT result; the shared
        // IsAggregate gate (aggjoin_rewrites.cpp) excludes HUGEINT, so WalkAndReplace
        // dispatches the agg-propagation logical rewrite BEFORE that gate specifically so integer
        // SUM reaches here -- it is accumulated EXACTLY in HUGEINT (see FoldNode).
        if (fn != "SUM" && fn != "MIN" && fn != "MAX" && fn != "AVG" && fn != "COUNT" && !IsSetSafe(fn) &&
            !IsVarFamily(fn)) {
            return false; // COVAR/CORR (bivariate, below), BIT_XOR, STRING_AGG, etc. -> bail
        }
        if (ba.children.size() != 1) {
            return false; // COVAR/CORR are bivariate (2 args) -> bail here
        }
        // Aggregate input must reach a bare column through AT MOST ONE cast
        // (DuckDB wraps integer SUM inputs in a widening cast). A multi-level or
        // function/arithmetic-wrapped input bails: re-aggregating the underlying
        // column through the logical rewrite's own DOUBLE cast could differ from the
        // user's cast semantics (truncation) or evaluate an unsupported cast that
        // native short-circuits on an empty join (e.g. DATE->DOUBLE).
        Expression *in = ba.children[0].get();
        if (in->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
            in = in->Cast<BoundCastExpression>().child.get();
        }
        if (in->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
            return false;
        }
        spec.fn = fn;
        spec.col_b = in->Cast<BoundColumnRefExpression>().binding; // home/type resolved after mapping
        aggs.push_back(std::move(spec));
    }
    (void)agg_child;

    // ── Descend the projection chain between the aggregate and the join, mapping
    // the GROUP BY column AND each aggregate input column to the join-output
    // binding space (with per-binding compress detection). ──
    vector<CompressInfo> group_compress(group_bindings.size());
    vector<CompressInfo> agg_compress(aggs.size());
    vector<ColumnBinding *> map_bindings;
    vector<CompressInfo *> map_compresses;
    for (idx_t i = 0; i < group_bindings.size(); i++) {
        map_bindings.push_back(&group_bindings[i]);
        map_compresses.push_back(&group_compress[i]);
    }
    for (idx_t j = 0; j < aggs.size(); j++) {
        if (aggs[j].is_count_star) {
            continue;
        }
        map_bindings.push_back(&aggs[j].col_b);
        map_compresses.push_back(&agg_compress[j]);
    }
    unique_ptr<LogicalOperator> *join_slot = DescendMappingBindings(&op->children[0], map_bindings, map_compresses);
    if (!join_slot) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] agg-propagation bail: could not descend aggregate projection chain\n");
        }
        return false;
    }
    // SUM/MIN/etc. over a compressed column would aggregate the compressed value
    // (raw - offset), not the original. Bail; only the GROUP BY column is
    // compress-aware (it is re-emitted, not aggregated).
    for (idx_t j = 0; j < aggs.size(); j++) {
        if (agg_compress[j].has_compress) {
            if (AggJoinTraceEnabled()) {
                fprintf(stderr, "[AGGJOIN] agg-propagation bail: aggregate input is compressed\n");
            }
            return false;
        }
    }

    // ── Phase 1: classify the join tree into leaves + equi-join tree edges ──
    vector<AggPropLeaf> leaves;
    vector<RawJoinPredicate> raw_predicates;
    vector<LoweredJoinPredicate> predicates;
    vector<LeafProjectionInfo> projection_info;
    vector<AggPropEdge> edges;
    CollectJoinGraph(context, *join_slot, leaves, raw_predicates);
    if (!LowerJoinPredicates(context, optimizer, leaves, raw_predicates, group_bindings, aggs, predicates,
                             projection_info)) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] agg-propagation bail: could not lower join predicates (leaves=%zu raw_preds=%zu)\n",
                    leaves.size(), raw_predicates.size());
        }
        return false;
    }
    ComputedKeyProjectionRollback projection_rollback(leaves, projection_info);
    if (!BuildEdgesFromPredicates(leaves, predicates, edges)) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] agg-propagation bail: lowered predicates do not form leaf edges (leaves=%zu preds=%zu)\n",
                    leaves.size(), predicates.size());
        }
        return false;
    }

    if (leaves.size() < 3) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] agg-propagation bail: fewer than 3 leaves (%zu)\n", leaves.size());
        }
        return false; // < 2 joins: leave single-join shapes to the fused/native path
    }

    vector<vector<TreeAdjEdge>> adj;
    if (!BuildAdjacency(leaves, edges, adj)) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] agg-propagation bail: leaf graph is not a connected tree (leaves=%zu edges=%zu)\n",
                    leaves.size(), edges.size());
        }
        return false; // cyclic or disconnected equi-join graph
    }

    // Resolve each aggregate's home leaf (where its input column lives) + type.
    for (idx_t j = 0; j < aggs.size(); j++) {
        if (aggs[j].is_count_star) {
            continue;
        }
        idx_t home = FindLeafForBinding(leaves, 0, leaves.size(), aggs[j].col_b);
        if (home == DConstants::INVALID_INDEX) {
            return false;
        }
        if (!TypeForBinding(leaves[home], aggs[j].col_b, aggs[j].col_t)) {
            return false;
        }
        // SUM/AVG require a numeric column (bails DATE/VARCHAR/etc. whose numeric
        // cast is unsupported). MIN/MAX/COUNT never cast the value, so any orderable
        // column is fine. The accumulator type (AggAccumType) is HUGEINT for integer
        // SUM/AVG, DECIMAL(38,s) for decimal SUM, DOUBLE for floating SUM/AVG -- all
        // exact vs native. The ONE numeric we bail: an UHUGEINT input, whose values
        // above 2^127 the signed-HUGEINT accumulator cannot represent (native
        // SUM(UHUGEINT) keeps them in UHUGEINT) -- a correctness edge, so decline it.
        if (aggs[j].fn == "SUM" || aggs[j].fn == "AVG") {
            if (!aggs[j].col_t.IsNumeric() || aggs[j].col_t.id() == LogicalTypeId::UHUGEINT) {
                return false;
            }
        }
        // VAR/STDDEV use EXACT HUGEINT moments (n·Σx² − (Σx)²). Restrict the INPUT to
        // <= 32-bit integers (x <= ~4.3e9 -> x² <= ~1.8e19) -- a necessary but NOT
        // sufficient overflow bound: n·Σx² <= n²·max(x²), and n (the virtual join
        // multiplicity) is unbounded by this check. The actual overflow guard is the
        // join_est-based bail after the perf gate (n²·max(x²) vs INT128 max). Wider
        // integers, DECIMAL, and FLOAT/DOUBLE bail (they would need a mean-shifted
        // DOUBLE form to avoid cancellation/overflow).
        if (IsVarFamily(aggs[j].fn)) {
            auto pid = aggs[j].col_t.id();
            bool ok32 = pid == LogicalTypeId::TINYINT || pid == LogicalTypeId::SMALLINT ||
                        pid == LogicalTypeId::INTEGER || pid == LogicalTypeId::UTINYINT ||
                        pid == LogicalTypeId::USMALLINT || pid == LogicalTypeId::UINTEGER;
            // Wider integers (BIGINT/UBIGINT/HUGEINT) are admitted ONLY when column
            // statistics are available to bound the actual values — the join_est
            // overflow guard below then uses that REAL bound instead of the type
            // worst-case.  No stats => conservative bail.  FLOAT/DECIMAL still bail
            // (they need the DECIMAL deferred-division moment, not the integer one).
            bool wide_int = pid == LogicalTypeId::BIGINT || pid == LogicalTypeId::UBIGINT ||
                            pid == LogicalTypeId::HUGEINT;
            bool ok_wide = wide_int && ColumnMaxAbsFromStats(context, leaves[home], aggs[j].col_b) >= 0.0;
            if (!ok32 && !ok_wide) {
                return false;
            }
        }
        aggs[j].home_leaf = home;
    }

    // ── Root selection: grouped -> any GROUP BY leaf can act as root; GROUP BY
    // columns on other leaves are threaded up through child keep columns. Ungrouped
    // -> the first aggregate's home (any root is correct -- aggregates propagate
    // to it). ──
    idx_t root = 0;
    vector<LogicalType> group_types;
    if (grouped_query) {
        for (idx_t i = 0; i < group_bindings.size(); i++) {
            idx_t group_leaf = FindLeafForBinding(leaves, 0, leaves.size(), group_bindings[i]);
            if (group_leaf == DConstants::INVALID_INDEX) {
                return false;
            }
            LogicalType gt;
            if (!TypeForBinding(leaves[group_leaf], group_bindings[i], gt)) {
                return false;
            }
            group_types.push_back(gt);
            if (i == 0) {
                root = group_leaf;
            }
        }
    } else {
        for (idx_t j = 0; j < aggs.size(); j++) {
            if (!aggs[j].is_count_star) {
                root = aggs[j].home_leaf;
                break;
            }
        }
    }

    // ── Perf gate: only fire when at least one leaf is large enough that the
    // logical rewrite beats native (mirrors the final-bag envelope). Skippable for
    // benchmarking with -DAGGJOIN_NO_PLANNER_GATE. ──
#ifndef AGGJOIN_NO_PLANNER_GATE
    {
        idx_t max_leaf_est = 0;
        for (auto &leaf : leaves) {
            max_leaf_est = std::max(max_leaf_est, leaf.estimated_cardinality);
        }
        idx_t join_est = join.has_estimated_cardinality ? (idx_t)join.estimated_cardinality : 0;
        bool used_stats_join_est = false;
        idx_t stats_join_est = 0;
        if (EstimateJoinTreeCardinalityFromStats(context, leaves, edges, stats_join_est) && stats_join_est > join_est) {
            join_est = stats_join_est;
            used_stats_join_est = true;
        }
        // The logical rewrite wins only when native must materialise a LARGE intermediate
        // join AND that intermediate is a multiplicity blowup of the inputs.
        // Two independent conditions, both required (calibrated 2026-06-15):
        //  - absolute size: a small intermediate (even at high blowup ratio) is
        //    already cheap for native; the logical rewrite's extra GROUP-BYs then lose.
        //  - blowup ratio: guards against a large 1:1 scan (join_est big but no
        //    multiplicity) where native is already optimal.
        const idx_t MIN_JOIN_EST = 2000000;
        const idx_t BLOWUP_FACTOR = 4;
        bool large_join = join_est >= MIN_JOIN_EST;
        bool blowup = join_est >= max_leaf_est * BLOWUP_FACTOR;
        bool has_composite_edge = false;
        bool has_derived_single_key_edge = false;
        bool has_derived_composite_edge = false;
        bool saw_composite_key_stats = false;
        bool composite_duplicate_density = false;
        for (auto &edge : edges) {
            bool edge_has_derived_key = false;
            auto *leaf_a_op = leaves[edge.leaf_a].slot ? leaves[edge.leaf_a].slot->get() : nullptr;
            auto *leaf_b_op = leaves[edge.leaf_b].slot ? leaves[edge.leaf_b].slot->get() : nullptr;
            for (auto &col : edge.cols_a) {
                edge_has_derived_key =
                    edge_has_derived_key || (leaf_a_op && BindingIsDerivedProjectionKey(*leaf_a_op, col));
            }
            for (auto &col : edge.cols_b) {
                edge_has_derived_key =
                    edge_has_derived_key || (leaf_b_op && BindingIsDerivedProjectionKey(*leaf_b_op, col));
            }
            if (edge_has_derived_key) {
                if (edge.cols_a.size() > 1) {
                    has_derived_composite_edge = true;
                } else {
                    has_derived_single_key_edge = true;
                }
            }
            if (edge.cols_a.size() <= 1) {
                continue;
            }
            has_composite_edge = true;
            idx_t domain_a = 0;
            if (leaf_a_op &&
                AggJoinTryGetCompositeKeyDomainFromStats(context, *leaf_a_op, edge.cols_a, edge.types_a, domain_a)) {
                saw_composite_key_stats = true;
                composite_duplicate_density =
                    composite_duplicate_density ||
                    AggJoinAtLeastRatio(leaves[edge.leaf_a].estimated_cardinality, domain_a, 2, 1);
            }
            idx_t domain_b = 0;
            if (leaf_b_op &&
                AggJoinTryGetCompositeKeyDomainFromStats(context, *leaf_b_op, edge.cols_b, edge.types_b, domain_b)) {
                saw_composite_key_stats = true;
                composite_duplicate_density =
                    composite_duplicate_density ||
                    AggJoinAtLeastRatio(leaves[edge.leaf_b].estimated_cardinality, domain_b, 2, 1);
            }
        }
        bool composite_stats_reject =
            has_composite_edge && saw_composite_key_stats && !composite_duplicate_density;
        const idx_t DERIVED_SINGLE_KEY_BLOWUP_FACTOR = 256;
        bool derived_single_key_reject =
            has_derived_single_key_edge && !has_derived_composite_edge &&
            !AggJoinAtLeastRatio(join_est, max_leaf_est, DERIVED_SINGLE_KEY_BLOWUP_FACTOR, 1);
        if (AggJoinTraceEnabled()) {
            fprintf(stderr,
                    "[AGGJOIN] agg-propagation gate: leaves=%zu max_leaf_est=%llu join_est=%llu stats_join_est=%d large_join=%d blowup=%d composite_stats_reject=%d derived_single_key_reject=%d\n",
                    leaves.size(), (unsigned long long)max_leaf_est, (unsigned long long)join_est,
                    (int)used_stats_join_est, (int)large_join, (int)blowup, (int)composite_stats_reject,
                    (int)derived_single_key_reject);
        }
        if (!large_join || !blowup || composite_stats_reject || derived_single_key_reject) {
            return false;
        }
    }
#endif

    // ── VAR/STDDEV overflow guard (ALWAYS active, even when the planner gate is
    // compiled out). The exact-moment numerator n·Σx² is formed in INT128/HUGEINT.
    // Σx² <= n·max(x²), so n·Σx² <= n²·max(x²); the column type caps max|x| but n is
    // the UNBOUNDED virtual join multiplicity. A huge blowup with a large x would
    // overflow INT128 (1.7e38) and FAIL the query where native returns a finite
    // value -- so bail VAR to native when the estimated join could approach that.
    // Threshold carries a 100x margin for join-estimate under-prediction:
    // bail when join_est > 1.3e17 / max|x|  (so actual n stays < ~6e9 for INT32). ──
    {
        double n_est = join.has_estimated_cardinality ? (double)join.estimated_cardinality : 0.0;
        for (auto &spec : aggs) {
            if (!IsVarFamily(spec.fn)) {
                continue;
            }
            double max_abs;
            switch (spec.col_t.id()) {
            case LogicalTypeId::TINYINT:
                max_abs = 127.0;
                break;
            case LogicalTypeId::SMALLINT:
                max_abs = 32767.0;
                break;
            case LogicalTypeId::INTEGER:
                max_abs = 2147483647.0;
                break;
            case LogicalTypeId::UTINYINT:
                max_abs = 255.0;
                break;
            case LogicalTypeId::USMALLINT:
                max_abs = 65535.0;
                break;
            case LogicalTypeId::UINTEGER:
                max_abs = 4294967295.0;
                break;
            default:
                // BIGINT/UBIGINT/HUGEINT — admitted at the gate only WITH stats; use
                // the actual value bound (re-read; cheap, plan-time) instead of the
                // type worst-case, so wide columns whose values are small can fire.
                max_abs = ColumnMaxAbsFromStats(context, leaves[spec.home_leaf], spec.col_b);
                if (max_abs < 0.0) {
                    return false; // stats no longer resolvable — be safe
                }
                max_abs = std::max(max_abs, 1.0); // avoid div-by-zero (constant col => VAR 0)
                break;
            }
            if (n_est > 1.3e17 / max_abs) {
                return false; // VAR over a blowup this large risks INT128 overflow -> native
            }
        }
    }
    projection_rollback.Commit();

    // ── Phase 2: fold the whole tree leaf-to-root, moving leaf subtrees out. ──
    // Every GROUP BY column rides through every fold as a persistent kept column,
    // in agg.groups order (so keep_b[i] is group column i at the root).
    vector<std::pair<ColumnBinding, LogicalType>> extra_keep;
    for (idx_t i = 0; i < group_bindings.size(); i++) {
        extra_keep.push_back({group_bindings[i], group_types[i]});
    }
    unique_ptr<LogicalOperator> cte;
    vector<ColumnBinding> keep_b;
    vector<LogicalType> keep_t;
    ColumnBinding freq_b;
    LogicalType freq_t;
    vector<AggState> root_states;
    vector<idx_t> keep_extra_pos;
    if (!FoldNode(context, optimizer, adj, leaves, root, DConstants::INVALID_INDEX, aggs, extra_keep, cte, keep_b,
                  keep_t, keep_extra_pos, freq_b, freq_t, root_states)) {
        return false;
    }
    for (idx_t j = 0; j < aggs.size(); j++) {
        if (!aggs[j].is_count_star && !root_states[j].present) {
            return false; // aggregate failed to propagate to the root (should not happen)
        }
    }

    // ── Final projection: [group (when grouped),] one combined value per aggregate. ──
    auto colref = [](const LogicalType &t, const ColumnBinding &b) {
        return make_uniq<BoundColumnRefExpression>(t, b);
    };
    auto castd = [&](unique_ptr<Expression> e) {
        return BoundCastExpression::AddCastToType(context, std::move(e), LogicalType::DOUBLE);
    };
    // COALESCE(e, 0) typed `t` -- the freq/COUNT accumulators are HUGEINT, so the
    // zero literal and the COALESCE result type must match (a DOUBLE zero would
    // coerce the exact HUGEINT count back to a lossy DOUBLE).
    auto coalesce_zero = [&](unique_ptr<Expression> e, const LogicalType &t) {
        auto c = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, t);
        c->children.push_back(std::move(e));
        c->children.push_back(t.id() == LogicalTypeId::DOUBLE ? make_uniq<BoundConstantExpression>(Value::DOUBLE(0.0))
                                                              : make_uniq<BoundConstantExpression>(Value::HUGEINT(0)));
        return c;
    };

    auto proj_index = optimizer.binder.GenerateTableIndex();
    vector<unique_ptr<Expression>> proj_exprs;
    idx_t group_offset = 0;
    if (grouped_query) {
        if (keep_b.size() != group_bindings.size() || keep_extra_pos.size() != group_bindings.size()) {
            return false;
        }
        for (idx_t i = 0; i < keep_extra_pos.size(); i++) {
            if (keep_extra_pos[i] != i) {
                return false;
            }
        }
        // Emit each GROUP BY column (keep_b[i]) in agg.groups order, with per-column
        // compress re-emit. agg.types[i] is the i-th group output's type.
        for (idx_t i = 0; i < group_bindings.size(); i++) {
            auto group_ref = colref(keep_t[i], keep_b[i]); // raw guard value
            unique_ptr<Expression> group_out;
            if (group_compress[i].has_compress) {
                // Re-emit the compressed value -- compress(x) = CAST(x - offset). The
                // parent decompress projection (c + offset) restores the original.
                auto raw_big = BoundCastExpression::AddCastToType(context, std::move(group_ref), LogicalType::BIGINT);
                auto off = make_uniq<BoundConstantExpression>(Value::BIGINT(group_compress[i].offset));
                auto sub = optimizer.BindScalarFunction("-", std::move(raw_big), std::move(off));
                group_out = BoundCastExpression::AddCastToType(context, std::move(sub), agg.types[i]);
            } else {
                group_out = BoundCastExpression::AddCastToType(context, std::move(group_ref), agg.types[i]);
            }
            proj_exprs.push_back(std::move(group_out));
        }
        group_offset = group_bindings.size();
    }
    for (idx_t j = 0; j < aggs.size(); j++) {
        const AggSpec &spec = aggs[j];
        unique_ptr<Expression> out;
        if (spec.is_count_star) {
            // COUNT(*) = SUM(_freq), empty -> 0.
            out = BoundCastExpression::AddCastToType(context, coalesce_zero(colref(freq_t, freq_b), freq_t),
                                                     spec.result_type);
        } else {
            const AggState &st = root_states[j];
            if (spec.fn == "COUNT") {
                out = BoundCastExpression::AddCastToType(context, coalesce_zero(colref(st.agg_t, st.agg_b), st.agg_t),
                                                         spec.result_type);
            } else if (spec.fn == "SUM" || IsSetSafe(spec.fn)) {
                // SUM: empty -> NULL (no COALESCE). MIN/MAX/BOOL/BIT: set-safe
                // pass-through (empty -> NULL, matching native).
                out = BoundCastExpression::AddCastToType(context, colref(st.agg_t, st.agg_b), spec.result_type);
            } else if (spec.fn == "AVG") { // CASE WHEN cnt <> 0 THEN (double)sum/(double)cnt ELSE NULL
                if (!st.has_cnt) {
                    return false;
                }
                // The denominator is HUGEINT for integer AVG, DOUBLE for floating
                // AVG; the bound comparison's zero literal must match its type (bound
                // expressions are not auto-coerced). The numerator+denominator were
                // accumulated exactly; casting both to DOUBLE only at the division
                // reproduces native AVG(int) = (double)sum/(double)count.
                auto cnt_zero = st.cnt_t.id() == LogicalTypeId::DOUBLE
                                    ? make_uniq<BoundConstantExpression>(Value::DOUBLE(0.0))
                                    : make_uniq<BoundConstantExpression>(Value::HUGEINT(0));
                auto when = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL,
                                                                 colref(st.cnt_t, st.cnt_b), std::move(cnt_zero));
                auto ratio = optimizer.BindScalarFunction("/", castd(colref(st.agg_t, st.agg_b)),
                                                          castd(colref(st.cnt_t, st.cnt_b)));
                auto ce = make_uniq<BoundCaseExpression>(LogicalType::DOUBLE);
                BoundCaseCheck chk;
                chk.when_expr = std::move(when);
                chk.then_expr = std::move(ratio);
                ce->case_checks.push_back(std::move(chk));
                ce->else_expr = make_uniq<BoundConstantExpression>(Value(LogicalType::DOUBLE)); // NULL
                out = BoundCastExpression::AddCastToType(context, std::move(ce), spec.result_type);
            } else { // VAR_POP / VAR_SAMP / VARIANCE / STDDEV_* -- EXACT HUGEINT moments
                if (!st.has_cnt || !st.has_sxx) {
                    return false;
                }
                bool sample = IsVarSample(spec.fn);
                bool want_stddev = IsStddev(spec.fn);
                auto sx = [&]() { return colref(st.agg_t, st.agg_b); };  // Σx (HUGEINT)
                auto nn = [&]() { return colref(st.cnt_t, st.cnt_b); };  // n  (HUGEINT)
                auto sxxc = [&]() { return colref(st.sxx_t, st.sxx_b); }; // Σx² (HUGEINT)
                // Exact integer numerator: n·Σx² − (Σx)²  (a non-negative HUGEINT).
                auto n_sxx = optimizer.BindScalarFunction("*", nn(), sxxc());
                auto sx_sx = optimizer.BindScalarFunction("*", sx(), sx());
                auto num = optimizer.BindScalarFunction("-", std::move(n_sxx), std::move(sx_sx));
                // Denominator: n² (pop) or n·(n−1) (samp).
                unique_ptr<Expression> denom;
                if (sample) {
                    auto n_m1 =
                        optimizer.BindScalarFunction("-", nn(), make_uniq<BoundConstantExpression>(Value::HUGEINT(1)));
                    denom = optimizer.BindScalarFunction("*", nn(), std::move(n_m1));
                } else {
                    denom = optimizer.BindScalarFunction("*", nn(), nn());
                }
                // var = (double)num / (double)denom; stddev = sqrt(greatest(var, 0)).
                unique_ptr<Expression> var = optimizer.BindScalarFunction("/", castd(std::move(num)), castd(std::move(denom)));
                unique_ptr<Expression> body;
                if (want_stddev) {
                    auto g0 = optimizer.BindScalarFunction("greatest", std::move(var),
                                                           make_uniq<BoundConstantExpression>(Value::DOUBLE(0.0)));
                    body = optimizer.BindScalarFunction("sqrt", std::move(g0));
                } else {
                    body = std::move(var);
                }
                // Empty/single guards: pop needs n>0, samp needs n>1, else NULL.
                auto thresh = make_uniq<BoundConstantExpression>(Value::HUGEINT(sample ? 1 : 0));
                auto when = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, nn(), std::move(thresh));
                auto ce = make_uniq<BoundCaseExpression>(LogicalType::DOUBLE);
                BoundCaseCheck chk;
                chk.when_expr = std::move(when);
                chk.then_expr = std::move(body);
                ce->case_checks.push_back(std::move(chk));
                ce->else_expr = make_uniq<BoundConstantExpression>(Value(LogicalType::DOUBLE)); // NULL
                out = BoundCastExpression::AddCastToType(context, std::move(ce), spec.result_type);
            }
        }
        proj_exprs.push_back(std::move(out));
    }

    auto final_proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_exprs));
    final_proj->children.push_back(std::move(cte));
    final_proj->ResolveOperatorTypes();
    if (op->has_estimated_cardinality) {
        final_proj->SetEstimatedCardinality(op->estimated_cardinality);
    }

    if (has_parent) {
        for (idx_t i = 0; i < group_bindings.size(); i++) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.group_index, i), ColumnBinding(proj_index, i));
        }
        for (idx_t j = 0; j < aggs.size(); j++) {
            state.replacement_bindings.emplace_back(ColumnBinding(agg.aggregate_index, j),
                                                    ColumnBinding(proj_index, group_offset + j));
        }
    }
    if (AggJoinTraceEnabled()) {
        fprintf(stderr, "[AGGJOIN] planner rewrite: native agg-propagation logical rewrite (leaves=%zu, aggs=%zu)\n",
                leaves.size(), aggs.size());
    }
    SetAggJoinLastRewrite("agg_propagation");
    op = std::move(final_proj);
    return true;
}

} // namespace duckdb
