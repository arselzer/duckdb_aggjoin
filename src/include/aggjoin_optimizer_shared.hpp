#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

// Resolve a group/aggregate-child expression to its position in the immediate
// child operator's output bindings. Handles the two cases DuckDB actually
// emits at post-optimize: BoundReferenceExpression (direct index) and
// BoundColumnRefExpression (binding lookup). Returns INVALID_INDEX for
// anything else — callers MUST bail rather than treat INVALID as COUNT(*),
// or they'll silently produce 0 for SUM(a*b), SUM(CAST(x)), etc.
static inline idx_t ResolveChildBinding(Expression &e, const vector<ColumnBinding> &child_bindings) {
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
}

class PhysicalPlanGenerator;
class ColumnBindingResolver;
class PhysicalOperator;

bool AggJoinTraceEnabled();
bool AggJoinTraceStatsEnabled();
bool AggJoinStaticDisabledByEnv();

// Per-path enable/disable (logical rewrites = native-lowering preaggregation
// rewrites; operator = the fused PhysicalAggJoin). See aggjoin_optimizer.cpp.
bool AggJoinLogicalRewritesEnabled();
bool AggJoinCascadeEnabled();
bool AggJoinOperatorEnabled();
void SetAggJoinLogicalRewritesEnabled(bool enabled);
void SetAggJoinCascadeEnabled(bool enabled);
void SetAggJoinOperatorEnabled(bool enabled);

// Last-fired rewrite marker (test introspection — see aggjoin_optimizer.cpp).
const char *GetAggJoinLastRewrite();
void SetAggJoinLastRewrite(const char *marker);
void ResetAggJoinLastRewrite();

int64_t GetAggJoinTestHashBits();
void SetAggJoinTestHashBits(int64_t bits);
int64_t GetAggJoinTestHTCapacity();
void SetAggJoinTestHTCapacity(int64_t capacity);

struct AggJoinRewriteState {
    vector<ReplacementBinding> replacement_bindings;
};

struct CompressInfo {
    bool has_compress = false;
    bool is_string_compress = false;
    int64_t offset = 0;
    LogicalType compressed_type;
    LogicalType original_type;
};

struct AggJoinColInfo {
    vector<idx_t> probe_key_cols;
    vector<idx_t> build_key_cols;
    vector<idx_t> group_cols;
    vector<idx_t> agg_input_cols;
    vector<string> agg_funcs;
    vector<bool> agg_on_build;
    vector<bool> agg_is_numeric;
    vector<idx_t> build_agg_cols;
    idx_t probe_col_count = 0;
    idx_t probe_estimate = 0;
    idx_t build_estimate = 0;
    idx_t group_estimate = 0;
    bool planned_direct_mode = false;
    int64_t planned_direct_key_min = 0;
    idx_t planned_direct_key_range = 0;
    vector<CompressInfo> group_compress;
};

class LogicalAggJoin : public LogicalExtensionOperator {
public:
    vector<LogicalType> return_types;
    idx_t group_index = 0;
    idx_t aggregate_index = 0;
    AggJoinColInfo col;
    vector<unique_ptr<Expression>> agg_expressions;
    vector<unique_ptr<Expression>> group_expressions;

    vector<ColumnBinding> GetColumnBindings() override;
    PhysicalOperator &CreatePlan(ClientContext &ctx, PhysicalPlanGenerator &planner) override;
    void ResolveTypes() override;

protected:
    void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) override;
};

PhysicalOperator &CreatePhysicalAggJoinPlan(LogicalAggJoin &op, ClientContext &ctx, PhysicalPlanGenerator &planner);

CompressInfo ExtractCompressInfo(LogicalOperator &op, idx_t idx);
CompressInfo FindCompressInChain(LogicalOperator &op, idx_t idx);
idx_t TraceProjectionChain(LogicalOperator &op, idx_t idx, int depth = 0);

void WalkAndReplace(ClientContext &context, Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                    AggJoinRewriteState &state, bool has_parent);
void StripDecompressProjections(unique_ptr<LogicalOperator> &op);

} // namespace duckdb
