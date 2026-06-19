/*
 * aggjoin DuckDB extension
 *
 * Registers the AggJoin optimizer extension which automatically detects
 * Aggregate(Join) patterns in query plans and replaces them with a fused
 * PhysicalAggJoin operator. No table functions — the optimizer fires
 * transparently on standard SQL.
 */

#define DUCKDB_EXTENSION_MAIN
#include "aggjoin_extension.hpp"

#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Forward declaration from aggjoin_optimizer.cpp
void RegisterAggJoinOptimizer(DatabaseInstance &db, bool ignore_disable_static = false);
void SetAggJoinTestHashBits(int64_t bits);
void SetAggJoinTestHTCapacity(int64_t capacity);
void SetAggJoinLogicalRewritesEnabled(bool enabled);
void SetAggJoinOperatorEnabled(bool enabled);
const char *GetAggJoinLastRewrite();
void ResetAggJoinLastRewrite();

// Returns which AggJoin rewrite path last fired ("chain_count", "final_bag",
// "native_build", "native_mixed", "fused", or "none"). The native-lowering
// rewrites are invisible in EXPLAIN, so this is the fire-assertion mechanism for
// sqllogictest. Reset the marker, run the query, then read it.
static void AggjoinLastRewriteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
    ConstantVector::GetData<string_t>(result)[0] = StringVector::AddString(result, GetAggJoinLastRewrite());
}

static void AggjoinResetRewriteMarkerFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    ResetAggJoinLastRewrite();
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
    ConstantVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "none");
}

static void AggjoinSetTestHashBitsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<int64_t, int64_t>(args.data[0], result, args.size(), [&](int64_t bits) {
        SetAggJoinTestHashBits(bits);
        return bits;
    });
}

static void AggjoinSetTestHTCapacityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<int64_t, int64_t>(args.data[0], result, args.size(), [&](int64_t capacity) {
        SetAggJoinTestHTCapacity(capacity);
        return capacity;
    });
}

static void AggjoinSetLogicalRewritesEnabledFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<bool, bool>(args.data[0], result, args.size(), [&](bool enabled) {
        SetAggJoinLogicalRewritesEnabled(enabled);
        return enabled;
    });
}

static void AggjoinSetOperatorEnabledFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<bool, bool>(args.data[0], result, args.size(), [&](bool enabled) {
        SetAggJoinOperatorEnabled(enabled);
        return enabled;
    });
}

static void RegisterAggJoinTestFunctions(ExtensionLoader &loader) {
    loader.RegisterFunction(
        ScalarFunction("aggjoin_set_test_hash_bits", {LogicalType::BIGINT}, LogicalType::BIGINT,
                       AggjoinSetTestHashBitsFunction));
    loader.RegisterFunction(
        ScalarFunction("aggjoin_set_test_ht_capacity", {LogicalType::BIGINT}, LogicalType::BIGINT,
                       AggjoinSetTestHTCapacityFunction));
    // Per-path enable/disable (logical rewrites vs. fused operator), runtime-settable.
    loader.RegisterFunction(
        ScalarFunction("aggjoin_set_logical_rewrites_enabled", {LogicalType::BOOLEAN}, LogicalType::BOOLEAN,
                       AggjoinSetLogicalRewritesEnabledFunction));
    // Compatibility alias for older tests/scripts.
    loader.RegisterFunction(
        ScalarFunction("aggjoin_set_cascade_enabled", {LogicalType::BOOLEAN}, LogicalType::BOOLEAN,
                       AggjoinSetLogicalRewritesEnabledFunction));
    loader.RegisterFunction(
        ScalarFunction("aggjoin_set_operator_enabled", {LogicalType::BOOLEAN}, LogicalType::BOOLEAN,
                       AggjoinSetOperatorEnabledFunction));
    // Fire-assertion hooks: nullary, VOLATILE so they always evaluate at runtime
    // (never constant-folded) and reflect the marker set during the prior query.
    ScalarFunction last_rewrite("aggjoin_last_rewrite", {}, LogicalType::VARCHAR, AggjoinLastRewriteFunction);
    last_rewrite.stability = FunctionStability::VOLATILE;
    loader.RegisterFunction(last_rewrite);
    ScalarFunction reset_marker("aggjoin_reset_rewrite_marker", {}, LogicalType::VARCHAR,
                                AggjoinResetRewriteMarkerFunction);
    reset_marker.stability = FunctionStability::VOLATILE;
    loader.RegisterFunction(reset_marker);
}

void AggjoinExtension::Load(ExtensionLoader &loader) {
    RegisterAggJoinOptimizer(loader.GetDatabaseInstance(), false);
    RegisterAggJoinTestFunctions(loader);
}

} // namespace duckdb

extern "C" DUCKDB_EXTENSION_API void aggjoin_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
    duckdb::RegisterAggJoinOptimizer(loader.GetDatabaseInstance(), true);
    duckdb::RegisterAggJoinTestFunctions(loader);
}
