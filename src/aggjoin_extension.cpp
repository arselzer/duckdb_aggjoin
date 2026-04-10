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

static void AggjoinSetTestHashBitsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<int64_t, int64_t>(args.data[0], result, args.size(), [&](int64_t bits) {
        SetAggJoinTestHashBits(bits);
        return bits;
    });
}

static void RegisterAggJoinTestFunctions(ExtensionLoader &loader) {
    loader.RegisterFunction(
        ScalarFunction("aggjoin_set_test_hash_bits", {LogicalType::BIGINT}, LogicalType::BIGINT,
                       AggjoinSetTestHashBitsFunction));
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
