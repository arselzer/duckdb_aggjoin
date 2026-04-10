#pragma once

#include "aggjoin_physical.hpp"
#include "aggjoin_runtime.hpp"
#include "aggjoin_state.hpp"

namespace duckdb {

bool TryExecuteDirectSourcePath(const PhysicalAggJoin &op, DataChunk &input, DataChunk &chunk, AggJoinSinkState &sink,
                                idx_t n, idx_t na);
bool TryExecuteSegmentedSourcePath(const PhysicalAggJoin &op, DataChunk &input, DataChunk &chunk, AggJoinSinkState &sink,
                                   idx_t n, idx_t na);
OperatorResultType ExecuteResultHashSourcePath(const PhysicalAggJoin &op, ExecutionContext &ctx, DataChunk &input,
                                               DataChunk &chunk, AggJoinSinkState &sink, idx_t n, idx_t na,
                                               bool same_keys, hash_t *h);

} // namespace duckdb
