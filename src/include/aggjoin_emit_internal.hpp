#pragma once

#include "aggjoin_physical.hpp"

namespace duckdb {

bool TryEmitDirectLikeResult(const PhysicalAggJoin &op, DataChunk &chunk, AggJoinSinkState &sink,
                             AggJoinSourceState &src, SourceResultType &result);

} // namespace duckdb
