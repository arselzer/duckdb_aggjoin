#pragma once

#include "aggjoin_state.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

#ifndef AGGJOIN_GETDATA
#if __has_include("duckdb/main/extension_callback_manager.hpp")
#define HAS_CALLBACK_MANAGER 1
#else
#define HAS_CALLBACK_MANAGER 0
#endif

#if HAS_CALLBACK_MANAGER
#define AGGJOIN_GETDATA GetDataInternal
#else
#define AGGJOIN_GETDATA GetData
#endif
#endif

namespace duckdb {

class PhysicalAggJoin : public CachingPhysicalOperator {
public:
    AggJoinColInfo col;
    vector<unique_ptr<Expression>> owned_agg_exprs;
    vector<LogicalType> group_types;
    vector<LogicalType> payload_types;

    PhysicalAggJoin(PhysicalPlan &plan, vector<LogicalType> types, idx_t estimated_cardinality);

    bool IsSink() const override { return true; }
    bool ParallelSink() const override { return false; }
    bool IsSource() const override { return true; }
    vector<const_reference<PhysicalOperator>> GetSources() const override { return {*this}; }

    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &ctx) const override;
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &ctx) const override;
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &ctx) const override;
    SinkResultType Sink(ExecutionContext &ctx, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &p, Event &e, ClientContext &c, OperatorSinkFinalizeInput &input) const override;
    OperatorResultType ExecuteInternal(ExecutionContext &ctx, DataChunk &input, DataChunk &chunk,
                                       GlobalOperatorState &gstate, OperatorState &state) const override;
    SourceResultType AGGJOIN_GETDATA(ExecutionContext &ctx, DataChunk &chunk, OperatorSourceInput &input) const override;
    void BuildPipelines(Pipeline &cur, MetaPipeline &mp) override;
    string GetName() const override;
};

} // namespace duckdb
