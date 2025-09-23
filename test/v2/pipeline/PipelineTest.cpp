#include <gtest/gtest.h>

#include "TuringConfig.h"
#include "SystemManager.h"
#include "Graph.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "SimpleGraph.h"
#include "LocalMemory.h"

#include "columns/ColumnIDs.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/MaterializeData.h"
#include "processors/LambdaProcessor.h"

#include "PipelineExecutor.h"
#include "ExecutionContext.h"

using namespace db;
using namespace db::v2;

class PipelineTest : public ::testing::Test {
public:
    PipelineTest()
        : _sysMan(_config)
    {
    }

    void SetUp() override {
        _graph = _sysMan.createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }

    void TearDown() override {
    }

protected:
    TuringConfig _config;
    SystemManager _sysMan;
    Graph* _graph {nullptr};
};

template <typename ColumnType>
ColumnType* addColumnInBuffer(LocalMemory* mem, PipelineBuffer* buffer) {
    auto* col = mem->alloc<ColumnType>();
    buffer->getBlock().addColumn(col);
    return col;
}

TEST_F(PipelineTest, scanNodes) {
    LocalMemory mem;
    PipelineV2 pipeline;

    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);

    // GetOutEdges1
    GetOutEdgesProcessor* getOutEdges1 = GetOutEdgesProcessor::create(&pipeline);
    PipelineBuffer* inNodeIDsBuffer = PipelineBuffer::create(&pipeline);
    auto* inNodeIDs = addColumnInBuffer<ColumnNodeIDs>(&mem, inNodeIDsBuffer);
    scanNodes->outNodeIDs()->connectTo(getOutEdges1->inNodeIDs(), inNodeIDsBuffer);

    // GetOutEdges2
    GetOutEdgesProcessor* getOutEdges2 = GetOutEdgesProcessor::create(&pipeline);
    PipelineBuffer* targetNodes1Buffer = PipelineBuffer::create(&pipeline);
    PipelineBuffer* indices1Buffer = PipelineBuffer::create(&pipeline);
    auto* indices1 = addColumnInBuffer<ColumnIndices>(&mem, indices1Buffer);
    auto* targetNodes1 = addColumnInBuffer<ColumnNodeIDs>(&mem, targetNodes1Buffer);
    getOutEdges1->outTargetNodes()->connectTo(getOutEdges2->inNodeIDs(), targetNodes1Buffer);

    // Materialize
    MaterializeData matData(&mem);

    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &matData);
    PipelineBuffer* targetNodes2Buffer = PipelineBuffer::create(&pipeline);
    PipelineBuffer* indices2Buffer = PipelineBuffer::create(&pipeline);
    auto* indices2 = addColumnInBuffer<ColumnIndices>(&mem, indices2Buffer);
    auto* targetNodes2 = addColumnInBuffer<ColumnNodeIDs>(&mem, targetNodes2Buffer);
    getOutEdges2->outTargetNodes()->connectTo(materialize->input(), targetNodes2Buffer);

    matData.addToStep(inNodeIDs);
    matData.createStep(indices1);
    matData.addToStep(targetNodes1);
    matData.createStep(indices2);
    matData.addToStep(targetNodes2);

    auto callback = [](const Block& block, LambdaProcessor::Operation operation) {
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    {
        PipelineBuffer* outputBuffer = PipelineBuffer::create(&pipeline);
        materialize->output()->connectTo(lambda->input(), outputBuffer);
    }

    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}
