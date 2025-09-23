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
    auto* scanNodesOutNodeIDs = addColumnInBuffer<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs()->getBuffer());

    // GetOutEdges1
    GetOutEdgesProcessor* getOutEdges1 = GetOutEdgesProcessor::create(&pipeline);
    auto* indices1 = addColumnInBuffer<ColumnIndices>(&mem, getOutEdges1->outIndices()->getBuffer());
    auto* targetNodes1 = addColumnInBuffer<ColumnNodeIDs>(&mem, getOutEdges1->outTargetNodes()->getBuffer());
    scanNodes->outNodeIDs()->connectTo(getOutEdges1->inNodeIDs());

    // GetOutEdges2
    GetOutEdgesProcessor* getOutEdges2 = GetOutEdgesProcessor::create(&pipeline);
    auto* indices2 = addColumnInBuffer<ColumnIndices>(&mem, getOutEdges2->outIndices()->getBuffer());
    auto* targetNodes2 = addColumnInBuffer<ColumnNodeIDs>(&mem, getOutEdges2->outTargetNodes()->getBuffer());
    getOutEdges1->outTargetNodes()->connectTo(getOutEdges2->inNodeIDs());

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    getOutEdges2->outTargetNodes()->connectTo(materialize->input());

    // Fill up materialize data
    MaterializeData& matData = materialize->getMaterializeData();
    matData.addToStep(scanNodesOutNodeIDs);
    matData.createStep(indices1);
    matData.addToStep(targetNodes1);
    matData.createStep(indices2);
    matData.addToStep(targetNodes2);

    // Lambda
    auto callback = [](const Block& block, LambdaProcessor::Operation operation) {
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    materialize->output()->connectTo(lambda->input());

    // Execute pipeline
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}
