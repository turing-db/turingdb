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
#include "reader/GraphReader.h"

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

TEST_F(PipelineTest, emptyPipeline) {
    LocalMemory mem;
    PipelineV2 pipeline;

    auto view = _graph->openTransaction().viewGraph();

    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodes) {
    LocalMemory mem;
    PipelineV2 pipeline;

    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    auto* scanNodesOutNodeIDs = addColumnInBuffer<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs()->getBuffer());

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    scanNodes->outNodeIDs()->connectTo(materialize->input());

    // Fill up materialize data
    MaterializeData& matData = materialize->getMaterializeData();
    matData.addToStep(scanNodesOutNodeIDs);

    // Lambda

    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    auto callback = [&](const Block& block, LambdaProcessor::Operation operation) {
        EXPECT_EQ(block.size(), 1);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(block[0]);
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        EXPECT_EQ(nodeIDs->getRaw(), allNodeIDs);
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    materialize->output()->connectTo(lambda->input());

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodesExpand2) {
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
    auto callback = [&](const Block& block, LambdaProcessor::Operation operation) {
        EXPECT_EQ(block.size(), 3);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(block[0]);
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        const ColumnNodeIDs* targetNodes1 = dynamic_cast<const ColumnNodeIDs*>(block[1]);
        ASSERT_TRUE(targetNodes1 != nullptr);
        ASSERT_TRUE(!targetNodes1->empty());

        const ColumnNodeIDs* targetNodes2 = dynamic_cast<const ColumnNodeIDs*>(block[2]);
        ASSERT_TRUE(targetNodes2 != nullptr);
        ASSERT_TRUE(!targetNodes2->empty());

        // Compare block against nested loop line-by-line implementation
        std::vector<EdgeID> tmpEdgeIDs;
        std::vector<EdgeTypeID> tmpEdgeTypes;
        std::vector<NodeID> tmpTargets;
        std::vector<EdgeID> tmpEdgeIDs2;
        std::vector<EdgeTypeID> tmpEdgeTypes2;
        std::vector<NodeID> tmpTargets2;
        std::vector<NodeID> expectedNodeIDs;
        std::vector<NodeID> expectedTargets1;
        std::vector<NodeID> expectedTargets2;
        for (NodeID nodeID : nodeIDs->getRaw()) {
            SimpleGraph::findOutEdges(_graph, {nodeID}, tmpEdgeIDs, tmpEdgeTypes, tmpTargets);
            for (NodeID targetNodeID : tmpTargets) {
                SimpleGraph::findOutEdges(_graph, {targetNodeID}, tmpEdgeIDs2, tmpEdgeTypes2, tmpTargets2);
                for (NodeID targetNodeID2 : tmpTargets2) {
                    std::cout << nodeID << "->" << targetNodeID << "->" << targetNodeID2 << std::endl;
                    expectedNodeIDs.push_back(nodeID);
                    expectedTargets1.push_back(targetNodeID);
                    expectedTargets2.push_back(targetNodeID2);
                }
            }
        }

        EXPECT_EQ(nodeIDs->getRaw(), expectedNodeIDs);
        EXPECT_EQ(targetNodes1->getRaw(), expectedTargets1);
        EXPECT_EQ(targetNodes2->getRaw(), expectedTargets2);
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    materialize->output()->connectTo(lambda->input());

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}
