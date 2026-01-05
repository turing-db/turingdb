#include <gtest/gtest.h>

#include <math.h>

#include "SystemManager.h"
#include "Graph.h"
#include "TuringDB.h"

#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "SimpleGraph.h"
#include "LocalMemory.h"

#include "columns/ColumnIDs.h"

#include "dataframe/Dataframe.h"
#include "dataframe/ColumnTagManager.h"
#include "dataframe/NamedColumn.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"
#include "reader/GraphReader.h"
#include "PipelineBuilder.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"
#include "processors/MaterializeProcessor.h"

#include "LineContainer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class PipelineTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

template <typename ColumnType>
NamedColumn* addColumnInDataframe(LocalMemory* mem, Dataframe* df, ColumnTag tag) {
    ColumnType* col = mem->alloc<ColumnType>();
    return NamedColumn::create(df, col, tag);
}

TEST_F(PipelineTest, emptyPipeline) {
    LocalMemory mem;
    PipelineV2 pipeline;

    auto view = _graph->openTransaction().viewGraph();

    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodes) {
    LocalMemory mem;
    PipelineV2 pipeline;

    PipelineBuilder builder(&mem, &pipeline);

    builder.setMaterializeProc(MaterializeProcessor::create(&pipeline, &mem));
    PipelineNodeOutputInterface& scanNodesOut = builder.addScanNodes();
    const ColumnTag scanOutNodeIDsTag = scanNodesOut.getNodeIDs()->getTag();

    builder.addMaterialize();

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

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 1);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());
        EXPECT_EQ(nodeIDs->getRaw(), allNodeIDs);
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodesExpand1) {
    LocalMemory mem;
    PipelineV2 pipeline;

    PipelineBuilder builder(&mem, &pipeline);

    builder.setMaterializeProc(MaterializeProcessor::create(&pipeline, &mem));
    PipelineNodeOutputInterface& scanNodesOut = builder.addScanNodes();
    const ColumnTag scanOutNodeIDsTag = scanNodesOut.getNodeIDs()->getTag();

    PipelineEdgeOutputInterface& getOutEdgesOut = builder.addGetOutEdges();
    const ColumnTag getOutEdgesOutTargetNodesTag = getOutEdgesOut.getOtherNodes()->getTag();
    
    builder.addMaterialize();

    // Lambda

    // Get all expected node IDs
    LineContainer<NodeID, NodeID> expected;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const auto edgeView = reader.getNodeView(node).edges();
            for (auto edge : edgeView.outEdges()) {
                expected.add({node, edge._otherID});
            }
        }
    }

    LineContainer<NodeID, NodeID> returned;
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 4);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        const ColumnNodeIDs* targetNodes = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(getOutEdgesOutTargetNodesTag)->getColumn());
        ASSERT_TRUE(targetNodes != nullptr);
        ASSERT_TRUE(!targetNodes->empty());

        ASSERT_EQ(nodeIDs->size(), targetNodes->size());
        const size_t size = nodeIDs->size();
        for (size_t i = 0; i < size; i++) {
            returned.add({nodeIDs->at(i), targetNodes->at(i)});
        }
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();

    ASSERT_TRUE(returned.equals(expected));
}

TEST_F(PipelineTest, scanNodesExpand2) {
    LocalMemory mem;
    PipelineV2 pipeline;

    PipelineBuilder builder(&mem, &pipeline);

    builder.setMaterializeProc(MaterializeProcessor::create(&pipeline, &mem));
    PipelineNodeOutputInterface& scanNodesOut = builder.addScanNodes();
    const ColumnTag scanOutNodeIDsTag = scanNodesOut.getNodeIDs()->getTag();

    PipelineEdgeOutputInterface& getOutEdges1Out = builder.addGetOutEdges();
    const ColumnTag getOutEdges1OutTargetNodesTag = getOutEdges1Out.getOtherNodes()->getTag();
    
    PipelineEdgeOutputInterface& getOutEdges2Out = builder.addGetOutEdges();
    const ColumnTag getOutEdges2OutTargetNodesTag = getOutEdges2Out.getOtherNodes()->getTag();
    
    builder.addMaterialize();

    // Lambda
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 7);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        const ColumnNodeIDs* targetNodes1 = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(getOutEdges1OutTargetNodesTag)->getColumn());
        ASSERT_TRUE(targetNodes1 != nullptr);
        ASSERT_TRUE(!targetNodes1->empty());

        const ColumnNodeIDs* targetNodes2 = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(getOutEdges2OutTargetNodesTag)->getColumn());
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

        for (NodeID nodeID : allNodeIDs) {
            SimpleGraph::findOutEdges(_graph, {nodeID}, tmpEdgeIDs, tmpEdgeTypes, tmpTargets);
            for (NodeID targetNodeID : tmpTargets) {
                SimpleGraph::findOutEdges(_graph, {targetNodeID}, tmpEdgeIDs2, tmpEdgeTypes2, tmpTargets2);
                for (NodeID targetNodeID2 : tmpTargets2) {
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

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodesLimit) {
    LocalMemory mem;
    PipelineV2 pipeline;

    PipelineBuilder builder(&mem, &pipeline);

    builder.setMaterializeProc(MaterializeProcessor::create(&pipeline, &mem));
    builder.addScanNodes();
    builder.addMaterialize();

    // Limit
    constexpr size_t limitCount = 4;
    builder.addLimit(limitCount);

    // Lambda
    std::vector<NodeID> resultNodeIds;
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 1);

        const NamedColumn* limitOut = df->cols().front();
        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(limitOut->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        resultNodeIds.insert(resultNodeIds.end(), nodeIDs->getRaw().begin(), nodeIDs->getRaw().end());
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();

    // Check returned node IDs
    std::vector<NodeID> expectedNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        size_t i = 0;
        for (auto node : nodes) {
            if (i < limitCount) {
                expectedNodeIDs.push_back(node);
            }
            i++;
        }
    }
    EXPECT_EQ(resultNodeIds, expectedNodeIDs);
}

TEST_F(PipelineTest, scanNodesSkip) {
    LocalMemory mem;
    PipelineV2 pipeline;

    PipelineBuilder builder(&mem, &pipeline);
    builder.setMaterializeProc(MaterializeProcessor::create(&pipeline, &mem));
    builder.addScanNodes();
    builder.addMaterialize();

    // Skip
    constexpr size_t skipCount = 4;
    builder.addSkip(skipCount);

    // Lambda
    std::vector<NodeID> resultNodeIds;
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        ASSERT_EQ(df->size(), 1);
        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->cols().front()->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        resultNodeIds.insert(resultNodeIds.end(), nodeIDs->getRaw().begin(), nodeIDs->getRaw().end());
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();

    // Check returned node IDs
    std::vector<NodeID> expectedNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        size_t i = 0;
        for (auto node : nodes) {
            if (i >= skipCount) {
                expectedNodeIDs.push_back(node);
            }
            i++;
        }
    }

    EXPECT_EQ(resultNodeIds, expectedNodeIDs);
}

TEST_F(PipelineTest, scanNodesCount) {
    LocalMemory mem;
    PipelineV2 pipeline;

    PipelineBuilder builder(&mem, &pipeline);
    builder.setMaterializeProc(MaterializeProcessor::create(&pipeline, &mem));
    builder.addScanNodes();
    builder.addMaterialize();
    builder.addCount();

    // Lambda
    size_t expectedCount = 0;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        expectedCount = reader.getTotalNodesAllocated();
    }

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 1);

        const ColumnConst<size_t>* count = dynamic_cast<const ColumnConst<size_t>*>(df->cols().front()->getColumn());
        ASSERT_TRUE(count != nullptr);

        EXPECT_TRUE(count->getRaw() != 0);
        EXPECT_EQ(count->getRaw(), expectedCount);
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, multiChunkCount) {
    LocalMemory mem;
    PipelineV2 pipeline;
    PipelineBuilder builder(&mem, &pipeline);

    // Create a source of 100 chunks
    size_t currentChunk = 1;
    constexpr size_t chunkCount = 1000;
    constexpr size_t chunkSize = 100;
    auto sourceCallback = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), 1);

        ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);

        nodeIDs->resize(chunkSize);
        std::iota(nodeIDs->begin(), nodeIDs->end(), 0);

        if (currentChunk == chunkCount) {
            isFinished = true;
        }

        currentChunk++;
    };

    builder.addLambdaSource(sourceCallback);
    builder.addColumnToOutput<ColumnNodeIDs>(pipeline.getDataframeManager()->allocTag());

    // Count
    builder.addCount();
    
    // Lambda
    size_t returnedCount = 0;
    size_t lambdaSinkExecutions = 0;
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 1);
        const ColumnConst<size_t>* count = dynamic_cast<const ColumnConst<size_t>*>(df->cols().front()->getColumn());
        ASSERT_TRUE(count != nullptr);
        returnedCount = count->getRaw();

        lambdaSinkExecutions += 1;
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
    
    // Check value of returned count
    EXPECT_EQ(returnedCount, chunkCount * chunkSize);
    EXPECT_EQ(lambdaSinkExecutions, 1);
}

TEST_F(PipelineTest, abcOverwrite) {
    LocalMemory mem;
    PipelineV2 pipeline;
    PipelineBuilder builder(&mem, &pipeline);

    const auto sourceATag = pipeline.getDataframeManager()->allocTag();
    const auto transBOutTag = pipeline.getDataframeManager()->allocTag();
    const auto transCOutTag = pipeline.getDataframeManager()->allocTag();

    NamedColumn* sourceAOut = nullptr;
    NamedColumn* transBOut = nullptr;
    NamedColumn* transCOut = nullptr;

    const size_t maxChunkA = 2;
    size_t chunkCountA = 0;
    auto sourceA = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if(operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ColumnConst<size_t>* value = df->getColumn<ColumnConst<size_t>>(sourceATag);
        value->set(10+chunkCountA);

        if (chunkCountA == maxChunkA) {
            isFinished = true;
        }

        chunkCountA++;
    };

    size_t invocationCountB = 0;
    auto transformB = [&](const Dataframe* src, Dataframe* dest, bool& isFinished, auto operation) -> void {
        if(operation != LambdaTransformProcessor::Operation::EXECUTE) {
            return;
        }

        isFinished = true;

        ColumnConst<size_t>* value = dest->getColumn<ColumnConst<size_t>>(transBOutTag);
        ASSERT_TRUE(value != nullptr);
        value->set(20+invocationCountB);
        invocationCountB++;
    };

    const size_t maxChunkC = 1;
    size_t chunkCountC = 0;
    auto transformC = [&](const Dataframe* src, Dataframe* dest, bool& isFinished, auto operation) -> void {
        if (operation == LambdaTransformProcessor::Operation::RESET) {
            chunkCountC = 0;
        }

        if(operation != LambdaTransformProcessor::Operation::EXECUTE) {
            return;
        }

        ColumnConst<size_t>* value = dest->getColumn<ColumnConst<size_t>>(transCOutTag);
        ASSERT_TRUE(value != nullptr);
        value->set(30+chunkCountC);

        if (chunkCountC == maxChunkC) {
            isFinished = true;
        }

        chunkCountC++;
    };

    std::vector<std::tuple<size_t, size_t, size_t>> returnedLines;
    bool sinkExecuted = false;
    auto sink = [&](const Dataframe* df, auto operation) -> void {
        if (operation != LambdaProcessor::Operation::EXECUTE) {
            return;
        }

        sinkExecuted = true;

        const ColumnConst<size_t>* a = sourceAOut->as<ColumnConst<size_t>>();
        const ColumnConst<size_t>* b = transBOut->as<ColumnConst<size_t>>();
        const ColumnConst<size_t>* c = transCOut->as<ColumnConst<size_t>>();
        ASSERT_TRUE(a != nullptr);
        ASSERT_TRUE(b != nullptr);
        ASSERT_TRUE(c != nullptr);

        returnedLines.emplace_back(a->getRaw(), b->getRaw(), c->getRaw());
    };

    builder.addLambdaSource(sourceA);
    sourceAOut = builder.addColumnToOutput<ColumnConst<size_t>>(sourceATag);

    builder.addLambdaTransform(transformB);
    transBOut = builder.addColumnToOutput<ColumnConst<size_t>>(transBOutTag);

    builder.addLambdaTransform(transformC);
    transCOut = builder.addColumnToOutput<ColumnConst<size_t>>(transCOutTag);

    builder.addLambda(sink);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(&_env->getSystemManager(), view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();

    ASSERT_TRUE(sinkExecuted);

    std::vector<std::tuple<size_t, size_t, size_t>> expectedLines = {
        {10, 20, 30},
        {10, 20, 31},
        {11, 21, 30},
        {11, 21, 31},
        {12, 22, 30},
        {12, 22, 31} 
    };

    ASSERT_EQ(returnedLines, expectedLines);
}
