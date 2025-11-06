#include <gtest/gtest.h>

#include <math.h>

#include "PipelineInterface.h"
#include "SystemManager.h"
#include "Graph.h"
#include "TuringDB.h"

#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "SimpleGraph.h"
#include "LocalMemory.h"

#include "iterators/ChunkConfig.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnIndices.h"

#include "dataframe/Dataframe.h"
#include "dataframe/ColumnTagManager.h"
#include "dataframe/NamedColumn.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"
#include "reader/GraphReader.h"
#include "PipelineBuilder.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"

#include "PipelineException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

namespace {

void getProperties(Graph* graph,
                   PropertyType propType,
                   ColumnNodeIDs* nodes,
                   ColumnVector<types::String::Primitive>* properties,
                   ColumnIndices* indices) {
    GetNodePropertiesChunkWriter<types::String> writer(graph->openTransaction().viewGraph(), propType._id, nodes);
    writer.setOutput(properties);
    writer.setIndices(indices);
    writer.fill(ChunkConfig::CHUNK_SIZE);
    ASSERT_FALSE(writer.isValid());
}

void getAllNodes(Graph* graph, ColumnNodeIDs* nodes) {
    ScanNodesChunkWriter writer(graph->openTransaction().viewGraph());
    writer.setNodeIDs(nodes);
    writer.fill(ChunkConfig::CHUNK_SIZE);
    ASSERT_FALSE(writer.isValid());
}

void generateGetOutEdges(Graph* graph,
                         ColumnNodeIDs* nodes,
                         ColumnNodeIDs* targets,
                         ColumnIndices* indices) {
    GetOutEdgesChunkWriter writer(graph->openTransaction().viewGraph(), nodes);
    writer.setTgtIDs(targets);
    writer.setIndices(indices);
    writer.fill(ChunkConfig::CHUNK_SIZE);
    ASSERT_FALSE(writer.isValid());
}

}

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
    return NamedColumn::create(df, col, ColumnHeader(tag));
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
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);
    
    PipelineNodeOutputInterface& scanNodesOut = builder.addScanNodes();
    const ColumnTag scanOutNodeIDsTag = scanNodesOut.getNodeIDs()->getHeader().getTag();

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

    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodesExpand1) {
    LocalMemory mem;
    PipelineV2 pipeline;
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);
    
    PipelineNodeOutputInterface& scanNodesOut = builder.addScanNodes();
    const ColumnTag scanOutNodeIDsTag = scanNodesOut.getNodeIDs()->getHeader().getTag();

    PipelineEdgeOutputInterface& getOutEdgesOut = builder.addGetOutEdges();
    const ColumnTag getOutEdgesOutTargetNodesTag = getOutEdgesOut.getTargetNodes()->getHeader().getTag();
    
    builder.addMaterialize();

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

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 4);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        const ColumnNodeIDs* targetNodes = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(getOutEdgesOutTargetNodesTag)->getColumn());
        ASSERT_TRUE(targetNodes != nullptr);
        ASSERT_TRUE(!targetNodes->empty());

        // Compare block against nested loop line-by-line implementation
        std::vector<NodeID> expectedNodeIDs;
        std::vector<NodeID> expectedTargets;
        std::vector<EdgeID> tmpEdgeIDs;
        std::vector<EdgeTypeID> tmpEdgeTypes;
        std::vector<NodeID> tmpTargets;

        for (NodeID nodeID : allNodeIDs) {
            SimpleGraph::findOutEdges(_graph, {nodeID}, tmpEdgeIDs, tmpEdgeTypes, tmpTargets);
            for (NodeID targetNodeID : tmpTargets) {
                expectedNodeIDs.push_back(nodeID);
                expectedTargets.push_back(targetNodeID);
            }
        }

        EXPECT_EQ(nodeIDs->getRaw(), expectedNodeIDs);
        EXPECT_EQ(targetNodes->getRaw(), expectedTargets);
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodesExpandGetProperties) {
    LocalMemory mem;
    PipelineV2 pipeline;
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);
    
    PipelineNodeOutputInterface& scanNodesOut = builder.addScanNodes();
    const ColumnTag scanOutNodeIDsTag = scanNodesOut.getNodeIDs()->getHeader().getTag();

    builder.addGetOutEdges();

    // Get properties
    const PropertyType namePropType = _graph->openTransaction().readGraph().getMetadata().propTypes().get("name").value();

    PipelineValuesOutputInterface& getNodePropertiesOut = builder.addGetNodeProperties<types::String>(namePropType);
    const ColumnTag getNodePropertiesOutValuesTag = getNodePropertiesOut.getValues()->getHeader().getTag();

    builder.addMaterialize();

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

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 5);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        const ColumnVector<types::String::Primitive>* properties = dynamic_cast<const ColumnVector<types::String::Primitive>*>(df->getColumn(getNodePropertiesOutValuesTag)->getColumn());
        ASSERT_TRUE(properties != nullptr);
        ASSERT_TRUE(!properties->empty());
        
        ColumnNodeIDs expectedNodeIDs;
        ColumnIndices expectedEdgesIndices;
        ColumnNodeIDs expectedTargets;
        ColumnIndices expectedPropertiesIndices;
        ColumnVector<types::String::Primitive> expectedProperties;
        getAllNodes(_graph, &expectedNodeIDs);
        generateGetOutEdges(_graph, &expectedNodeIDs, &expectedTargets, &expectedEdgesIndices);
        getProperties(_graph, namePropType, &expectedTargets, &expectedProperties, &expectedPropertiesIndices);

        EXPECT_EQ(properties->getRaw(), expectedProperties.getRaw());
    };

    builder.addLambda(callback);

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
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);

    PipelineNodeOutputInterface& scanNodesOut = builder.addScanNodes();
    const ColumnTag scanOutNodeIDsTag = scanNodesOut.getNodeIDs()->getHeader().getTag();
    
    PipelineEdgeOutputInterface& getOutEdges1Out = builder.addGetOutEdges();
    const ColumnTag getOutEdges1OutTargetNodesTag = getOutEdges1Out.getTargetNodes()->getHeader().getTag();
    
    PipelineEdgeOutputInterface& getOutEdges2Out = builder.addGetOutEdges();
    const ColumnTag getOutEdges2OutTargetNodesTag = getOutEdges2Out.getTargetNodes()->getHeader().getTag();
    
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

    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, scanNodesLimit) {
    LocalMemory mem;
    PipelineV2 pipeline;
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);

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
    ExecutionContext execCtxt(view);
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
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);
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
    ExecutionContext execCtxt(view);
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
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);
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

    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, multiChunkCount) {
    LocalMemory mem;
    PipelineV2 pipeline;
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);

    // Create a source of 100 chunks
    size_t currentChunk = 0;
    constexpr size_t chunkCount = 1000;
    constexpr size_t chunkSize = ChunkConfig::CHUNK_SIZE;
    auto sourceCallback = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), 1);

        ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);

        nodeIDs->resize(chunkSize);
        std::iota(nodeIDs->begin(), nodeIDs->end(), 0);

        currentChunk++;
        if (currentChunk == chunkCount) {
            isFinished = true;
        }
    };

    builder.addLambdaSource(sourceCallback);
    builder.addColumnToOutput<ColumnNodeIDs>(tagMan.allocTag());

    // Count
    builder.addCount();
    
    // Lambda
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        EXPECT_EQ(df->size(), 1);
        const ColumnConst<size_t>* count = dynamic_cast<const ColumnConst<size_t>*>(df->cols().front()->getColumn());
        ASSERT_TRUE(count != nullptr);
        EXPECT_EQ(count->getRaw(), chunkCount * chunkSize);
    };

    builder.addLambda(callback);

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}

TEST_F(PipelineTest, testLambdaWithoutMaterialize) {
    LocalMemory mem;
    PipelineV2 pipeline;
    ColumnTagManager tagMan;

    PipelineBuilder builder(&mem, &pipeline, &tagMan);
    builder.addScanNodes();

    EXPECT_THROW(builder.addLambda([](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
    }), PipelineException);
}
