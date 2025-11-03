#include <gtest/gtest.h>

#include <math.h>

#include "SystemManager.h"
#include "Graph.h"
#include "TuringDB.h"
#include "iterators/GetPropertiesIterator.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "SimpleGraph.h"
#include "LocalMemory.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"

#include "dataframe/Dataframe.h"
#include "dataframe/ColumnTagManager.h"
#include "dataframe/NamedColumn.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/MaterializeData.h"
#include "processors/LambdaProcessor.h"
#include "processors/GetPropertiesProcessor.h"
#include "processors/LimitProcessor.h"
#include "processors/SkipProcessor.h"
#include "processors/CountProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "reader/GraphReader.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "PipelineExecutor.h"
#include "ExecutionContext.h"

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

    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    const ColumnTag scanOutNodeIDsTag = tagMan.allocTag();
    NamedColumn* scanNodesOutNodeIDs = addColumnInDataframe<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs().getDataframe(), scanOutNodeIDsTag);
    scanNodes->outNodeIDs().setNodeIDs(static_cast<ColumnNodeIDs*>(scanNodesOutNodeIDs->getColumn()));

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    scanNodes->outNodeIDs().connectTo(materialize->input());

    // Fill up materialize data
    MaterializeData& matData = materialize->getMaterializeData();
    matData.addToStep<ColumnNodeIDs>(scanNodesOutNodeIDs);

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

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 1);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());
        EXPECT_EQ(nodeIDs->getRaw(), allNodeIDs);
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    materialize->output().connectTo(lambda->input());

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

    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    const ColumnTag scanOutNodeIDsTag = tagMan.allocTag();
    NamedColumn* scanNodesOutNodeIDs = addColumnInDataframe<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs().getDataframe(), scanOutNodeIDsTag);
    scanNodes->outNodeIDs().setNodeIDs(static_cast<ColumnNodeIDs*>(scanNodesOutNodeIDs->getColumn()));

    GetOutEdgesProcessor* getOutEdges = GetOutEdgesProcessor::create(&pipeline);
    const ColumnTag getOutEdgesOutIndicesTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutIndices = addColumnInDataframe<ColumnIndices>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutIndicesTag);
    getOutEdges->outEdges().setIndices(static_cast<ColumnVector<size_t>*>(getOutEdgesOutIndices->getColumn()));

    const ColumnTag getOutEdgesOutEdgeIDsTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutEdgeIDs = addColumnInDataframe<ColumnEdgeIDs>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutEdgeIDsTag);

    const ColumnTag getOutEdgesOutEdgeTypesTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutEdgeTypes = addColumnInDataframe<ColumnEdgeTypes>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutEdgeTypesTag);

    const ColumnTag getOutEdgesOutTargetNodesTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutTargetNodes = addColumnInDataframe<ColumnNodeIDs>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutTargetNodesTag);
    getOutEdges->outEdges().setEdges(static_cast<ColumnEdgeIDs*>(getOutEdgesOutEdgeIDs->getColumn()), 
                                     static_cast<ColumnNodeIDs*>(getOutEdgesOutTargetNodes->getColumn()), 
                                     static_cast<ColumnEdgeTypes*>(getOutEdgesOutEdgeTypes->getColumn()));

    scanNodes->outNodeIDs().connectTo(getOutEdges->inNodeIDs());

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    getOutEdges->outEdges().connectTo(materialize->input());

    // Fill up materialize data
    MaterializeData& matData = materialize->getMaterializeData();
    matData.addToStep<ColumnNodeIDs>(scanNodesOutNodeIDs);
    matData.createStep(getOutEdgesOutIndices);
    matData.addToStep<ColumnNodeIDs>(getOutEdgesOutTargetNodes);

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

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 2);

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

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    materialize->output().connectTo(lambda->input());

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

    // Scan nodes
    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    const ColumnTag scanNodesOutNodeIDsTag = tagMan.allocTag();
    NamedColumn* scanNodesOutNodeIDs = addColumnInDataframe<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs().getDataframe(), scanNodesOutNodeIDsTag);
    scanNodes->outNodeIDs().setNodeIDs(static_cast<ColumnNodeIDs*>(scanNodesOutNodeIDs->getColumn()));

    // Get out edges
    GetOutEdgesProcessor* getOutEdges = GetOutEdgesProcessor::create(&pipeline);
    const ColumnTag getOutEdgesOutIndicesTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutIndices = addColumnInDataframe<ColumnIndices>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutIndicesTag);
    getOutEdges->outEdges().setIndices(static_cast<ColumnVector<size_t>*>(getOutEdgesOutIndices->getColumn()));

    const ColumnTag getOutEdgesOutTargetNodesTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutTargetNodes = addColumnInDataframe<ColumnNodeIDs>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutTargetNodesTag);

    const ColumnTag getOutEdgesOutEdgeIDsTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutEdgeIDs = addColumnInDataframe<ColumnEdgeIDs>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutEdgeIDsTag);

    const ColumnTag getOutEdgesOutEdgeTypesTag = tagMan.allocTag();
    NamedColumn* getOutEdgesOutEdgeTypes = addColumnInDataframe<ColumnEdgeTypes>(&mem, getOutEdges->outEdges().getDataframe(), getOutEdgesOutEdgeTypesTag);
    getOutEdges->outEdges().setEdges(static_cast<ColumnEdgeIDs*>(getOutEdgesOutEdgeIDs->getColumn()), 
                                     static_cast<ColumnNodeIDs*>(getOutEdgesOutTargetNodes->getColumn()), 
                                     static_cast<ColumnEdgeTypes*>(getOutEdgesOutEdgeTypes->getColumn()));

    scanNodes->outNodeIDs().connectTo(getOutEdges->inNodeIDs());

    // Get properties
    const PropertyType namePropType = _graph->openTransaction().readGraph().getMetadata().propTypes().get("name").value();

    GetNodePropertiesStringProcessor* getNodeProperties = GetNodePropertiesStringProcessor::create(&pipeline, namePropType);
    const ColumnTag getNodePropertiesOutIndicesTag = tagMan.allocTag();
    NamedColumn* getNodePropertiesOutIndices = addColumnInDataframe<ColumnIndices>(&mem, getNodeProperties->outValues().getDataframe(), getNodePropertiesOutIndicesTag);
    getNodeProperties->outValues().setIndices(static_cast<ColumnIndices*>(getNodePropertiesOutIndices->getColumn()));

    const ColumnTag getNodePropertiesOutValuesTag = tagMan.allocTag();
    NamedColumn* getNodePropertiesOutValues = addColumnInDataframe<ColumnVector<types::String::Primitive>>(&mem, getNodeProperties->outValues().getDataframe(), getNodePropertiesOutValuesTag);
    getNodeProperties->outValues().setValues(static_cast<ColumnVector<types::String::Primitive>*>(getNodePropertiesOutValues->getColumn()));

    getOutEdges->outEdges().connectTo(getNodeProperties->inIDs());

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    getNodeProperties->outValues().connectTo(materialize->input());
    {
        MaterializeData& matData = materialize->getMaterializeData();
        matData.addToStep<ColumnNodeIDs>(scanNodesOutNodeIDs);
        matData.createStep(getOutEdgesOutIndices);
        matData.addToStep<ColumnNodeIDs>(getOutEdgesOutTargetNodes);
        matData.createStep(getNodePropertiesOutIndices);
        matData.addToStep<ColumnVector<types::String::Primitive>>(getNodePropertiesOutValues);
    }

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

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 3);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanNodesOutNodeIDsTag)->getColumn());
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

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    materialize->output().connectTo(lambda->input());

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

    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    const ColumnTag scanNodesOutNodeIDsTag = tagMan.allocTag();
    NamedColumn* scanNodesOutNodeIDs = addColumnInDataframe<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs().getDataframe(), scanNodesOutNodeIDsTag);
    scanNodes->outNodeIDs().setNodeIDs(static_cast<ColumnNodeIDs*>(scanNodesOutNodeIDs->getColumn()));

    // GetOutEdges1
    GetOutEdgesProcessor* getOutEdges1 = GetOutEdgesProcessor::create(&pipeline);

    const ColumnTag getOutEdges1OutIndicesTag = tagMan.allocTag();
    NamedColumn* getOutEdges1OutIndices = addColumnInDataframe<ColumnIndices>(&mem, getOutEdges1->outEdges().getDataframe(), getOutEdges1OutIndicesTag);
    getOutEdges1->outEdges().setIndices(static_cast<ColumnVector<size_t>*>(getOutEdges1OutIndices->getColumn()));

    const ColumnTag getOutEdges1OutEdgeIDsTag = tagMan.allocTag();
    NamedColumn* getOutEdges1OutEdgeIDs = addColumnInDataframe<ColumnEdgeIDs>(&mem, getOutEdges1->outEdges().getDataframe(), getOutEdges1OutEdgeIDsTag);

    const ColumnTag getOutEdges1OutEdgeTypesTag = tagMan.allocTag();
    NamedColumn* getOutEdges1OutEdgeTypes = addColumnInDataframe<ColumnEdgeTypes>(&mem, getOutEdges1->outEdges().getDataframe(), getOutEdges1OutEdgeTypesTag);

    const ColumnTag getOutEdges1OutTargetNodesTag = tagMan.allocTag();
    NamedColumn* getOutEdges1OutTargetNodes = addColumnInDataframe<ColumnNodeIDs>(&mem, getOutEdges1->outEdges().getDataframe(), getOutEdges1OutTargetNodesTag);
    getOutEdges1->outEdges().setEdges(static_cast<ColumnEdgeIDs*>(getOutEdges1OutEdgeIDs->getColumn()), 
                                      static_cast<ColumnNodeIDs*>(getOutEdges1OutTargetNodes->getColumn()), 
                                      static_cast<ColumnEdgeTypes*>(getOutEdges1OutEdgeTypes->getColumn()));
    scanNodes->outNodeIDs().connectTo(getOutEdges1->inNodeIDs());

    // GetOutEdges2
    GetOutEdgesProcessor* getOutEdges2 = GetOutEdgesProcessor::create(&pipeline);
    const ColumnTag getOutEdges2OutIndicesTag = tagMan.allocTag();
    NamedColumn* getOutEdges2OutIndices = addColumnInDataframe<ColumnIndices>(&mem, getOutEdges2->outEdges().getDataframe(), getOutEdges2OutIndicesTag);
    getOutEdges2->outEdges().setIndices(static_cast<ColumnVector<size_t>*>(getOutEdges2OutIndices->getColumn()));

    const ColumnTag getOutEdges2OutTargetNodesTag = tagMan.allocTag();
    NamedColumn* getOutEdges2OutTargetNodes = addColumnInDataframe<ColumnNodeIDs>(&mem, getOutEdges2->outEdges().getDataframe(), getOutEdges2OutTargetNodesTag);

    const ColumnTag getOutEdges2OutEdgeIDsTag = tagMan.allocTag();
    NamedColumn* getOutEdges2OutEdgeIDs = addColumnInDataframe<ColumnEdgeIDs>(&mem, getOutEdges2->outEdges().getDataframe(), getOutEdges2OutEdgeIDsTag);

    const ColumnTag getOutEdges2OutEdgeTypesTag = tagMan.allocTag();
    NamedColumn* getOutEdges2OutEdgeTypes = addColumnInDataframe<ColumnEdgeTypes>(&mem, getOutEdges2->outEdges().getDataframe(), getOutEdges2OutEdgeTypesTag);
    getOutEdges2->outEdges().setEdges(static_cast<ColumnEdgeIDs*>(getOutEdges2OutEdgeIDs->getColumn()), 
                                     static_cast<ColumnNodeIDs*>(getOutEdges2OutTargetNodes->getColumn()), 
                                     static_cast<ColumnEdgeTypes*>(getOutEdges2OutEdgeTypes->getColumn()));

    getOutEdges1->outEdges().connectTo(getOutEdges2->inNodeIDs());

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    getOutEdges2->outEdges().connectTo(materialize->input());

    // Fill up materialize data
    MaterializeData& matData = materialize->getMaterializeData();
    matData.addToStep<ColumnNodeIDs>(scanNodesOutNodeIDs);
    matData.createStep(getOutEdges1OutIndices);
    matData.addToStep<ColumnNodeIDs>(getOutEdges1OutTargetNodes);
    matData.createStep(getOutEdges2OutIndices);
    matData.addToStep<ColumnNodeIDs>(getOutEdges2OutTargetNodes);

    // Lambda
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 3);

        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(scanNodesOutNodeIDsTag)->getColumn());
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

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    materialize->output().connectTo(lambda->input());

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

    // Scan nodes
    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    const ColumnTag scanNodesOutNodeIDsTag = tagMan.allocTag();
    NamedColumn* scanNodesOutNodeIDs = addColumnInDataframe<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs().getDataframe(), scanNodesOutNodeIDsTag);
    scanNodes->outNodeIDs().setNodeIDs(static_cast<ColumnNodeIDs*>(scanNodesOutNodeIDs->getColumn()));

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    scanNodes->outNodeIDs().connectTo(materialize->input());
    {
        MaterializeData& matData = materialize->getMaterializeData();
        matData.addToStep<ColumnNodeIDs>(scanNodesOutNodeIDs);
    }

    // Limit
    constexpr size_t limitCount = 4;
    LimitProcessor* limit = LimitProcessor::create(&pipeline, limitCount);
    const ColumnTag limitOutNodeIDsTag = tagMan.allocTag();
    addColumnInDataframe<ColumnNodeIDs>(&mem, limit->output().getDataframe(), limitOutNodeIDsTag);
    materialize->output().connectTo(limit->input());

    // Lambda
    std::vector<NodeID> resultNodeIds;
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 1);
        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(limitOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        resultNodeIds.insert(resultNodeIds.end(), nodeIDs->getRaw().begin(), nodeIDs->getRaw().end());
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    limit->output().connectTo(lambda->input());

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

    // Scan nodes
    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    const ColumnTag scanNodesOutNodeIDsTag = tagMan.allocTag();
    NamedColumn* scanNodesOutNodeIDs = addColumnInDataframe<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs().getDataframe(), scanNodesOutNodeIDsTag);
    scanNodes->outNodeIDs().setNodeIDs(static_cast<ColumnNodeIDs*>(scanNodesOutNodeIDs->getColumn()));

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    scanNodes->outNodeIDs().connectTo(materialize->input());
    {
        MaterializeData& matData = materialize->getMaterializeData();
        matData.addToStep<ColumnNodeIDs>(scanNodesOutNodeIDs);
    }

    // Skip
    constexpr size_t skipCount = 4;
    SkipProcessor* skip = SkipProcessor::create(&pipeline, skipCount);
    const ColumnTag skipOutNodeIDsTag = tagMan.allocTag();
    addColumnInDataframe<ColumnNodeIDs>(&mem, skip->output().getDataframe(), skipOutNodeIDsTag);
    materialize->output().connectTo(skip->input());

    // Lambda
    std::vector<NodeID> resultNodeIds;
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 1);
        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->getColumn(skipOutNodeIDsTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());

        resultNodeIds.insert(resultNodeIds.end(), nodeIDs->getRaw().begin(), nodeIDs->getRaw().end());
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    skip->output().connectTo(lambda->input());

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

    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(&pipeline);
    const ColumnTag scanNodesOutNodeIDsTag = tagMan.allocTag();
    NamedColumn* scanNodesOutNodeIDs = addColumnInDataframe<ColumnNodeIDs>(&mem, scanNodes->outNodeIDs().getDataframe(), scanNodesOutNodeIDsTag);
    scanNodes->outNodeIDs().setNodeIDs(static_cast<ColumnNodeIDs*>(scanNodesOutNodeIDs->getColumn()));

    // Materialize
    MaterializeProcessor* materialize = MaterializeProcessor::create(&pipeline, &mem);
    scanNodes->outNodeIDs().connectTo(materialize->input());

    // Fill up materialize data
    MaterializeData& matData = materialize->getMaterializeData();
    matData.addToStep<ColumnNodeIDs>(scanNodesOutNodeIDs);

    // Count
    CountProcessor* count = CountProcessor::create(&pipeline);
    const ColumnTag countOutCountTag = tagMan.allocTag();
    NamedColumn* countOutCount = addColumnInDataframe<ColumnConst<size_t>>(&mem, count->output().getDataframe(), countOutCountTag);
    count->output().setValue(static_cast<ColumnConst<size_t>*>(countOutCount->getColumn()));
    materialize->output().connectTo(count->input());

    // Lambda
    size_t expectedCount = 0;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        expectedCount = reader.getNodeCount();
    }

    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 1);

        const ColumnConst<size_t>* count = dynamic_cast<const ColumnConst<size_t>*>(df->getColumn(countOutCountTag)->getColumn());
        ASSERT_TRUE(count != nullptr);

        EXPECT_TRUE(count->getRaw() != 0);
        EXPECT_EQ(count->getRaw(), expectedCount);
    };

    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    count->output().connectTo(lambda->input());

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

    const ColumnTag sourceTag = tagMan.allocTag();

    // Create a source of 100 chunks
    size_t currentChunk = 0;
    constexpr size_t chunkCount = 1000;
    constexpr size_t chunkSize = ChunkConfig::CHUNK_SIZE;
    auto sourceCallback = [&](Dataframe* df, bool& isFinished, auto operation) {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        EXPECT_EQ(df->size(), 1);

        ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->getColumn(sourceTag)->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);

        nodeIDs->resize(chunkSize);
        std::iota(nodeIDs->begin(), nodeIDs->end(), 0);

        currentChunk++;
        if (currentChunk == chunkCount) {
            isFinished = true;
        }
    };

    LambdaSourceProcessor* source = LambdaSourceProcessor::create(&pipeline, sourceCallback);
    addColumnInDataframe<ColumnNodeIDs>(&mem, source->output().getDataframe(), sourceTag);

    // Count
    CountProcessor* count = CountProcessor::create(&pipeline);
    const ColumnTag countOutCountTag = tagMan.allocTag();
    NamedColumn* countOutCount = addColumnInDataframe<ColumnConst<size_t>>(&mem, count->output().getDataframe(), countOutCountTag);
    count->output().setValue(static_cast<ColumnConst<size_t>*>(countOutCount->getColumn()));
    source->output().connectTo(count->input());
    
    // Lambda
    auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) {
        EXPECT_EQ(df->size(), 1);
        const ColumnConst<size_t>* count = dynamic_cast<const ColumnConst<size_t>*>(df->getColumn(countOutCountTag)->getColumn());
        ASSERT_TRUE(count != nullptr);
        EXPECT_EQ(count->getRaw(), chunkCount * chunkSize);
    };
    LambdaProcessor* lambda = LambdaProcessor::create(&pipeline, callback);
    count->output().connectTo(lambda->input());

    // Execute pipeline
    const auto transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ExecutionContext execCtxt(view);
    PipelineExecutor executor(&pipeline, &execCtxt);
    executor.execute();
}
