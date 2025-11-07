#include <spdlog/spdlog.h>
#include <spdlog/fmt/ranges.h>

#include "TuringTest.h"

#include "Graph.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "metadata/GraphMetadata.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Change.h"
#include "writers/DataPartBuilder.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "writers/GraphWriter.h"
#include "writers/MetadataBuilder.h"

using namespace db;
using namespace turing::test;

struct TestEdgeRecord {
    EdgeID _edgeID;
    NodeID _nodeID;
    NodeID _otherID;
};

struct GraphUpdate {
    Graph* _graph {nullptr};
    std::unique_ptr<Change> _change;
    CommitBuilder& _commit;
    DataPartBuilder& _builder;
    MetadataBuilder& _metadata;

    static GraphUpdate create(Graph& graph) {
        auto change = graph.newChange();
        auto* commit = change->access().getTip();
        auto& builder = commit->newBuilder();
        auto& metadata = builder.getMetadata();
        return GraphUpdate {&graph, std::move(change), *commit, builder, metadata};
    }

    auto submit(JobSystem& jobSystem) {
        auto res = _change->access().submit(jobSystem);
        if (!res) {
            spdlog::error("Failed to submit change: {}", res.error().fmtMessage());
        }
        return res;
    }
};

class IteratorsTest : public TuringTest {
protected:
    /**
    * @warn This test originally added properties to nodes/edges which were created in an
    * older datapart to the change which added the properties. This is not possible
    * currently in TuringDB and so it felt strange to test it.
    *
    * It also resulted in the StringIndex not working correctly, since the index builder
    * requires the mapping of temporary IDs to permenant IDs. If the node/edge to which we
    * are adding a new property to was created in a different datapart, the
    * _tmpToFinalNodeIDs map would not contain the mapping for the node created created in
    * a previous datapart, and hence would fail (throw exception). @Cyrus has changed the
    * test so that it does not apply these "patch" updates (but the properties added are still
    * named as "TmpId<x> patch" to note those which were changed).
    */
    void initialize() override {
        _jobSystem = JobSystem::create();
        _graph = Graph::create();

        /* FIRST BUFFER */
        auto update1  = GraphUpdate::create(*_graph);
        auto& builder1 = update1._builder;
        auto& metadata1 = update1._metadata;

        PropertyTypeID uint64ID = 0;
        PropertyTypeID stringID = 1;

        {
            // Metadata
            metadata1.getOrCreateLabel("0");
            metadata1.getOrCreateLabel("1");
            metadata1.getOrCreateEdgeType("0");
            metadata1.getOrCreatePropertyType("UIntProp", ValueType::UInt64);
            metadata1.getOrCreatePropertyType("StringProp", ValueType::String);
        }

        {
            // Node 0
            const NodeID tmpID = builder1.addNode(LabelSet::fromList({0}));
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder1.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 1
            const NodeID tmpID = builder1.addNode(LabelSet::fromList({0}));
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder1.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 2
            const NodeID tmpID = builder1.addNode(LabelSet::fromList({1}));
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder1.addNodeProperty<types::String>(
                2, stringID, "TmpID2 patch");
        }

        {
            // Edge 001
            const EdgeRecord& edge = builder1.addEdge(0, 0, 1);
            builder1.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder1.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 102
            const EdgeRecord& edge = builder1.addEdge(0, 0, 2);
            builder1.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder1.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        spdlog::info(" -- Pushing 1");
        ASSERT_TRUE(update1.submit(*_jobSystem));

        /* SECOND BUFFER */
        auto update2  = GraphUpdate::create(*_graph);
        auto& builder2 = update2._builder;
        auto& metadata2 = update2._metadata;

        {
            // Metadata
            metadata2.getOrCreateLabel("0");
            metadata2.getOrCreateLabel("1");
            metadata2.getOrCreateEdgeType("0");
            metadata2.getOrCreatePropertyType("UIntProp", ValueType::UInt64);
            metadata2.getOrCreatePropertyType("StringProp", ValueType::String);
        }

        {
            // Node 3
            const NodeID tmpID = builder2.addNode(LabelSet::fromList({0, 1}));
            builder2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder2.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 4
            const NodeID tmpID = builder2.addNode(LabelSet::fromList({1}));
            builder2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // Edge 343
            const EdgeRecord& edge = builder2.addEdge(0, 3, 4);
            builder2.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder2.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 443
            const EdgeRecord& edge = builder2.addEdge(0, 3, 4);
            builder2.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder2.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 234
            const EdgeRecord& edge = builder2.addEdge(0, 4, 3);
            builder2.addEdgeProperty<types::UInt64>(edge, uint64ID,
                                                    edge._edgeID.getValue());
            builder2.addEdgeProperty<types::String>(edge, stringID, "TmpEdgeID2 patch");
        }

        spdlog::info(" -- Pushing 2");
        ASSERT_TRUE(update2.submit(*_jobSystem));

        /* THIRD BUFFER (Empty) */
        auto update3  = GraphUpdate::create(*_graph);
        spdlog::info(" -- Pushing 3");
        ASSERT_TRUE(update3.submit(*_jobSystem));

        /* FOURTH BUFFER (First node and edge ids: 5, 5) */
        auto update4  = GraphUpdate::create(*_graph);
        auto& builder4 = update4._builder;

        {
            // Node 8
            const NodeID tmpID = builder4.addNode(LabelSet::fromList({0, 1}));
            builder4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 5
            const NodeID tmpID = builder4.addNode(LabelSet::fromList({0}));
            builder4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 6
            const NodeID tmpID = builder4.addNode(LabelSet::fromList({1}));
            builder4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 7
            const NodeID tmpID = builder4.addNode(LabelSet::fromList({1}));
            builder4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Edge 654
            const EdgeRecord& edge = builder4.addEdge(0, 6, 4);
            builder4.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder4.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 757
            const EdgeRecord& edge = builder4.addEdge(0, 6, 8);
            builder4.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder4.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 867
            const EdgeRecord& edge = builder4.addEdge(0, 7, 8);
            builder4.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder4.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 528
            const EdgeRecord& edge = builder4.addEdge(0, 2, 5);
            builder4.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            builder4.addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        // XXX: Updates a node property for a node in a different datapart
        // builder4.addNodeProperty<types::String>(
            // 2, stringID, "TmpID2 patch");

        // const auto readTransaction = _graph->openTransaction();
        // const EdgeRecord* edgeToPatch = readTransaction.viewGraph().read().getEdge(2);
        // XXX: Updates an edge property for an edge in a different datapart
        // builder4.addEdgeProperty<types::String>(
            // *edgeToPatch, stringID, "TmpEdgeID2 patch");

        spdlog::info(" -- Pushing 4");
        ASSERT_TRUE(update4.submit(*_jobSystem));
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<db::JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(IteratorsTest, ScanEdgesIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::vector<TestEdgeRecord> compareSet {
        {0, 0, 1},
        {1, 0, 2},
        {2, 3, 4},
        {3, 4, 3},
        {4, 4, 3},
        {5, 2, 8},
        {6, 5, 4},
        {7, 5, 7},
        {8, 6, 7},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EdgeRecord& v : reader.scanOutEdges()) {
        spdlog::info("Testing edge {}: {} -> {}", v._edgeID, it->_nodeID, it->_otherID);
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        spdlog::info("Node: {} has labelset {}",
                     it->_nodeID, reader.getNodeLabelSet(it->_nodeID).getID().getValue());
        count++;
        it++;
    }

    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodesIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::vector<NodeID> compareSet {0, 1, 2, 3, 4, 5, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    for (const NodeID id : reader.scanNodes()) {
        fmt::print("node: {}\n", id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, goToPartGetOutEdgesTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnVector<NodeID> nodes;

    auto VERIFY = [&](auto& cmpSet, auto& it) -> bool {
        auto cmpIt = cmpSet.begin();
        size_t count = 0;
        for (; it.isValid() && cmpIt != cmpSet.end(); it.next()) {
            EdgeRecord er = *it;
            EXPECT_EQ(cmpIt->_nodeID.getValue(), er._nodeID.getValue());
            EXPECT_EQ(cmpIt->_otherID.getValue(), er._otherID.getValue());
            count++;
            cmpIt++;
        }
        EXPECT_EQ(count, cmpSet.size());
        return true;
    };

    { // Skip the first datapart, check out edges on 0
        std::vector<TestEdgeRecord> compareSet {}; // Should be no edges
        nodes.clear();
        nodes.push_back(0);
        GetOutEdgesRange rg = reader.getOutEdges(&nodes);
        GetOutEdgesIterator it = rg.begin();
        it.goToPart(1);
        ASSERT_TRUE(VERIFY(compareSet, it));
    }

    { // Skip the first two dataparts - ends up pointing at empty DP : check that we
      // advance again to a valid part. Check out edges of Node 2
        std::vector<TestEdgeRecord> compareSet {
            // {0, 0, 1},
            // {1, 0, 2},
            // {2, 3, 4},
            // {3, 4, 3},
            // {4, 4, 3},
            {5, 2, 8},
            // {6, 5, 4},
            // {7, 5, 7},
            // {8, 6, 7},
        };
        nodes.clear();
        nodes.push_back(2);

        GetOutEdgesRange rg = reader.getOutEdges(&nodes);
        GetOutEdgesIterator it = rg.begin();
        it.goToPart(2);

        ASSERT_TRUE(VERIFY(compareSet, it));
    }

    { // Skip all four dataparts, check all nodes
        std::vector<TestEdgeRecord> compareSet {};
        nodes.clear();
        nodes.push_back(0);
        nodes.push_back(1);
        nodes.push_back(2);
        nodes.push_back(3);
        nodes.push_back(4);
        nodes.push_back(5);
        nodes.push_back(6);
        nodes.push_back(7);

        GetOutEdgesRange rg = reader.getOutEdges(&nodes);
        GetOutEdgesIterator it = rg.begin();
        it.goToPart(4);

        ASSERT_TRUE(VERIFY(compareSet, it));
    }

    { // Attempt to GOTO a part index which is greater than the number of parts that exist
        std::vector<TestEdgeRecord> compareSet {};
        nodes.clear();
        nodes.push_back(0);
        nodes.push_back(1);
        nodes.push_back(2);
        nodes.push_back(3);
        nodes.push_back(4);
        nodes.push_back(5);
        nodes.push_back(6);
        nodes.push_back(7);

        GetOutEdgesRange rg = reader.getOutEdges(&nodes);
        GetOutEdgesIterator it = rg.begin();
        it.goToPart(10);

        ASSERT_TRUE(VERIFY(compareSet, it));
        ASSERT_FALSE(it.isValid());
    }
}

TEST_F(IteratorsTest, goToPartGetInEdgesTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnVector<NodeID> nodes;

    auto VERIFY = [&](auto& cmpSet, auto& it) -> bool {
        auto cmpIt = cmpSet.begin();
        size_t count = 0;
        for (; it.isValid() && cmpIt != cmpSet.end(); it.next()) {
            EdgeRecord er = *it;
            EXPECT_EQ(cmpIt->_nodeID.getValue(), er._nodeID.getValue());
            EXPECT_EQ(cmpIt->_otherID.getValue(), er._otherID.getValue());
            count++;
            cmpIt++;
        }
        EXPECT_EQ(count, cmpSet.size());
        return true;
    };

    { // Skip the first datapart, check in edges on 0
        std::vector<TestEdgeRecord> compareSet {}; // Should be no edges
        nodes.clear();
        nodes.push_back(0);
        GetInEdgesRange rg = reader.getInEdges(&nodes);
        GetInEdgesIterator it = rg.begin();
        it.goToPart(1);
        ASSERT_TRUE(VERIFY(compareSet, it));
    }

    { // Skip the first two dataparts - ends up pointing at empty DP : check that we
      // advance again to a valid part. Check in edges of Node 8
        std::vector<TestEdgeRecord> compareSet {
            // {0, 1, 0},
            // {1, 2, 0},
            // {3, 3, 4},
            // {4, 3, 4},
            // {2, 4, 3},
            // {6, 4, 5},
            // {7, 7, 5},
            // {8, 7, 6},
            {5, 8, 2}
        };
        nodes.clear();
        nodes.push_back(8);

        GetInEdgesRange rg = reader.getInEdges(&nodes);
        GetInEdgesIterator it = rg.begin();
        it.goToPart(2);

        ASSERT_TRUE(VERIFY(compareSet, it));
    }

    { // Skip all four dataparts, check all nodes
        std::vector<TestEdgeRecord> compareSet {};
        nodes.clear();
        nodes.push_back(0);
        nodes.push_back(1);
        nodes.push_back(2);
        nodes.push_back(3);
        nodes.push_back(4);
        nodes.push_back(5);
        nodes.push_back(6);
        nodes.push_back(7);

        GetInEdgesRange rg = reader.getInEdges(&nodes);
        GetInEdgesIterator it = rg.begin();
        it.goToPart(4);

        ASSERT_TRUE(VERIFY(compareSet, it));
    }

    { // Attempt to GOTO a part index which is greater than the number of parts that exist
        std::vector<TestEdgeRecord> compareSet {};
        nodes.clear();
        nodes.push_back(0);
        nodes.push_back(1);
        nodes.push_back(2);
        nodes.push_back(3);
        nodes.push_back(4);
        nodes.push_back(5);
        nodes.push_back(6);
        nodes.push_back(7);

        GetInEdgesRange rg = reader.getInEdges(&nodes);
        GetInEdgesIterator it = rg.begin();
        it.goToPart(10);

        ASSERT_TRUE(VERIFY(compareSet, it));
        ASSERT_FALSE(it.isValid());
    }
}

TEST_F(IteratorsTest, ScanNodesByLabelIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::vector<NodeID> compareSet {2, 3, 4, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    const auto labelset = LabelSet::fromList({1});

    for (const NodeID id : reader.scanNodesByLabel(labelset.handle())) {
        fmt::print("Found node id: {}\n", id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanOutEdgesByLabelIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::map<EdgeID, const EdgeRecord*> byScanNodesRecords;
    std::map<EdgeID, const EdgeRecord*> byScanEdgesRecords;

    // For each existing labelset compare scanOutEdgesByLabel to scanNodesByLabel -> getOutEdges
    for (const auto& [lsetID, labelset] : reader.getMetadata().labelsets()) {
        ColumnNodeIDs nodeIDs;
        for (const NodeID nodeID : reader.scanNodesByLabel(labelset->handle())) {
            nodeIDs = ColumnVector {nodeID};
            for (const EdgeRecord& edge : reader.getOutEdges(&nodeIDs)) {
                byScanNodesRecords.emplace(edge._edgeID, &edge);
            }
        }

        for (const EdgeRecord& edge : reader.scanOutEdgesByLabel(labelset->handle())) {
            byScanEdgesRecords.emplace(edge._edgeID, &edge);
        }
    }

    ASSERT_EQ(byScanNodesRecords.size(), byScanEdgesRecords.size());
    const size_t count = byScanNodesRecords.size();
    for (EdgeID edgeID = 0; edgeID < count; ++edgeID) {
        const auto& byScanNodes = *byScanNodesRecords.at(edgeID);
        const auto& byScanEdges = *byScanEdgesRecords.at(edgeID);
        spdlog::info("Comparing [{}:{}->{}] to [{}:{}->{}]",
                     byScanNodes._edgeID, byScanNodes._nodeID, byScanNodes._otherID,
                     byScanEdges._edgeID, byScanEdges._nodeID, byScanEdges._otherID);
        ASSERT_EQ(byScanNodes._nodeID, byScanEdges._nodeID);
        ASSERT_EQ(byScanNodes._otherID, byScanEdges._otherID);
    }
}

TEST_F(IteratorsTest, ScanInEdgesByLabelIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::map<EdgeID, const EdgeRecord*> byScanNodesRecords;
    std::map<EdgeID, const EdgeRecord*> byScanEdgesRecords;

    const auto& labelsets = reader.getMetadata().labelsets();
    const size_t labelsetCount = labelsets.getCount();

    // For each existing labelset compare scanInEdgesByLabel to scanNodesByLabel -> getInEdges
    for (LabelSetID lid = 0; lid != labelsetCount - 1; ++lid) {
        const auto labelset = labelsets.getValue(lid);
        ASSERT_TRUE(labelset);

        ColumnNodeIDs nodeIDs;
        std::vector<LabelID> labelIDs;
        labelset.value().decompose(labelIDs);
        fmt::print("Scanning nodes from labelset {}: [{}]\n", lid, fmt::join(labelIDs, ", "));
        for (const NodeID nodeID : reader.scanNodesByLabel(labelset.value())) {
            nodeIDs = ColumnVector {nodeID};
            fmt::print("- Expanding in edges from node {}\n", nodeID.getValue());
            for (const EdgeRecord& edge : reader.getInEdges(&nodeIDs)) {
                fmt::print("   Edge {}: {}->{}\n", edge._edgeID.getValue(), edge._nodeID.getValue(), edge._otherID.getValue());
                byScanNodesRecords.emplace(edge._edgeID, &edge);
            }
        }

        fmt::print("Scanning nodes from labelset {}: [{}]\n", lid, fmt::join(labelIDs, ", "));
        for (const EdgeRecord& edge : reader.scanInEdgesByLabel(labelset.value())) {
            fmt::print("   Edge {}: {}->{}\n", edge._edgeID.getValue(), edge._nodeID.getValue(), edge._otherID.getValue());
            byScanEdgesRecords.emplace(edge._edgeID, &edge);
        }
    }

    ASSERT_EQ(byScanNodesRecords.size(), byScanEdgesRecords.size());
    const size_t count = byScanNodesRecords.size();
    for (EdgeID edgeID = 0; edgeID < count; ++edgeID) {
        fmt::print("- Checking edge {}\n", edgeID);
        const auto& byScanNodes = *byScanNodesRecords.at(edgeID);
        const auto& byScanEdges = *byScanEdgesRecords.at(edgeID);
        spdlog::info("  Comparing [{}:{}->{}] to [{}:{}->{}]",
                     byScanNodes._edgeID, byScanNodes._nodeID, byScanNodes._otherID,
                     byScanEdges._edgeID, byScanEdges._nodeID, byScanEdges._otherID);
        ASSERT_EQ(byScanNodes._nodeID, byScanEdges._nodeID);
        ASSERT_EQ(byScanNodes._otherID, byScanEdges._otherID);
    }
}

TEST_F(IteratorsTest, GetEdgesIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnNodeIDs inputNodeIDs = {1, 2, 3, 8};
    std::vector<TestEdgeRecord> compareSet {
        {2, 3, 4},
        {5, 2, 8},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    spdlog::info("-- Out edges");
    for (const EdgeRecord& v : reader.getOutEdges(&inputNodeIDs)) {
        spdlog::info("[{}: {}->{}]", v._edgeID, v._nodeID, v._otherID);
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }

    ASSERT_EQ(compareSet.size(), count);

    count = 0;
    compareSet = {
        {0, 0, 1},
        {1, 0, 2},
        {3, 4, 3},
        {4, 4, 3},
        {5, 2, 8},
    };
    it = compareSet.begin();

    spdlog::info("-- In edges");
    for (const EdgeRecord& v : reader.getInEdges(&inputNodeIDs)) {
        spdlog::info("[{}: {}->{}]", v._edgeID, v._otherID, v._nodeID);
        ASSERT_EQ(it->_nodeID.getValue(), v._otherID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._nodeID.getValue());
        count++;
        it++;
    }

    ASSERT_EQ(count, compareSet.size());

    count = 0;
    compareSet = {
        {0, 1, 0},
        {1, 2, 0},
        {3, 3, 4},
        {4, 3, 4},
        {2, 3, 4},
        {5, 2, 8},
        {5, 8, 2},
    };
    it = compareSet.begin();

    spdlog::info("-- Out/In Edges");

    for (const EdgeRecord& v : reader.getEdges(&inputNodeIDs)) {
        spdlog::info("[{}: {}<->{}]", v._edgeID, v._nodeID, v._otherID);
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }

    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodePropertiesIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    {
        std::vector<uint64_t> compareSet {0, 1, 2, 4, 3, 6, 7, 8, 5};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.scanNodeProperties<types::UInt64>(0)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            "TmpID0",
            "TmpID1",
            // "TmpID4", This property is not set for this node
            "TmpID2 patch",
            "TmpID3",
            "TmpID6",
            "TmpID7",
            "TmpID8",
            "TmpID5",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (std::string_view v : reader.scanNodeProperties<types::String>(1)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, ScanEdgePropertiesIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    {
        std::vector<uint64_t> compareSet {0, 1, 4, 2, 3, 8, 5, 6, 7};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.scanEdgeProperties<types::UInt64>(0)) {
            fmt::print("v = {}\n", v);
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            "TmpEdgeID0",
            "TmpEdgeID1",
            "TmpEdgeID2 patch",
            "TmpEdgeID2",
            "TmpEdgeID3",
            "TmpEdgeID8",
            "TmpEdgeID5",
            "TmpEdgeID6",
            "TmpEdgeID7",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (std::string_view v : reader.scanEdgeProperties<types::String>(1)) {
            fmt::print("v = {}\n", v);
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, ScanNodePropertiesByLabelIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const auto labelset = LabelSet::fromList({1});
    const LabelSetHandle ref {labelset};

    {
        std::vector<uint64_t> compareSet {2, 4, 3, 7, 8, 5};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.scanNodePropertiesByLabel<types::UInt64>(0, ref)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            "TmpID2 patch",
            "TmpID3",
            // "TmpID4", This property is not set for this node
            "TmpID7",
            "TmpID8",
            "TmpID5",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (std::string_view v : reader.scanNodePropertiesByLabel<types::String>(1, ref)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, GetNodeViewsIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnNodeIDs inputNodeIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8};

    {
        struct NodeInfo {
            NodeID _tmpID;
            size_t _props = 0;
            size_t _outs = 0;
            size_t _ins = 0;
        };

        std::map<uint64_t, NodeInfo> compareSet = {
            {0, {._tmpID = 0, ._props = 2, ._outs = 2, ._ins = 0}},
            {1, {._tmpID = 1, ._props = 2, ._outs = 0, ._ins = 1}},
            {2, {._tmpID = 2, ._props = 2, ._outs = 1, ._ins = 1}},
            {3, {._tmpID = 4, ._props = 1, ._outs = 1, ._ins = 2}},
            {4, {._tmpID = 3, ._props = 2, ._outs = 2, ._ins = 2}},
            {5, {._tmpID = 6, ._props = 2, ._outs = 2, ._ins = 0}},
            {6, {._tmpID = 7, ._props = 2, ._outs = 1, ._ins = 0}},
            {7, {._tmpID = 8, ._props = 2, ._outs = 0, ._ins = 2}},
            {8, {._tmpID = 5, ._props = 2, ._outs = 0, ._ins = 1}},
        };

        auto it = compareSet.begin();
        size_t count = 0;
        for (const NodeView& v : reader.getNodeViews(&inputNodeIDs)) {
            const auto& props = v.properties();
            const auto& edges = v.edges();
            spdlog::info("[{}, [._tmpID = {}, ._props = {}, ._outs = {}, ._ins = {}]]",
                         v.nodeID(),
                         props.getProperty<types::UInt64>(0),
                         props.getCount(),
                         edges.getOutEdgeCount(),
                         edges.getInEdgeCount());

            ASSERT_TRUE(v.isValid());
            ASSERT_EQ(NodeID(it->first), v.nodeID());
            ASSERT_EQ(it->second._tmpID, props.getProperty<types::UInt64>(0));
            ASSERT_EQ(it->second._props, props.getCount());
            ASSERT_EQ(it->second._outs, edges.getOutEdgeCount());
            ASSERT_EQ(it->second._ins, edges.getInEdgeCount());
            ++it;
            ++count;
        }

        ASSERT_EQ(compareSet.size(), count);
    }
}

TEST_F(IteratorsTest, GetNodePropertiesIteratorTest) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnNodeIDs inputNodeIDs = {1, 3, 8};

    {
        std::vector<uint64_t> compareSet {1, 4, 5};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.getNodeProperties<types::UInt64>(0, &inputNodeIDs)) {
            fmt::print("v={}\n", v);
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            "TmpID1",
            "TmpID5",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (std::string_view v : reader.getNodeProperties<types::String>(1, &inputNodeIDs)) {
            spdlog::info("{} ?= {}", *it, v);
            ASSERT_TRUE(*it == v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
