#include "TuringTest.h"
#include "Graph.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "metadata/GraphMetadata.h"
#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "writers/MetadataBuilder.h"

#include <spdlog/spdlog.h>

using namespace db;
using namespace turing::test;

struct TestEdgeRecord {
    EntityID _edgeID;
    EntityID _nodeID;
    EntityID _otherID;
};

class IteratorsTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = JobSystem::create();
        _graph = Graph::create();

        /* FIRST BUFFER */
        const auto tx1 = _graph->openWriteTransaction();
        auto commitBuilder1 = tx1.prepareCommit();
        auto& builder1 = commitBuilder1->newBuilder();
        PropertyTypeID uint64ID = 0;
        PropertyTypeID stringID = 1;

        {
            // Metadata
            commitBuilder1->metadata().getOrCreateLabel("0");
            commitBuilder1->metadata().getOrCreateLabel("1");
            commitBuilder1->metadata().getOrCreateEdgeType("0");
            commitBuilder1->metadata().getOrCreatePropertyType("UIntProp", ValueType::UInt64);
            commitBuilder1->metadata().getOrCreatePropertyType("StringProp", ValueType::String);
        }

        {
            // Node 0
            const EntityID tmpID = builder1.addNode(LabelSet::fromList({0}));
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder1.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 1
            const EntityID tmpID = builder1.addNode(LabelSet::fromList({0}));
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder1.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 2
            const EntityID tmpID = builder1.addNode(LabelSet::fromList({1}));
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
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
        ASSERT_TRUE(_graph->rebaseAndCommit(*commitBuilder1, *_jobSystem));

        /* SECOND BUFFER */
        const auto tx2 = _graph->openWriteTransaction();
        auto commitBuilder2 = tx2.prepareCommit();
        auto& builder2 = commitBuilder2->newBuilder();

        {
            // Metadata
            commitBuilder2->metadata().getOrCreateLabel("0");
            commitBuilder2->metadata().getOrCreateLabel("1");
            commitBuilder2->metadata().getOrCreateEdgeType("0");
            commitBuilder2->metadata().getOrCreatePropertyType("UIntProp", ValueType::UInt64);
            commitBuilder2->metadata().getOrCreatePropertyType("StringProp", ValueType::String);
        }

        {
            // Node 4
            const EntityID tmpID = builder2.addNode(LabelSet::fromList({0, 1}));
            builder2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder2.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 3
            const EntityID tmpID = builder2.addNode(LabelSet::fromList({1}));
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
            builder2.addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
        }

        spdlog::info(" -- Pushing 2");
        ASSERT_TRUE(_graph->rebaseAndCommit(*commitBuilder2, *_jobSystem));

        /* THIRD BUFFER (Empty) */
        const auto tx3 = _graph->openWriteTransaction();
        auto commitBuilder3 = tx3.prepareCommit();
        [[maybe_unused]] auto& builder3 = commitBuilder3->newBuilder();
        ASSERT_TRUE(_graph->rebaseAndCommit(*commitBuilder3, *_jobSystem));
        spdlog::info(" -- Pushing 3");

        /* FOURTH BUFFER (First node and edge ids: 5, 5) */
        const auto tx4 = _graph->openWriteTransaction();
        auto commitBuilder4 = tx4.prepareCommit();
        auto& builder4 = commitBuilder4->newBuilder();

        {
            // Node 8
            const EntityID tmpID = builder4.addNode(LabelSet::fromList({0, 1}));
            builder4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 5
            const EntityID tmpID = builder4.addNode(LabelSet::fromList({0}));
            builder4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 6
            const EntityID tmpID = builder4.addNode(LabelSet::fromList({1}));
            builder4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            builder4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 7
            const EntityID tmpID = builder4.addNode(LabelSet::fromList({1}));
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

        builder4.addNodeProperty<types::String>(
            2, stringID, "TmpID2 patch");

        const auto readTransaction = _graph->openTransaction();
        const EdgeRecord* edgeToPatch = readTransaction.viewGraph().read().getEdge(2);
        builder4.addEdgeProperty<types::String>(
            *edgeToPatch, stringID, "TmpEdgeID2 patch");

        spdlog::info(" -- Pushing 4");
        ASSERT_TRUE(_graph->rebaseAndCommit(*commitBuilder4, *_jobSystem));
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
    const Transaction transaction = _graph->openTransaction();
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
    const Transaction transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::vector<EntityID> compareSet {0, 1, 2, 3, 4, 5, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EntityID id : reader.scanNodes()) {
        fmt::print("node: {}\n", id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodesByLabelIteratorTest) {
    const Transaction transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::vector<EntityID> compareSet {2, 3, 4, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    const auto labelset = LabelSet::fromList({1});

    for (const EntityID id : reader.scanNodesByLabel(labelset.handle())) {
        fmt::print("Found node id: {}\n", id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanOutEdgesByLabelIteratorTest) {
    const Transaction transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::map<EntityID, const EdgeRecord*> byScanNodesRecords;
    std::map<EntityID, const EdgeRecord*> byScanEdgesRecords;

    // For each existing labelset compare scanOutEdgesByLabel to scanNodesByLabel -> getOutEdges
    for (const auto& [lsetID, labelset] : reader.getMetadata().labelsets()) {
        ColumnIDs nodeIDs;
        for (const EntityID nodeID : reader.scanNodesByLabel(labelset->handle())) {
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
    for (EntityID edgeID = 0; edgeID < count; ++edgeID) {
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
    const Transaction transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    std::map<EntityID, const EdgeRecord*> byScanNodesRecords;
    std::map<EntityID, const EdgeRecord*> byScanEdgesRecords;

    const auto& labelsets = reader.getMetadata().labelsets();
    const size_t labelsetCount = labelsets.getCount();

    // For each existing labelset compare scanInEdgesByLabel to scanNodesByLabel -> getInEdges
    for (LabelSetID lid = 0; lid != labelsetCount - 1; ++lid) {
        const auto labelset = labelsets.getValue(lid);
        ASSERT_TRUE(labelset);

        ColumnIDs nodeIDs;
        std::vector<LabelID> labelIDs;
        labelset.value().decompose(labelIDs);
        fmt::print("Scanning nodes from labelset {}: [{}]\n", lid, fmt::join(labelIDs, ", "));
        for (const EntityID nodeID : reader.scanNodesByLabel(labelset.value())) {
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
    for (EntityID edgeID = 0; edgeID < count; ++edgeID) {
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
    const Transaction transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnIDs inputNodeIDs = {1, 2, 3, 8};
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
}

TEST_F(IteratorsTest, ScanNodePropertiesIteratorTest) {
    const Transaction transaction = _graph->openTransaction();
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
            "TmpID3",
            "TmpID2 patch",
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
    const Transaction transaction = _graph->openTransaction();
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
            "TmpEdgeID2",
            "TmpEdgeID3",
            "TmpEdgeID2 patch",
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
    const Transaction transaction = _graph->openTransaction();
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
            "TmpID3",
            // "TmpID4", This property is not set for this node
            "TmpID2 patch",
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
    const Transaction transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnIDs inputNodeIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8};

    {
        struct NodeInfo {
            EntityID _tmpID;
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
            ASSERT_EQ(it->first, v.nodeID());
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
    const Transaction transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ColumnIDs inputNodeIDs = {1, 3, 8};

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
