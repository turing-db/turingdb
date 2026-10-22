#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>

#include "Graph.h"
#include "JobSystem.h"
#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "datapart/NodeContainer.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/EdgeView.h"
#include "views/GraphView.h"
#include "views/NodeView.h"
#include "writers/GraphWriter.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

LabelSetHandle labelSetInAnyPart(const GraphView& view, NodeID nodeID) {
    for (const auto& part : view.dataparts()) {
        if (part->hasNode(nodeID)) {
            return part->nodes().getNodeLabelSet(nodeID);
        }
    }

    return LabelSetHandle {};
}

const EdgeRecord* edgeInAnyPart(const GraphView& view, EdgeID edgeID) {
    for (const auto& part : view.dataparts()) {
        const EdgeRecord* edge = part->edges().tryGet(edgeID);
        if (edge) {
            return edge;
        }
    }

    return nullptr;
}

}

// The reader finds the part owning a node or an edge by its ID. Every ID, and a few past the
// last one allocated, has to resolve to what a search of every part finds. The commits after
// simpledb add a part with edges and no nodes, one with nodes and no edges, and one with both.
class OwnerPartLookupTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        const NodeID remy {0};
        const NodeID adam {1};

        {
            GraphWriter writer(_graph.get(), _jobSystem.get());
            writer.addEdge("KNOWS_WELL", remy, adam);
            writer.submit();
        }

        {
            GraphWriter writer(_graph.get(), _jobSystem.get());
            writer.addNode({"Person"});
            writer.addNode({"Interest", "Exotic"});
            writer.submit();
        }

        {
            GraphWriter writer(_graph.get(), _jobSystem.get());
            const NodeID chess = writer.addNode({"Interest"});
            writer.addEdge("INTERESTED_IN", adam, chess);
            writer.addEdge("INTERESTED_IN", chess, remy);
            writer.submit();
        }
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(OwnerPartLookupTest, findsTheNodeOfEveryID) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    const size_t partCount = reader.dataparts().size();
    const size_t nodeCount = reader.getTotalNodesAllocated();
    ASSERT_GT(partCount, 1u);

    for (size_t id = 0; id < nodeCount + 3; id++) {
        const NodeID nodeID {id};
        const LabelSetHandle expected = labelSetInAnyPart(view, nodeID);

        const LabelSetHandle labelset = reader.getNodeLabelSet(nodeID);
        EXPECT_EQ(labelset.isValid(), expected.isValid()) << "node " << id;
        if (expected.isValid()) {
            EXPECT_EQ(labelset.getID(), expected.getID()) << "node " << id;
        }

        const size_t partIndex = reader.getNodePartIndex(nodeID);
        if (expected.isValid()) {
            ASSERT_LT(partIndex, partCount) << "node " << id;
            EXPECT_TRUE(reader.dataparts()[partIndex]->hasNode(nodeID)) << "node " << id;
        } else {
            EXPECT_EQ(partIndex, partCount) << "node " << id;
        }

        const NodeView nodeView = reader.getNodeView(nodeID);
        EXPECT_EQ(nodeView.isValid(), expected.isValid()) << "node " << id;
    }
}

TEST_F(OwnerPartLookupTest, findsTheEdgeOfEveryID) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    const size_t partCount = reader.dataparts().size();
    const size_t edgeCount = reader.getTotalEdgesAllocated();

    for (size_t id = 0; id < edgeCount + 3; id++) {
        const EdgeID edgeID {id};
        const EdgeRecord* expected = edgeInAnyPart(view, edgeID);

        EXPECT_EQ(reader.getEdge(edgeID), expected) << "edge " << id;

        const size_t partIndex = reader.getEdgePartIndex(edgeID);
        const EdgeView edgeView = reader.getEdgeView(edgeID);
        if (expected) {
            ASSERT_LT(partIndex, partCount) << "edge " << id;
            EXPECT_TRUE(reader.dataparts()[partIndex]->hasEdge(edgeID)) << "edge " << id;
            EXPECT_EQ(reader.getEdgeTypeID(edgeID), expected->_edgeTypeID) << "edge " << id;
            ASSERT_TRUE(edgeView.isValid()) << "edge " << id;
            EXPECT_EQ(edgeView.sourceID(), expected->_nodeID) << "edge " << id;
            EXPECT_EQ(edgeView.targetID(), expected->_otherID) << "edge " << id;
        } else {
            EXPECT_EQ(partIndex, partCount) << "edge " << id;
            EXPECT_FALSE(reader.getEdgeTypeID(edgeID).isValid()) << "edge " << id;
            EXPECT_FALSE(edgeView.isValid()) << "edge " << id;
        }
    }
}
