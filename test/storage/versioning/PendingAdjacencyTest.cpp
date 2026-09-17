#include "gtest/gtest.h"

#include "versioning/CommitJournal.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/PendingAdjacency.h"

using namespace db;

namespace {

constexpr size_t firstPendingNodeID = 100;
constexpr size_t firstPendingEdgeID = 200;

}

// The index one program shares between its walks: every walk hands it the buffer again, and
// what a walk reads is held to the edges that existed when that walk started.
class PendingAdjacencyTest : public ::testing::Test {
protected:
    CommitJournal _journal {*CommitJournal::emptyJournal()};
    GraphView _view;
    CommitWriteBuffer _buffer {_journal, _view};
    PendingAdjacency _adjacency;

    // An edge between two nodes the graph already holds
    void writeEdge(uint64_t source, uint64_t target, uint64_t edgeType) {
        CommitWriteBuffer::PendingEdge& edge = _buffer.newPendingEdge(NodeID(source), NodeID(target));
        edge.edgeType = EdgeTypeID(edgeType);
    }

    void index(size_t firstQueryEdge = 0) {
        _adjacency.index(_buffer, firstQueryEdge, firstPendingNodeID, firstPendingEdgeID);
    }

    size_t outCount(uint64_t node) {
        return _adjacency.outOf(NodeID(node), _adjacency.getEdgeIDBound()).size();
    }

    size_t inCount(uint64_t node) {
        return _adjacency.into(NodeID(node), _adjacency.getEdgeIDBound()).size();
    }
};

TEST_F(PendingAdjacencyTest, indexesAnEdgeUnderBothItsEndpoints) {
    writeEdge(1, 2, 7);
    index();

    ASSERT_EQ(outCount(1), 1u);
    ASSERT_EQ(inCount(2), 1u);
    EXPECT_EQ(outCount(2), 0u);
    EXPECT_EQ(inCount(1), 0u);

    const EdgeRecord& record = _adjacency.outOf(NodeID(1), _adjacency.getEdgeIDBound()).front();
    EXPECT_EQ(record._edgeID.getValue(), firstPendingEdgeID);
    EXPECT_EQ(record._nodeID.getValue(), 1u);
    EXPECT_EQ(record._otherID.getValue(), 2u);
    EXPECT_EQ(record._edgeTypeID.getValue(), 7u);
}

// The edge a pending node hangs off is named by the ID that node will commit as.
TEST_F(PendingAdjacencyTest, namesAPendingEndpointByTheIDItWillCommitAs) {
    _buffer.newPendingNode();
    CommitWriteBuffer::PendingEdge& edge = _buffer.newPendingEdge(NodeID(1), size_t {0});
    edge.edgeType = EdgeTypeID(0);
    index();

    ASSERT_EQ(outCount(1), 1u);
    EXPECT_EQ(_adjacency.outOf(NodeID(1), _adjacency.getEdgeIDBound()).front()._otherID.getValue(),
              firstPendingNodeID);
}

// Indexing twice over the same buffer walks each edge once: a second run of a walk would
// otherwise go through every edge the first one indexed a second time.
TEST_F(PendingAdjacencyTest, indexesAnEdgeOnceAcrossRepeatedCalls) {
    writeEdge(1, 2, 0);
    index();
    index();
    index();

    EXPECT_EQ(outCount(1), 1u);
    EXPECT_EQ(inCount(2), 1u);
}

TEST_F(PendingAdjacencyTest, takesInTheEdgesWrittenBetweenTwoCalls) {
    writeEdge(1, 2, 0);
    index();
    ASSERT_EQ(outCount(1), 1u);

    writeEdge(1, 3, 0);
    index();
    EXPECT_EQ(outCount(1), 2u);
}

// A walk reads to the bound it started with, so an edge written while it runs is one it does
// not go through - what the second walk of the program sees, not the first.
TEST_F(PendingAdjacencyTest, holdsAWalkToTheEdgesItStartedWith) {
    writeEdge(1, 2, 0);
    index();

    const size_t boundOfTheFirstWalk = _adjacency.getEdgeIDBound();

    writeEdge(1, 3, 0);
    index();

    EXPECT_EQ(_adjacency.outOf(NodeID(1), boundOfTheFirstWalk).size(), 1u);
    EXPECT_EQ(_adjacency.outOf(NodeID(1), _adjacency.getEdgeIDBound()).size(), 2u);
}

// What an earlier statement of the change staged is read once it commits, so a walk starts
// at its own query's first edge.
TEST_F(PendingAdjacencyTest, leavesOutTheEdgesAnEarlierStatementStaged) {
    writeEdge(1, 2, 0);
    writeEdge(1, 3, 0);
    index(1);

    ASSERT_EQ(outCount(1), 1u);
    EXPECT_EQ(_adjacency.outOf(NodeID(1), _adjacency.getEdgeIDBound()).front()._otherID.getValue(), 3u);
}

// A dropped write invalidates what is already indexed, so the index is built again rather
// than appended to.
TEST_F(PendingAdjacencyTest, forgetsAnEdgeTheChangeDropped) {
    writeEdge(1, 2, 0);
    writeEdge(1, 3, 0);
    index();
    ASSERT_EQ(outCount(1), 2u);

    _buffer.addDeletedPendingEdge(0);
    index();

    ASSERT_EQ(outCount(1), 1u);
    EXPECT_EQ(_adjacency.outOf(NodeID(1), _adjacency.getEdgeIDBound()).front()._otherID.getValue(), 3u);
}

TEST_F(PendingAdjacencyTest, readsTheNodeIDBoundOffTheNodesTheChangeWrote) {
    EXPECT_EQ(_adjacency.getNodeIDBound(), 0u);

    _buffer.newPendingNode();
    _buffer.newPendingNode();
    index();

    EXPECT_EQ(_adjacency.getNodeIDBound(), firstPendingNodeID + 2);
    EXPECT_TRUE(_adjacency.isPendingNode(NodeID(firstPendingNodeID)));
    EXPECT_FALSE(_adjacency.isPendingNode(NodeID(firstPendingNodeID - 1)));
}
