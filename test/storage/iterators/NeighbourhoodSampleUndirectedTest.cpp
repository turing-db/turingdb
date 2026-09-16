#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/NeighbourhoodSampleIterator.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// A node's out edges and in edges are one sample. simpledb degrees, by node ID: Remy (0)
// 4 out and 2 in, Computers (2) 0 out and 2 in (from Remy and from Luc, written in
// different commits), Ghosts (6) 1 out and 1 in.
class NeighbourhoodSampleUndirectedTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void sampledNeighbours(const GraphView& view,
                           const ColumnNodeIDs& input,
                           size_t sampleSize,
                           std::vector<NodeID>& neighbours) {
        ColumnNodeIDs neighbourIDs;

        NeighbourhoodSampleChunkWriter writer(view, &input, sampleSize);
        writer.setOutputColumns(nullptr, nullptr, nullptr, &neighbourIDs);
        writer.fill(ChunkConfig::CHUNK_SIZE);

        neighbours.assign(neighbourIDs.begin(), neighbourIDs.end());
        std::ranges::sort(neighbours);
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;
};

// Computers has no out-edges at all, and its two in-edges were written in different
// commits, so reaching them means reading both sides of more than one datapart.
TEST_F(NeighbourhoodSampleUndirectedTest, aNodeWithOnlyInEdgesIsSampled) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {2};

    std::vector<NodeID> neighbours;
    sampledNeighbours(reader.getView(), input, 10, neighbours);

    const std::vector<NodeID> expected {0, 9};
    EXPECT_EQ(neighbours, expected);
}

// Remy's 4 out-edges and 2 in-edges are one stream of 6. Adam and Ghosts are each reached
// twice, once from either side, because both pairs are joined in both directions.
TEST_F(NeighbourhoodSampleUndirectedTest, bothSidesOfTheNodeAreSampled) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {0};

    std::vector<NodeID> neighbours;
    sampledNeighbours(reader.getView(), input, 10, neighbours);

    const std::vector<NodeID> expected {1, 1, 2, 3, 6, 6};
    EXPECT_EQ(neighbours, expected);
}

// sampleSize is the size of the sample, not a budget per side: 3 over a degree of 6 draws
// 3 rows, never 3 out-edges and 3 in-edges.
TEST_F(NeighbourhoodSampleUndirectedTest, sampleSizeCapsTheWholeStream) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {0};

    std::vector<NodeID> neighbours;
    sampledNeighbours(reader.getView(), input, 3, neighbours);

    EXPECT_EQ(neighbours.size(), 3U);
}

// The source column is the node the edge was drawn from and the other column the far end,
// on an in-edge as much as on an out-edge.
TEST_F(NeighbourhoodSampleUndirectedTest, theSourceIsTheSampledNode) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {2, 6};
    ColumnNodeIDs srcIDs;
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, 10);
    writer.setOutputColumns(&srcIDs, nullptr, nullptr, &dstIDs);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    ASSERT_EQ(srcIDs.size(), 4U);
    ASSERT_EQ(dstIDs.size(), 4U);

    for (size_t row = 0; row < srcIDs.size(); row++) {
        const NodeID src = srcIDs[row];
        const bool sampledFromInput = src == NodeID {2} || src == NodeID {6};
        EXPECT_TRUE(sampledFromInput) << "row " << row << " reports source " << src.getValue();
        EXPECT_NE(dstIDs[row], src);
    }
}

// A self-loop is an out-edge and an in-edge of the one node, so reading both sides would
// offer it twice. Remy's degree must read 7 and not 8.
TEST_F(NeighbourhoodSampleUndirectedTest, aSelfLoopIsSampledOnce) {
    GraphWriter writer(_graph.get(), _jobSystem.get());
    writer.addEdge("KNOWS_WELL", NodeID {0}, NodeID {0});
    writer.submit();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {0};

    std::vector<NodeID> neighbours;
    sampledNeighbours(reader.getView(), input, 20, neighbours);

    const std::vector<NodeID> expected {0, 1, 1, 2, 3, 6, 6};
    EXPECT_EQ(neighbours, expected);
}

// Ghosts is reached from Remy and reaches Remy back. Deleting the edge into Ghosts leaves
// its sample with the one out-edge, so the in side honours tombstones too.
TEST_F(NeighbourhoodSampleUndirectedTest, aDeletedEdgeIsNotSampledFromTheInSide) {
    std::vector<EdgeID> edgeIDs;
    std::vector<EdgeTypeID> edgeTypes;
    std::vector<NodeID> outTargets;
    SimpleGraph::findOutEdges(_graph.get(), {0}, edgeIDs, edgeTypes, outTargets);

    const auto ghosts = std::ranges::find(outTargets, NodeID {6});
    ASSERT_NE(ghosts, outTargets.end());

    GraphWriter writer(_graph.get(), _jobSystem.get());
    writer.deleteEdge(edgeIDs[std::distance(outTargets.begin(), ghosts)]);
    writer.submit();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {6};

    std::vector<NodeID> neighbours;
    sampledNeighbours(reader.getView(), input, 10, neighbours);

    const std::vector<NodeID> expected {0};
    EXPECT_EQ(neighbours, expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 100;
    });
}
