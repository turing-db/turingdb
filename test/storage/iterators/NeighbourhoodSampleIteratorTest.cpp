#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

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

class NeighbourhoodSampleIteratorTest : public TuringTest {
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

    // The sampler reports each edge as the graph holds it, so the neighbour it reached is
    // the source column and the node it sampled for is the target one.
    void sampleNeighbours(const ColumnNodeIDs& input, size_t sampleSize, ColumnNodeIDs& neighbours) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, sampleSize);
        writer.setOutputColumns(&neighbours, nullptr, nullptr, nullptr);
        writer.fill(ChunkConfig::CHUNK_SIZE);
    }

    void deleteEdges(const std::vector<EdgeID>& edgeIDs) {
        GraphWriter writer(_graph.get(), _jobSystem.get());
        for (const EdgeID edgeID : edgeIDs) {
            writer.deleteEdge(edgeID);
        }
        writer.submit();
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(NeighbourhoodSampleIteratorTest, rootFirstInputYieldsSamples) {
    // Luc (9) has no in-edges, Computers (2) has two.
    const ColumnNodeIDs input = {9, 2};
    ColumnNodeIDs neighbours;

    sampleNeighbours(input, 2, neighbours);

    EXPECT_GT(neighbours.size(), 0U);
}

TEST_F(NeighbourhoodSampleIteratorTest, allRootInputYieldsEmpty) {
    // Maxime (8), Luc (9), Martina (11), Suhas (12) are never the target of an edge.
    const ColumnNodeIDs input = {8, 9, 11, 12};
    ColumnNodeIDs neighbours;

    sampleNeighbours(input, 2, neighbours);

    EXPECT_EQ(neighbours.size(), 0U);
}

TEST_F(NeighbourhoodSampleIteratorTest, sampleSizeCapRespected) {
    // Gym (13) has 3 in-edges, from Cyrus, Doruk and Suhas; sampleSize 2 must cap the
    // output to exactly 2.
    const ColumnNodeIDs input = {13};
    ColumnNodeIDs neighbours;

    sampleNeighbours(input, 2, neighbours);

    EXPECT_EQ(neighbours.size(), 2U);
}

TEST_F(NeighbourhoodSampleIteratorTest, deletedEdgesAreNotSampled) {
    // Delete one of Remy's in-edges (Remy=0 has 2: from Adam and from Ghosts). The
    // sampler must not return the node it came from.
    std::vector<EdgeID> edgeIDs;
    std::vector<EdgeTypeID> edgeTypes;
    std::vector<NodeID> sources;
    SimpleGraph::findInEdges(_graph.get(), {0}, edgeIDs, edgeTypes, sources);
    ASSERT_FALSE(edgeIDs.empty());

    const NodeID deletedSource = sources[0];
    deleteEdges({edgeIDs[0]});

    const ColumnNodeIDs input = {0};
    ColumnNodeIDs neighbours;

    sampleNeighbours(input, 4, neighbours);

    for (const NodeID neighbour : neighbours) {
        EXPECT_NE(neighbour, deletedSource);
    }
}

TEST_F(NeighbourhoodSampleIteratorTest, allEdgesDeletedYieldsEmpty) {
    // Delete all of Remy's in-edges; the sampler must produce no rows for Remy.
    std::vector<EdgeID> edgeIDs;
    std::vector<EdgeTypeID> edgeTypes;
    std::vector<NodeID> sources;
    SimpleGraph::findInEdges(_graph.get(), {0}, edgeIDs, edgeTypes, sources);
    ASSERT_FALSE(edgeIDs.empty());

    deleteEdges(edgeIDs);

    const ColumnNodeIDs input = {0};
    ColumnNodeIDs neighbours;

    sampleNeighbours(input, 4, neighbours);

    EXPECT_EQ(neighbours.size(), 0U);
}

TEST_F(NeighbourhoodSampleIteratorTest, deletedEdgesNotSampledAcrossMultipleNodes) {
    // Delete Gym's in-edges; sampling both Computers and Gym must still return
    // Computers' neighbours but nothing for Gym.
    std::vector<EdgeID> gymEdgeIDs;
    std::vector<EdgeTypeID> gymEdgeTypes;
    std::vector<NodeID> gymSources;
    SimpleGraph::findInEdges(_graph.get(), {13}, gymEdgeIDs, gymEdgeTypes, gymSources);
    ASSERT_FALSE(gymEdgeIDs.empty());

    deleteEdges(gymEdgeIDs);

    const ColumnNodeIDs input = {2, 13};
    ColumnNodeIDs neighbours;

    sampleNeighbours(input, 4, neighbours);

    // None of Gym's neighbours must appear.
    for (const NodeID source : gymSources) {
        const bool found = std::ranges::find(neighbours, source) != neighbours.end();
        EXPECT_FALSE(found);
    }

    // Computers still has live in-edges so output must be non-empty.
    EXPECT_GT(neighbours.size(), 0U);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
