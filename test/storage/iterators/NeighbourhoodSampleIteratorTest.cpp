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

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(NeighbourhoodSampleIteratorTest, leafFirstInputYieldsSamples) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {2, 0};
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, 2);
    writer.setOutputColumns(nullptr, nullptr, nullptr, &dstIDs);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    EXPECT_GT(dstIDs.size(), 0U);
}

TEST_F(NeighbourhoodSampleIteratorTest, leafInputYieldsItsInEdges) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Computers (2), Eighties (3), Bio (4), Cooking (5) have no out-edges and 2, 1, 2 and
    // 2 in-edges. The sample is undirected, so it reads them, capped at 2 a node.
    const ColumnNodeIDs input = {2, 3, 4, 5};
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, 2);
    writer.setOutputColumns(nullptr, nullptr, nullptr, &dstIDs);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    EXPECT_EQ(dstIDs.size(), 7U);
}

TEST_F(NeighbourhoodSampleIteratorTest, sampleSizeCapRespected) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Remy (0) has 4 out-edges and 2 in-edges; sampleSize 2 must cap the output to 2.
    const ColumnNodeIDs input = {0};
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, 2);
    writer.setOutputColumns(nullptr, nullptr, nullptr, &dstIDs);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    EXPECT_EQ(dstIDs.size(), 2U);
}

TEST_F(NeighbourhoodSampleIteratorTest, deletedEdgesAreNotSampled) {
    // Delete one of Remy's out-edges (Remy=0 has 4: Adam, Ghosts, Computers, Eighties).
    // The sampler must not return it. Adam and Ghosts are reached by a second edge of
    // their own, so the assertion is on the edge rather than on the node it reaches.
    std::vector<EdgeID> edgeIDs;
    std::vector<EdgeTypeID> edgeTypes;
    std::vector<NodeID> targets;
    SimpleGraph::findOutEdges(_graph.get(), {0}, edgeIDs, edgeTypes, targets);
    ASSERT_FALSE(edgeIDs.empty());

    const EdgeID deletedEdge = edgeIDs[0];

    GraphWriter writer(_graph.get(), _jobSystem.get());
    writer.deleteEdge(deletedEdge);
    writer.submit();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {0};
    ColumnEdgeIDs sampledEdges;

    NeighbourhoodSampleChunkWriter chunkWriter(reader.getView(), &input, 4);
    chunkWriter.setOutputColumns(nullptr, &sampledEdges, nullptr, nullptr);
    chunkWriter.fill(ChunkConfig::CHUNK_SIZE);

    for (const EdgeID edgeID : sampledEdges) {
        EXPECT_NE(edgeID, deletedEdge);
    }
}

TEST_F(NeighbourhoodSampleIteratorTest, allEdgesDeletedYieldsEmpty) {
    // Remy's neighbourhood is his 4 out-edges plus the 2 edges into him, which are
    // out-edges of Adam (1) and Ghosts (6). With all of them gone the sample is empty.
    std::vector<EdgeID> edgeIDs;
    std::vector<EdgeTypeID> edgeTypes;
    std::vector<NodeID> targets;
    SimpleGraph::findOutEdges(_graph.get(), {0, 1, 6}, edgeIDs, edgeTypes, targets);
    ASSERT_FALSE(edgeIDs.empty());

    GraphWriter writer(_graph.get(), _jobSystem.get());
    for (const EdgeID edgeID : edgeIDs) {
        writer.deleteEdge(edgeID);
    }
    writer.submit();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {0};
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter chunkWriter(reader.getView(), &input, 4);
    chunkWriter.setOutputColumns(nullptr, nullptr, nullptr, &dstIDs);
    chunkWriter.fill(ChunkConfig::CHUNK_SIZE);

    EXPECT_EQ(dstIDs.size(), 0U);
}

TEST_F(NeighbourhoodSampleIteratorTest, deletedEdgesNotSampledAcrossMultipleNodes) {
    // Delete Adam's out-edges; sampling both Remy and Adam must return none of them and
    // still reach Remy's own neighbours.
    std::vector<EdgeID> adamEdgeIDs;
    std::vector<EdgeTypeID> adamEdgeTypes;
    std::vector<NodeID> adamTargets;
    SimpleGraph::findOutEdges(_graph.get(), {1}, adamEdgeIDs, adamEdgeTypes, adamTargets);
    ASSERT_FALSE(adamEdgeIDs.empty());

    GraphWriter writer(_graph.get(), _jobSystem.get());
    for (const EdgeID edgeID : adamEdgeIDs) {
        writer.deleteEdge(edgeID);
    }
    writer.submit();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {0, 1};
    ColumnEdgeIDs sampledEdges;
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter chunkWriter(reader.getView(), &input, 4);
    chunkWriter.setOutputColumns(nullptr, &sampledEdges, nullptr, &dstIDs);
    chunkWriter.fill(ChunkConfig::CHUNK_SIZE);

    // None of Adam's deleted edges must appear.
    for (const EdgeID edgeID : adamEdgeIDs) {
        const bool found = std::ranges::find(sampledEdges, edgeID) != sampledEdges.end();
        EXPECT_FALSE(found);
    }

    // Remy still has live edges so output must be non-empty.
    EXPECT_GT(dstIDs.size(), 0U);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
