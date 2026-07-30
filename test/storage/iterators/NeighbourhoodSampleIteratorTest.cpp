#include <gtest/gtest.h>

#include <memory>

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/NeighbourhoodSampleIterator.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

class NeighbourhoodSampleIteratorTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(NeighbourhoodSampleIteratorTest, leafFirstInputYieldsSamples) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs input = {2, 0};
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, 2);
    writer.setOutputColumns(nullptr, nullptr, nullptr, &dstIDs);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    EXPECT_GT(dstIDs.size(), 0u);
}

TEST_F(NeighbourhoodSampleIteratorTest, allLeafInputYieldsEmpty) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Computers (2), Eighties (3), Bio (4), Cooking (5) all have no out-edges.
    const ColumnNodeIDs input = {2, 3, 4, 5};
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, 2);
    writer.setOutputColumns(nullptr, nullptr, nullptr, &dstIDs);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    EXPECT_EQ(dstIDs.size(), 0u);
}

TEST_F(NeighbourhoodSampleIteratorTest, sampleSizeCapRespected) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Remy (0) has 4 out-edges; sampleSize 2 must cap the output to exactly 2.
    const ColumnNodeIDs input = {0};
    ColumnNodeIDs dstIDs;

    NeighbourhoodSampleChunkWriter writer(reader.getView(), &input, 2);
    writer.setOutputColumns(nullptr, nullptr, nullptr, &dstIDs);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    EXPECT_EQ(dstIDs.size(), 2u);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 100;
    });
}
