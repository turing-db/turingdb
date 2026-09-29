#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

// One emitted (input index, edge, neighbour) row as raw values
struct CollectedEdge {
    size_t _index {0};
    uint64_t _edgeID {0};
    uint64_t _neighbour {0};
};

void collectOutEdges(const GraphReader& reader,
                     const ColumnNodeIDs* input,
                     std::span<const ColumnEdgeIDs* const> excluded,
                     size_t maxCount,
                     std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;

    GetOutEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);
    writer.setDistinctFrom(excluded);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        for (size_t row = 0; row < indices.size(); row++) {
            out.push_back({indices[row], edgeIDs[row].getValue(), targets[row].getValue()});
        }
    }
}

void collectInEdges(const GraphReader& reader,
                    const ColumnNodeIDs* input,
                    std::span<const ColumnEdgeIDs* const> excluded,
                    size_t maxCount,
                    std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs sources;

    GetInEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setSrcIDs(&sources);
    writer.setDistinctFrom(excluded);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        for (size_t row = 0; row < indices.size(); row++) {
            out.push_back({indices[row], edgeIDs[row].getValue(), sources[row].getValue()});
        }
    }
}

uint64_t edgeTo(const std::vector<CollectedEdge>& edges, size_t index, uint64_t neighbour) {
    for (const CollectedEdge& edge : edges) {
        if (edge._index == index && edge._neighbour == neighbour) {
            return edge._edgeID;
        }
    }

    throw TuringException("No such edge in the collected rows");
}

void expectSameContent(std::vector<std::pair<size_t, uint64_t>> expected, const std::vector<CollectedEdge>& actual) {
    std::vector<std::pair<size_t, uint64_t>> actualPairs;
    for (const CollectedEdge& edge : actual) {
        actualPairs.emplace_back(edge._index, edge._neighbour);
    }

    std::sort(expected.begin(), expected.end());
    std::sort(actualPairs.begin(), actualPairs.end());
    EXPECT_EQ(expected, actualPairs);
}

}

// The writers leave out of each input row the edges its exclusion columns hold: an out-run
// by the arithmetic over its consecutive IDs, an in-run by a scan. Node 1 has two out-edges
// (to 0 and 2) and two in-edges (from 0 and 2).
class EdgeExclusionTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID label = metadata.getOrCreateLabel("Node");
        const LabelSet labelset = LabelSet::fromList({label});
        const EdgeTypeID link = metadata.getOrCreateEdgeType("LINK");

        for (size_t node = 0; node < 3; node++) {
            builder.addNode(labelset);
        }

        builder.addEdge(link, 0, 1);
        builder.addEdge(link, 0, 2);
        builder.addEdge(link, 1, 0);
        builder.addEdge(link, 1, 2);
        builder.addEdge(link, 2, 1);

        const auto res = change->access().submit(*_jobSystem);
        ASSERT_TRUE(res);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(EdgeExclusionTest, leavesTheExcludedEdgeOutOfAnOutRun) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1};

    std::vector<CollectedEdge> all;
    collectOutEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);
    expectSameContent({{0, 0}, {0, 2}, {1, 0}, {1, 2}}, all);

    // Row 0 leaves 1 -> 0 out, row 1 an edge node 1 does not have
    std::vector<CollectedEdge> allOfZero;
    const ColumnNodeIDs zero = {0};
    collectOutEdges(reader, &zero, {}, ChunkConfig::CHUNK_SIZE, allOfZero);

    const ColumnEdgeIDs excluded = {EdgeID(edgeTo(all, 0, 0)), EdgeID(edgeTo(allOfZero, 0, 1))};
    const ColumnEdgeIDs* const columns[] = {&excluded};

    std::vector<CollectedEdge> pruned;
    collectOutEdges(reader, &input, columns, ChunkConfig::CHUNK_SIZE, pruned);
    expectSameContent({{0, 2}, {1, 0}, {1, 2}}, pruned);
}

TEST_F(EdgeExclusionTest, leavesTheExcludedEdgeOutOfAnInRun) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1};

    std::vector<CollectedEdge> all;
    collectInEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);
    expectSameContent({{0, 0}, {0, 2}, {1, 0}, {1, 2}}, all);

    const ColumnEdgeIDs excluded = {EdgeID(edgeTo(all, 0, 0)), EdgeID(edgeTo(all, 1, 2))};
    const ColumnEdgeIDs* const columns[] = {&excluded};

    std::vector<CollectedEdge> pruned;
    collectInEdges(reader, &input, columns, ChunkConfig::CHUNK_SIZE, pruned);
    expectSameContent({{0, 2}, {1, 0}}, pruned);
}

TEST_F(EdgeExclusionTest, prunesARunSplitAcrossFills) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1};

    std::vector<CollectedEdge> all;
    collectOutEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);

    const ColumnEdgeIDs excluded = {EdgeID(edgeTo(all, 0, 2)), EdgeID(edgeTo(all, 1, 0))};
    const ColumnEdgeIDs* const columns[] = {&excluded};

    std::vector<CollectedEdge> pruned;
    collectOutEdges(reader, &input, columns, 1, pruned);
    expectSameContent({{0, 0}, {1, 2}}, pruned);
}

TEST_F(EdgeExclusionTest, twoColumnsLeaveTwoEdgesOut) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1};

    std::vector<CollectedEdge> all;
    collectOutEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);

    const ColumnEdgeIDs first = {EdgeID(edgeTo(all, 0, 0))};
    const ColumnEdgeIDs second = {EdgeID(edgeTo(all, 0, 2))};
    const ColumnEdgeIDs* const columns[] = {&first, &second};

    std::vector<CollectedEdge> pruned;
    collectOutEdges(reader, &input, columns, ChunkConfig::CHUNK_SIZE, pruned);
    EXPECT_TRUE(pruned.empty());
}
