#include <algorithm>
#include <memory>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetInEdgesByTypeIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesByTypeIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "FileUtils.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

// One emitted (input-index, edge) row, flattened to raw values so the rows sort
// and compare without depending on the ID types' operators.
struct CollectedEdge {
    size_t _index {0};
    uint64_t _edgeID {0};
    uint64_t _neighbour {0};
    uint64_t _type {0};
};

// Drive a by-type out-edge writer over the whole input, gathering one row per
// emitted edge. maxCount is the per-fill row budget: a small value exercises the
// mid-span resume path across successive fill() calls.
void collectOutEdgesByType(const GraphReader& reader,
                           const ColumnNodeIDs* input,
                           EdgeTypeID edgeType,
                           size_t maxCount,
                           std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;
    ColumnEdgeTypes types;

    GetOutEdgesByTypeChunkWriter writer(reader.getView(), input, edgeType);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);
    writer.setEdgeTypes(&types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), targets[i].getValue(), types[i].getValue()});
        }
    }
}

// As collectOutEdgesByType, but for in-edges: the neighbour is the source.
void collectInEdgesByType(const GraphReader& reader,
                          const ColumnNodeIDs* input,
                          EdgeTypeID edgeType,
                          size_t maxCount,
                          std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs sources;
    ColumnEdgeTypes types;

    GetInEdgesByTypeChunkWriter writer(reader.getView(), input, edgeType);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setSrcIDs(&sources);
    writer.setEdgeTypes(&types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), sources[i].getValue(), types[i].getValue()});
        }
    }
}

// Gather every out-edge of the input via the unfiltered writer - ground truth
// that the by-type writer is compared against once filtered to a single type.
void collectAllOutEdges(const GraphReader& reader,
                        const ColumnNodeIDs* input,
                        std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;
    ColumnEdgeTypes types;

    GetOutEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);
    writer.setEdgeTypes(&types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), targets[i].getValue(), types[i].getValue()});
        }
    }
}

// Gather every in-edge of the input via the unfiltered writer.
void collectAllInEdges(const GraphReader& reader,
                       const ColumnNodeIDs* input,
                       std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs sources;
    ColumnEdgeTypes types;

    GetInEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setSrcIDs(&sources);
    writer.setEdgeTypes(&types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), sources[i].getValue(), types[i].getValue()});
        }
    }
}

// Keep only the rows whose edge type matches, preserving order.
void filterByType(const std::vector<CollectedEdge>& all,
                  EdgeTypeID edgeType,
                  std::vector<CollectedEdge>& out) {
    out.clear();
    for (const CollectedEdge& edge : all) {
        if (edge._type == edgeType.getValue()) {
            out.push_back(edge);
        }
    }
}

// The by-type writer and the filtered unfiltered writer walk nodes and edge
// spans in the same order, so the two row sequences must be identical.
void expectSameRows(const std::vector<CollectedEdge>& expected,
                    const std::vector<CollectedEdge>& actual) {
    ASSERT_EQ(expected.size(), actual.size());

    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(expected[i]._index, actual[i]._index);
        EXPECT_EQ(expected[i]._edgeID, actual[i]._edgeID);
        EXPECT_EQ(expected[i]._neighbour, actual[i]._neighbour);
        EXPECT_EQ(expected[i]._type, actual[i]._type);
    }
}

// Compare rows by (index, neighbour) as an order-independent multiset, so a test
// can pin the emitted content without hard-coding the edge-span traversal order.
void expectSameContent(std::vector<std::pair<size_t, uint64_t>> expected,
                       const std::vector<CollectedEdge>& actual) {
    std::vector<std::pair<size_t, uint64_t>> actualPairs;
    for (const CollectedEdge& edge : actual) {
        actualPairs.emplace_back(edge._index, edge._neighbour);
    }

    std::sort(expected.begin(), expected.end());
    std::sort(actualPairs.begin(), actualPairs.end());

    EXPECT_EQ(expected, actualPairs);
}

}

class GetEdgesByTypeIteratorTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID person = metadata.getOrCreateLabel("Person");
        const LabelSet labelset = LabelSet::fromList({person});

        _knows = metadata.getOrCreateEdgeType("KNOWS");
        _likes = metadata.getOrCreateEdgeType("LIKES");
        _worksAt = metadata.getOrCreateEdgeType("WORKS_AT");

        // Registered but never attached to an edge: an existing type that matches
        // nothing.
        _follows = metadata.getOrCreateEdgeType("FOLLOWS");

        // Single datapart, so the temporary IDs 0..5 are also the final node IDs.
        for (size_t i = 0; i < 6; i++) {
            builder.addNode(labelset);
        }

        // A mix of types per source node, and both directions per node, so that
        // filtering has something to keep and something to drop at every node.
        builder.addEdge(_knows, 0, 1);
        builder.addEdge(_likes, 0, 2);
        builder.addEdge(_knows, 0, 3);
        builder.addEdge(_likes, 1, 2);
        builder.addEdge(_worksAt, 2, 3);
        builder.addEdge(_knows, 4, 5);
        builder.addEdge(_likes, 4, 0);
        builder.addEdge(_knows, 5, 0);
        builder.addEdge(_worksAt, 5, 1);

        const auto res = change->access().submit(*_jobSystem);
        if (!res) {
            spdlog::error("Failed to submit change: {}", res.error().fmtMessage());
        }
        ASSERT_TRUE(res);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<db::JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph = nullptr;

    EdgeTypeID _knows;
    EdgeTypeID _likes;
    EdgeTypeID _worksAt;
    EdgeTypeID _follows;

    FileUtils::Path _logPath;
};

// For every registered edge type, the out-edges the by-type writer emits must
// equal the unfiltered out-edges filtered down to that type.
TEST_F(GetEdgesByTypeIteratorTest, outEdgesByTypeMatchFilteredUnfiltered) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> allEdges;
    collectAllOutEdges(reader, &input, allEdges);

    for (const EdgeTypeID edgeType : {_knows, _likes, _worksAt, _follows}) {
        std::vector<CollectedEdge> expected;
        filterByType(allEdges, edgeType, expected);

        std::vector<CollectedEdge> actual;
        collectOutEdgesByType(reader, &input, edgeType, ChunkConfig::CHUNK_SIZE, actual);

        expectSameRows(expected, actual);
    }
}

// Same equivalence for in-edges.
TEST_F(GetEdgesByTypeIteratorTest, inEdgesByTypeMatchFilteredUnfiltered) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> allEdges;
    collectAllInEdges(reader, &input, allEdges);

    for (const EdgeTypeID edgeType : {_knows, _likes, _worksAt, _follows}) {
        std::vector<CollectedEdge> expected;
        filterByType(allEdges, edgeType, expected);

        std::vector<CollectedEdge> actual;
        collectInEdgesByType(reader, &input, edgeType, ChunkConfig::CHUNK_SIZE, actual);

        expectSameRows(expected, actual);
    }
}

// Hand-derived expected content, so the differential tests above cannot be fooled
// by a matching bug in the unfiltered writer.
TEST_F(GetEdgesByTypeIteratorTest, outEdgesByTypeExplicit) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    {
        // KNOWS: 0->1, 0->3, 4->5, 5->0 as (input index, target).
        std::vector<CollectedEdge> knows;
        collectOutEdgesByType(reader, &input, _knows, ChunkConfig::CHUNK_SIZE, knows);
        expectSameContent({{0, 1}, {0, 3}, {4, 5}, {5, 0}}, knows);
    }

    {
        // LIKES: 0->2, 1->2, 4->0.
        std::vector<CollectedEdge> likes;
        collectOutEdgesByType(reader, &input, _likes, ChunkConfig::CHUNK_SIZE, likes);
        expectSameContent({{0, 2}, {1, 2}, {4, 0}}, likes);
    }

    {
        // WORKS_AT: 2->3, 5->1.
        std::vector<CollectedEdge> worksAt;
        collectOutEdgesByType(reader, &input, _worksAt, ChunkConfig::CHUNK_SIZE, worksAt);
        expectSameContent({{2, 3}, {5, 1}}, worksAt);
    }
}

TEST_F(GetEdgesByTypeIteratorTest, inEdgesByTypeExplicit) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    // KNOWS in-edges, neighbour is the source: node 0<-5, node 1<-0, node 3<-0,
    // node 5<-4, expressed as (input index, source).
    std::vector<CollectedEdge> knows;
    collectInEdgesByType(reader, &input, _knows, ChunkConfig::CHUNK_SIZE, knows);
    expectSameContent({{0, 5}, {1, 0}, {3, 0}, {5, 4}}, knows);
}

// An edge type present in the schema but on no edge yields nothing, in either
// direction. This is the storage-level counterpart of the interpreter's
// unknown-type short-circuit.
TEST_F(GetEdgesByTypeIteratorTest, absentEdgeTypeYieldsNoRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> outRows;
    collectOutEdgesByType(reader, &input, _follows, ChunkConfig::CHUNK_SIZE, outRows);
    EXPECT_TRUE(outRows.empty());

    std::vector<CollectedEdge> inRows;
    collectInEdgesByType(reader, &input, _follows, ChunkConfig::CHUNK_SIZE, inRows);
    EXPECT_TRUE(inRows.empty());
}

// The emitted index is relative to the input column, not the node ID: node 4 sits
// at input index 1 here, so its edges must be tagged 1.
TEST_F(GetEdgesByTypeIteratorTest, indexIsRelativeToInput) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 4};

    std::vector<CollectedEdge> knows;
    collectOutEdgesByType(reader, &input, _knows, ChunkConfig::CHUNK_SIZE, knows);

    // node 0 (index 0): 0->1, 0->3 ; node 4 (index 1): 4->5.
    expectSameContent({{0, 1}, {0, 3}, {1, 5}}, knows);

    // Still consistent with the unfiltered writer on this reduced input.
    std::vector<CollectedEdge> allEdges;
    collectAllOutEdges(reader, &input, allEdges);
    std::vector<CollectedEdge> expected;
    filterByType(allEdges, _knows, expected);
    expectSameRows(expected, knows);
}

// A tight row budget must not change the result: the writer resumes mid-span
// across fills and still produces exactly the full-budget rows, in order.
TEST_F(GetEdgesByTypeIteratorTest, respectsRowBudgetAcrossFills) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> wholeChunk;
    collectOutEdgesByType(reader, &input, _knows, ChunkConfig::CHUNK_SIZE, wholeChunk);

    for (const size_t budget : {size_t {1}, size_t {2}, size_t {3}}) {
        std::vector<CollectedEdge> chunked;
        collectOutEdgesByType(reader, &input, _knows, budget, chunked);
        expectSameRows(wholeChunk, chunked);
    }

    std::vector<CollectedEdge> wholeChunkIn;
    collectInEdgesByType(reader, &input, _likes, ChunkConfig::CHUNK_SIZE, wholeChunkIn);

    for (const size_t budget : {size_t {1}, size_t {2}}) {
        std::vector<CollectedEdge> chunked;
        collectInEdgesByType(reader, &input, _likes, budget, chunked);
        expectSameRows(wholeChunkIn, chunked);
    }
}

TEST_F(GetEdgesByTypeIteratorTest, emptyInputYieldsNoRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input;

    std::vector<CollectedEdge> outRows;
    collectOutEdgesByType(reader, &input, _knows, ChunkConfig::CHUNK_SIZE, outRows);
    EXPECT_TRUE(outRows.empty());

    std::vector<CollectedEdge> inRows;
    collectInEdgesByType(reader, &input, _knows, ChunkConfig::CHUNK_SIZE, inRows);
    EXPECT_TRUE(inRows.empty());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
