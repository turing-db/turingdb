#include <algorithm>
#include <memory>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetInEdgesByLabelIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesByLabelIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
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
};

// Drive a by-label out-edge writer over the whole input, gathering one row per
// emitted edge. maxCount is the per-fill row budget: a small value exercises the
// mid-span resume path across successive fill() calls.
void collectOutEdgesByLabel(const GraphReader& reader,
                            const ColumnNodeIDs* input,
                            const LabelSetHandle& labelset,
                            size_t maxCount,
                            std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;

    GetOutEdgesByLabelChunkWriter writer(reader.getView(), input, labelset);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), targets[i].getValue()});
        }
    }
}

// As collectOutEdgesByLabel, but for in-edges: the neighbour is the source, and it is
// its labels the writer keeps the edge on.
void collectInEdgesByLabel(const GraphReader& reader,
                           const ColumnNodeIDs* input,
                           const LabelSetHandle& labelset,
                           size_t maxCount,
                           std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs sources;

    GetInEdgesByLabelChunkWriter writer(reader.getView(), input, labelset);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setSrcIDs(&sources);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), sources[i].getValue()});
        }
    }
}

// Gather every out-edge of the input via the unfiltered writer - ground truth that the
// by-label writer is compared against once filtered to one label set.
void collectAllOutEdges(const GraphReader& reader,
                        const ColumnNodeIDs* input,
                        std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;

    GetOutEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), targets[i].getValue()});
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

    GetInEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setSrcIDs(&sources);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);

        for (size_t i = 0; i < indices.size(); i++) {
            out.push_back({indices[i], edgeIDs[i].getValue(), sources[i].getValue()});
        }
    }
}

// Keep only the rows whose neighbour carries at least the label set, preserving order.
void filterByNeighbourLabel(const GraphReader& reader,
                            const std::vector<CollectedEdge>& all,
                            const LabelSetHandle& labelset,
                            std::vector<CollectedEdge>& out) {
    out.clear();
    for (const CollectedEdge& edge : all) {
        const LabelSetHandle labels = reader.getNodeLabelSet(NodeID {edge._neighbour});

        if (labels.isValid() && labels.hasAtLeastLabels(labelset)) {
            out.push_back(edge);
        }
    }
}

// The by-label writer and the filtered unfiltered writer walk nodes and edge spans in the
// same order, so the two row sequences must be identical.
void expectSameRows(const std::vector<CollectedEdge>& expected,
                    const std::vector<CollectedEdge>& actual) {
    ASSERT_EQ(expected.size(), actual.size());

    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(expected[i]._index, actual[i]._index);
        EXPECT_EQ(expected[i]._edgeID, actual[i]._edgeID);
        EXPECT_EQ(expected[i]._neighbour, actual[i]._neighbour);
    }
}

// Compare rows by (index, neighbour) as an order-independent multiset, so a test can pin
// the emitted content without hard-coding the edge-span traversal order.
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

class GetEdgesByLabelIteratorTest : public TuringTest {
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
        const LabelID robot = metadata.getOrCreateLabel("Robot");

        // Registered but carried by no node: an existing label that matches nothing.
        const LabelID ghost = metadata.getOrCreateLabel("Ghost");

        _person = LabelSet::fromList({person});
        _robot = LabelSet::fromList({robot});
        _personRobot = LabelSet::fromList({person, robot});
        _ghost = LabelSet::fromList({ghost});

        const EdgeTypeID knows = metadata.getOrCreateEdgeType("KNOWS");

        // A datapart groups its nodes by label set, so the nodes are added in that order
        // to keep the IDs 0..5 the ones they commit as: 0 and 1 carry Person, 2 and 3
        // both labels - what a conjunction keeps that either label alone does not - and
        // 4 and 5 Robot.
        builder.addNode(_person);
        builder.addNode(_person);
        builder.addNode(_personRobot);
        builder.addNode(_personRobot);
        builder.addNode(_robot);
        builder.addNode(_robot);

        // Every node has an edge to each side of the split, so filtering has something to
        // keep and something to drop at every one of them.
        builder.addEdge(knows, 0, 1);
        builder.addEdge(knows, 0, 2);
        builder.addEdge(knows, 0, 3);
        builder.addEdge(knows, 1, 2);
        builder.addEdge(knows, 2, 3);
        builder.addEdge(knows, 4, 5);
        builder.addEdge(knows, 4, 0);
        builder.addEdge(knows, 5, 0);
        builder.addEdge(knows, 5, 1);

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

    LabelSet _person;
    LabelSet _robot;
    LabelSet _personRobot;
    LabelSet _ghost;

    FileUtils::Path _logPath;
};

// For every label set, the out-edges the by-label writer emits must equal the unfiltered
// out-edges filtered down to the targets carrying it.
TEST_F(GetEdgesByLabelIteratorTest, outEdgesByLabelMatchFilteredUnfiltered) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> allEdges;
    collectAllOutEdges(reader, &input, allEdges);

    for (const LabelSet& labelset : {_person, _robot, _personRobot, _ghost}) {
        const LabelSetHandle handle {labelset};

        std::vector<CollectedEdge> expected;
        filterByNeighbourLabel(reader, allEdges, handle, expected);

        std::vector<CollectedEdge> actual;
        collectOutEdgesByLabel(reader, &input, handle, ChunkConfig::CHUNK_SIZE, actual);

        expectSameRows(expected, actual);
    }
}

// Same equivalence for in-edges, where the labelled end is the source.
TEST_F(GetEdgesByLabelIteratorTest, inEdgesByLabelMatchFilteredUnfiltered) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> allEdges;
    collectAllInEdges(reader, &input, allEdges);

    for (const LabelSet& labelset : {_person, _robot, _personRobot, _ghost}) {
        const LabelSetHandle handle {labelset};

        std::vector<CollectedEdge> expected;
        filterByNeighbourLabel(reader, allEdges, handle, expected);

        std::vector<CollectedEdge> actual;
        collectInEdgesByLabel(reader, &input, handle, ChunkConfig::CHUNK_SIZE, actual);

        expectSameRows(expected, actual);
    }
}

// Hand-derived expected content, so the differential tests above cannot be fooled by a
// matching bug in the unfiltered writer.
TEST_F(GetEdgesByLabelIteratorTest, outEdgesByLabelExplicit) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    {
        // Person targets are 0 to 3, so every edge but 4->5 is kept, as (input index,
        // target).
        std::vector<CollectedEdge> people;
        collectOutEdgesByLabel(reader, &input, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, people);
        expectSameContent({{0, 1}, {0, 2}, {0, 3}, {1, 2}, {2, 3}, {4, 0}, {5, 0}, {5, 1}}, people);
    }

    {
        // Robot targets are 2 to 5.
        std::vector<CollectedEdge> robots;
        collectOutEdgesByLabel(reader, &input, LabelSetHandle {_robot}, ChunkConfig::CHUNK_SIZE, robots);
        expectSameContent({{0, 2}, {0, 3}, {1, 2}, {2, 3}, {4, 5}}, robots);
    }

    {
        // Only 2 and 3 carry both labels.
        std::vector<CollectedEdge> both;
        collectOutEdgesByLabel(reader, &input, LabelSetHandle {_personRobot}, ChunkConfig::CHUNK_SIZE, both);
        expectSameContent({{0, 2}, {0, 3}, {1, 2}, {2, 3}}, both);
    }
}

TEST_F(GetEdgesByLabelIteratorTest, inEdgesByLabelExplicit) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    // In-edges whose source is a Person: from 0 (0->1, 0->2, 0->3), from 1 (1->2) and
    // from 2 (2->3), expressed as (input index, source).
    std::vector<CollectedEdge> people;
    collectInEdgesByLabel(reader, &input, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, people);
    expectSameContent({{1, 0}, {2, 0}, {2, 1}, {3, 0}, {3, 2}}, people);
}

// A label present in the schema and on no node yields nothing, in either direction. This
// is the storage-level counterpart of the interpreter's unmatchable-label short-circuit.
TEST_F(GetEdgesByLabelIteratorTest, absentLabelYieldsNoRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> outRows;
    collectOutEdgesByLabel(reader, &input, LabelSetHandle {_ghost}, ChunkConfig::CHUNK_SIZE, outRows);
    EXPECT_TRUE(outRows.empty());

    std::vector<CollectedEdge> inRows;
    collectInEdgesByLabel(reader, &input, LabelSetHandle {_ghost}, ChunkConfig::CHUNK_SIZE, inRows);
    EXPECT_TRUE(inRows.empty());
}

// The emitted index is relative to the input column, not the node ID: node 4 sits at input
// index 1 here, so its edges must be tagged 1.
TEST_F(GetEdgesByLabelIteratorTest, indexIsRelativeToInput) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 4};

    std::vector<CollectedEdge> people;
    collectOutEdgesByLabel(reader, &input, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, people);

    // node 0 (index 0): 0->1, 0->2, 0->3 ; node 4 (index 1): 4->0, since 4->5 leaves the
    // labels.
    expectSameContent({{0, 1}, {0, 2}, {0, 3}, {1, 0}}, people);

    std::vector<CollectedEdge> allEdges;
    collectAllOutEdges(reader, &input, allEdges);
    std::vector<CollectedEdge> expected;
    filterByNeighbourLabel(reader, allEdges, LabelSetHandle {_person}, expected);
    expectSameRows(expected, people);
}

// A tight row budget must not change the result: the writer resumes mid-span across fills
// and still produces exactly the full-budget rows, in order.
TEST_F(GetEdgesByLabelIteratorTest, respectsRowBudgetAcrossFills) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> wholeChunk;
    collectOutEdgesByLabel(reader, &input, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, wholeChunk);

    for (const size_t budget : {size_t {1}, size_t {2}, size_t {3}}) {
        std::vector<CollectedEdge> chunked;
        collectOutEdgesByLabel(reader, &input, LabelSetHandle {_person}, budget, chunked);
        expectSameRows(wholeChunk, chunked);
    }

    std::vector<CollectedEdge> wholeChunkIn;
    collectInEdgesByLabel(reader, &input, LabelSetHandle {_robot}, ChunkConfig::CHUNK_SIZE, wholeChunkIn);

    for (const size_t budget : {size_t {1}, size_t {2}}) {
        std::vector<CollectedEdge> chunked;
        collectInEdgesByLabel(reader, &input, LabelSetHandle {_robot}, budget, chunked);
        expectSameRows(wholeChunkIn, chunked);
    }
}

TEST_F(GetEdgesByLabelIteratorTest, emptyInputYieldsNoRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input;

    std::vector<CollectedEdge> outRows;
    collectOutEdgesByLabel(reader, &input, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, outRows);
    EXPECT_TRUE(outRows.empty());

    std::vector<CollectedEdge> inRows;
    collectInEdgesByLabel(reader, &input, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, inRows);
    EXPECT_TRUE(inRows.empty());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
