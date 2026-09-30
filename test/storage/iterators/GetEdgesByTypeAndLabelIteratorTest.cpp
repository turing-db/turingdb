#include <algorithm>
#include <memory>
#include <span>
#include <tuple>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetInEdgesByTypeAndLabelIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesByTypeAndLabelIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"
#include "writers/GraphWriter.h"
#include "writers/MetadataBuilder.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

struct CollectedEdge {
    size_t _index {0};
    uint64_t _edgeID {0};
    uint64_t _neighbour {0};
    uint64_t _type {0};
};

struct CollectedChunk {
    ColumnVector<size_t> _indices;
    ColumnEdgeIDs _edgeIDs;
    ColumnNodeIDs _neighbours;
    ColumnEdgeTypes _types;
};

void appendChunk(const CollectedChunk& chunk, std::vector<CollectedEdge>& out) {
    ASSERT_EQ(chunk._indices.size(), chunk._edgeIDs.size());
    ASSERT_EQ(chunk._neighbours.size(), chunk._edgeIDs.size());
    ASSERT_EQ(chunk._types.size(), chunk._edgeIDs.size());

    for (size_t row = 0; row < chunk._indices.size(); row++) {
        out.push_back({chunk._indices[row],
                       chunk._edgeIDs[row].getValue(),
                       chunk._neighbours[row].getValue(),
                       chunk._types[row].getValue()});
    }
}

void collectOutEdges(const GraphReader& reader,
                     const ColumnNodeIDs* input,
                     std::span<const EdgeTypeID> edgeTypes,
                     const LabelSetHandle& labelset,
                     size_t maxCount,
                     std::vector<CollectedEdge>& out) {
    CollectedChunk chunk;

    GetOutEdgesByTypeAndLabelChunkWriter writer(reader.getView(), input, edgeTypes, labelset);
    writer.setIndices(&chunk._indices);
    writer.setEdgeIDs(&chunk._edgeIDs);
    writer.setTgtIDs(&chunk._neighbours);
    writer.setEdgeTypes(&chunk._types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        appendChunk(chunk, out);
    }
}

void collectInEdges(const GraphReader& reader,
                    const ColumnNodeIDs* input,
                    std::span<const EdgeTypeID> edgeTypes,
                    const LabelSetHandle& labelset,
                    size_t maxCount,
                    std::vector<CollectedEdge>& out) {
    CollectedChunk chunk;

    GetInEdgesByTypeAndLabelChunkWriter writer(reader.getView(), input, edgeTypes, labelset);
    writer.setIndices(&chunk._indices);
    writer.setEdgeIDs(&chunk._edgeIDs);
    writer.setSrcIDs(&chunk._neighbours);
    writer.setEdgeTypes(&chunk._types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        appendChunk(chunk, out);
    }
}

void collectAllOutEdges(const GraphReader& reader,
                        const ColumnNodeIDs* input,
                        std::vector<CollectedEdge>& out) {
    CollectedChunk chunk;

    GetOutEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&chunk._indices);
    writer.setEdgeIDs(&chunk._edgeIDs);
    writer.setTgtIDs(&chunk._neighbours);
    writer.setEdgeTypes(&chunk._types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);
        appendChunk(chunk, out);
    }
}

void collectAllInEdges(const GraphReader& reader,
                       const ColumnNodeIDs* input,
                       std::vector<CollectedEdge>& out) {
    CollectedChunk chunk;

    GetInEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&chunk._indices);
    writer.setEdgeIDs(&chunk._edgeIDs);
    writer.setSrcIDs(&chunk._neighbours);
    writer.setEdgeTypes(&chunk._types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);
        appendChunk(chunk, out);
    }
}

void filterByTypeAndNeighbourLabel(const GraphReader& reader,
                                   const std::vector<CollectedEdge>& all,
                                   std::span<const EdgeTypeID> edgeTypes,
                                   const LabelSetHandle& labelset,
                                   std::vector<CollectedEdge>& out) {
    out.clear();
    for (const CollectedEdge& edge : all) {
        const bool typeMatches = std::ranges::find(edgeTypes, EdgeTypeID {edge._type}) != edgeTypes.end();
        const LabelSetHandle labels = reader.getNodeLabelSet(NodeID {edge._neighbour});
        const bool labelMatches = labels.isValid() && labels.hasAtLeastLabels(labelset);

        if (typeMatches && labelMatches) {
            out.push_back(edge);
        }
    }
}

void expectSameRows(const std::vector<CollectedEdge>& expected,
                    const std::vector<CollectedEdge>& actual) {
    ASSERT_EQ(expected.size(), actual.size());

    for (size_t row = 0; row < expected.size(); row++) {
        EXPECT_EQ(expected[row]._index, actual[row]._index);
        EXPECT_EQ(expected[row]._edgeID, actual[row]._edgeID);
        EXPECT_EQ(expected[row]._neighbour, actual[row]._neighbour);
        EXPECT_EQ(expected[row]._type, actual[row]._type);
    }
}

void expectSameContent(std::vector<std::tuple<size_t, uint64_t, uint64_t>> expected,
                       const std::vector<CollectedEdge>& actual) {
    std::vector<std::tuple<size_t, uint64_t, uint64_t>> actualRows;
    for (const CollectedEdge& edge : actual) {
        actualRows.emplace_back(edge._index, edge._neighbour, edge._type);
    }

    std::sort(expected.begin(), expected.end());
    std::sort(actualRows.begin(), actualRows.end());

    EXPECT_EQ(expected, actualRows);
}

}

class GetEdgesByTypeAndLabelIteratorTest : public TuringTest {
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

        _person = LabelSet::fromList({person});
        _robot = LabelSet::fromList({robot});
        _personRobot = LabelSet::fromList({person, robot});

        _knows = metadata.getOrCreateEdgeType("KNOWS");
        _likes = metadata.getOrCreateEdgeType("LIKES");
        _hates = metadata.getOrCreateEdgeType("HATES");

        // Nodes are added grouped by label set so they commit as 0..5: 0 and 1 are
        // Person, 2 and 3 Person and Robot, 4 and 5 Robot.
        builder.addNode(_person);
        builder.addNode(_person);
        builder.addNode(_personRobot);
        builder.addNode(_personRobot);
        builder.addNode(_robot);
        builder.addNode(_robot);

        builder.addEdge(_knows, 0, 1);
        builder.addEdge(_knows, 0, 2);
        builder.addEdge(_knows, 1, 2);
        builder.addEdge(_knows, 4, 5);
        builder.addEdge(_knows, 5, 0);
        builder.addEdge(_likes, 0, 3);
        builder.addEdge(_likes, 2, 3);
        builder.addEdge(_likes, 4, 0);
        builder.addEdge(_likes, 5, 1);
        builder.addEdge(_hates, 0, 4);
        builder.addEdge(_hates, 3, 5);

        const auto res = change->access().submit(*_jobSystem);
        if (!res) {
            spdlog::error("Failed to submit change: {}", res.error().fmtMessage());
        }
        ASSERT_TRUE(res);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;

    LabelSet _person;
    LabelSet _robot;
    LabelSet _personRobot;

    EdgeTypeID _knows;
    EdgeTypeID _likes;
    EdgeTypeID _hates;
};

TEST_F(GetEdgesByTypeAndLabelIteratorTest, outEdgesMatchFilteredUnfiltered) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> allEdges;
    collectAllOutEdges(reader, &input, allEdges);

    const std::vector<std::vector<EdgeTypeID>> typeSets = {{_knows}, {_likes}, {_hates}, {_knows, _likes}};

    for (const std::vector<EdgeTypeID>& edgeTypes : typeSets) {
        for (const LabelSet& labelset : {_person, _robot, _personRobot}) {
            const LabelSetHandle handle {labelset};

            std::vector<CollectedEdge> expected;
            filterByTypeAndNeighbourLabel(reader, allEdges, edgeTypes, handle, expected);

            std::vector<CollectedEdge> actual;
            collectOutEdges(reader, &input, edgeTypes, handle, ChunkConfig::CHUNK_SIZE, actual);

            expectSameRows(expected, actual);
        }
    }
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, inEdgesMatchFilteredUnfiltered) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    std::vector<CollectedEdge> allEdges;
    collectAllInEdges(reader, &input, allEdges);

    const std::vector<std::vector<EdgeTypeID>> typeSets = {{_knows}, {_likes}, {_hates}, {_knows, _likes}};

    for (const std::vector<EdgeTypeID>& edgeTypes : typeSets) {
        for (const LabelSet& labelset : {_person, _robot, _personRobot}) {
            const LabelSetHandle handle {labelset};

            std::vector<CollectedEdge> expected;
            filterByTypeAndNeighbourLabel(reader, allEdges, edgeTypes, handle, expected);

            std::vector<CollectedEdge> actual;
            collectInEdges(reader, &input, edgeTypes, handle, ChunkConfig::CHUNK_SIZE, actual);

            expectSameRows(expected, actual);
        }
    }
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, outEdgesExplicit) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    const uint64_t knows = _knows.getValue();
    const uint64_t likes = _likes.getValue();

    const std::vector<EdgeTypeID> knowsOnly = {_knows};
    const std::vector<EdgeTypeID> knowsOrLikes = {_knows, _likes};

    std::vector<CollectedEdge> knowsPeople;
    collectOutEdges(reader, &input, knowsOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, knowsPeople);
    expectSameContent({{0, 1, knows}, {0, 2, knows}, {1, 2, knows}, {5, 0, knows}}, knowsPeople);

    std::vector<CollectedEdge> knowsRobots;
    collectOutEdges(reader, &input, knowsOnly, LabelSetHandle {_robot}, ChunkConfig::CHUNK_SIZE, knowsRobots);
    expectSameContent({{0, 2, knows}, {1, 2, knows}, {4, 5, knows}}, knowsRobots);

    std::vector<CollectedEdge> eitherToBoth;
    collectOutEdges(reader, &input, knowsOrLikes, LabelSetHandle {_personRobot}, ChunkConfig::CHUNK_SIZE, eitherToBoth);
    expectSameContent({{0, 2, knows}, {1, 2, knows}, {0, 3, likes}, {2, 3, likes}}, eitherToBoth);
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, inEdgesExplicit) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    const uint64_t likes = _likes.getValue();
    const std::vector<EdgeTypeID> likesOnly = {_likes};

    // LIKES edges with a Robot source are 2->3, 4->0 and 5->1, as (input index, source)
    std::vector<CollectedEdge> likedByRobots;
    collectInEdges(reader, &input, likesOnly, LabelSetHandle {_robot}, ChunkConfig::CHUNK_SIZE, likedByRobots);
    expectSameContent({{3, 2, likes}, {0, 4, likes}, {1, 5, likes}}, likedByRobots);
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, typeMatchWithoutLabelMatchYieldsNoRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    // Both HATES edges, 0->4 and 3->5, end on a node that is only a Robot
    const std::vector<EdgeTypeID> hatesOnly = {_hates};

    std::vector<CollectedEdge> outRows;
    collectOutEdges(reader, &input, hatesOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, outRows);
    EXPECT_TRUE(outRows.empty());

    // The KNOWS sources are 0, 1, 4 and 5, none of which carries both labels
    const std::vector<EdgeTypeID> knowsOnly = {_knows};

    std::vector<CollectedEdge> inRows;
    collectInEdges(reader, &input, knowsOnly, LabelSetHandle {_personRobot}, ChunkConfig::CHUNK_SIZE, inRows);
    EXPECT_TRUE(inRows.empty());
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, indexIsRelativeToInput) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {5, 0};

    const uint64_t knows = _knows.getValue();
    const std::vector<EdgeTypeID> knowsOnly = {_knows};

    std::vector<CollectedEdge> knowsPeople;
    collectOutEdges(reader, &input, knowsOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, knowsPeople);
    expectSameContent({{0, 0, knows}, {1, 1, knows}, {1, 2, knows}}, knowsPeople);
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, respectsRowBudgetAcrossFills) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};

    const std::vector<EdgeTypeID> knowsOrLikes = {_knows, _likes};

    std::vector<CollectedEdge> wholeChunk;
    collectOutEdges(reader, &input, knowsOrLikes, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, wholeChunk);
    ASSERT_EQ(wholeChunk.size(), size_t {8});

    for (const size_t budget : {size_t {1}, size_t {2}, size_t {3}}) {
        std::vector<CollectedEdge> chunked;
        collectOutEdges(reader, &input, knowsOrLikes, LabelSetHandle {_person}, budget, chunked);
        expectSameRows(wholeChunk, chunked);
    }

    std::vector<CollectedEdge> wholeChunkIn;
    collectInEdges(reader, &input, knowsOrLikes, LabelSetHandle {_robot}, ChunkConfig::CHUNK_SIZE, wholeChunkIn);
    ASSERT_EQ(wholeChunkIn.size(), size_t {5});

    for (const size_t budget : {size_t {1}, size_t {2}}) {
        std::vector<CollectedEdge> chunked;
        collectInEdges(reader, &input, knowsOrLikes, LabelSetHandle {_robot}, budget, chunked);
        expectSameRows(wholeChunkIn, chunked);
    }
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, emptyInputYieldsNoRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input;

    const std::vector<EdgeTypeID> knowsOnly = {_knows};

    std::vector<CollectedEdge> outRows;
    collectOutEdges(reader, &input, knowsOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, outRows);
    EXPECT_TRUE(outRows.empty());

    std::vector<CollectedEdge> inRows;
    collectInEdges(reader, &input, knowsOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, inRows);
    EXPECT_TRUE(inRows.empty());
}

TEST_F(GetEdgesByTypeAndLabelIteratorTest, deletedEdgeIsDroppedFromEveryColumn) {
    const ColumnNodeIDs input = {0, 1, 2, 3, 4, 5};
    const std::vector<EdgeTypeID> knowsOnly = {_knows};

    std::vector<CollectedEdge> before;
    {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        collectOutEdges(reader, &input, knowsOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, before);
    }
    ASSERT_EQ(before.size(), size_t {4});

    const CollectedEdge deleted = before[1];
    {
        GraphWriter writer(_graph.get(), _jobSystem.get());
        writer.deleteEdge(EdgeID {deleted._edgeID});
        writer.submit();
    }

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<CollectedEdge> expected = before;
    expected.erase(expected.begin() + 1);

    std::vector<CollectedEdge> outRows;
    collectOutEdges(reader, &input, knowsOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, outRows);
    expectSameRows(expected, outRows);

    std::vector<CollectedEdge> allInEdges;
    collectAllInEdges(reader, &input, allInEdges);

    std::vector<CollectedEdge> expectedIn;
    filterByTypeAndNeighbourLabel(reader, allInEdges, knowsOnly, LabelSetHandle {_person}, expectedIn);

    std::vector<CollectedEdge> inRows;
    collectInEdges(reader, &input, knowsOnly, LabelSetHandle {_person}, ChunkConfig::CHUNK_SIZE, inRows);
    expectSameRows(expectedIn, inRows);

    for (const CollectedEdge& edge : inRows) {
        EXPECT_NE(edge._edgeID, deleted._edgeID);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
