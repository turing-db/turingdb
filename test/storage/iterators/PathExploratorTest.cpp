#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathExplorator.h"
#include "list/PathTrie.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

bool nothingPasses(uint64_t, uint64_t, uint64_t) {
    return false;
}

}

// A graph submitted in two commits, so the second commit's edges among first-commit nodes
// are patch edges: 0->1->2->0 is a 3-cycle, 1->0 closes a 2-cycle with 0->1, 2 carries a
// self-loop, 3->4->5 is a chain, and the second commit adds node 6 with 5->6, 6->0 and the
// patch edge 3->0 between two existing nodes. Every node carries the same label, so within
// each part the temporary IDs are the final ones.
class PathExploratorTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = 7;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _typeA = metadata.getOrCreateEdgeType("A");
            _typeB = metadata.getOrCreateEdgeType("B");

            for (size_t node = 0; node < 6; node++) {
                builder.addNode(labelset);
            }

            builder.addEdge(_typeA, 0, 1);
            builder.addEdge(_typeA, 1, 2);
            builder.addEdge(_typeA, 2, 0);
            builder.addEdge(_typeB, 1, 0);
            builder.addEdge(_typeA, 2, 2);
            builder.addEdge(_typeB, 3, 4);
            builder.addEdge(_typeA, 4, 5);

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            const NodeID six = builder.addNode(labelset);
            ASSERT_EQ(six.getValue(), 6u);

            builder.addEdge(_typeA, 5, six);
            builder.addEdge(_typeB, six, 0);
            builder.addEdge(_typeA, 3, 0);

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void allNodes(ColumnNodeIDs& input) const {
        input.clear();
        for (size_t node = 0; node < nodeCount; node++) {
            input.push_back(NodeID(node));
        }
    }

    // Deletes one edge in a further commit, so the view carries an edge tombstone
    void deleteEdge(uint64_t edge) {
        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        commitBuilder->writeBuffer().addDeletedEdge(EdgeID(edge));

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    EdgeTypeID _typeA;
    EdgeTypeID _typeB;
};

TEST_F(PathExploratorTest, matchesTheReferenceEnumerationInEveryConfiguration) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    const std::vector<std::pair<uint64_t, uint64_t>> bounds {
        {1, 1}, {0, 2}, {2, 3}, {1, unbounded}, {0, unbounded},
    };

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : bounds) {
            ReferenceEnumerator reference(_adjacency, direction, minHops, maxHops);
            std::vector<PathRow> expected;
            reference.enumerate(input, expected);
            ASSERT_FALSE(expected.empty());

            for (const size_t maxCount : {size_t {1}, size_t {2}, ChunkConfig::CHUNK_SIZE}) {
                for (const size_t lookahead : {size_t {0}, size_t {1}}) {
                    ExplorationOptions options;
                    options._maxCount = maxCount;
                    options._lookahead = lookahead;

                    std::vector<PathRow> actual;
                    collectPaths(view, input, direction, minHops, maxHops, options, actual);
                    expectSameRows(expected, actual);
                }
            }
        }
    }
}

TEST_F(PathExploratorTest, typeFilterMatchesTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    for (const EdgeTypeID edgeType : {_typeA, _typeB}) {
        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
            ReferenceEnumerator reference(_adjacency, direction, 0, unbounded);
            reference.setEdgeType(edgeType.getValue());
            std::vector<PathRow> expected;
            reference.enumerate(input, expected);

            ExplorationOptions options;
            options._edgeType = edgeType;
            options._maxCount = 2;

            std::vector<PathRow> actual;
            collectPaths(view, input, direction, 0, unbounded, options, actual);
            expectSameRows(expected, actual);

            // The zero-length rows do not depend on the type: one per seed
            size_t zeroLength = 0;
            for (const PathRow& row : actual) {
                zeroLength += row._edges.empty() ? 1 : 0;
            }
            EXPECT_EQ(zeroLength, input.size());
        }
    }
}

TEST_F(PathExploratorTest, maxHopsZeroNeverExpands) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input {0, 3, 0};

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 0, 0, ExplorationOptions {}, rows);
    expectSameRows({{0, 0, {}}, {1, 3, {}}, {2, 0, {}}}, rows);

    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 0, ExplorationOptions {}, rows);
    EXPECT_TRUE(rows.empty());
}

TEST_F(PathExploratorTest, nullTargetsAndPathsAreSkipped) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    ReferenceEnumerator reference(_adjacency, PathExplorationDir::FORWARD, 1, 3);
    std::vector<PathRow> expected;
    reference.enumerate(input, expected);

    ExplorationOptions options;
    options._collectTargets = false;
    options._collectPaths = false;

    std::vector<PathRow> actual;
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, actual);
    EXPECT_EQ(actual.size(), expected.size());

    std::vector<size_t> expectedIndices;
    for (const PathRow& row : expected) {
        expectedIndices.push_back(row._index);
    }
    std::vector<size_t> actualIndices;
    for (const PathRow& row : actual) {
        actualIndices.push_back(row._index);
    }
    std::sort(expectedIndices.begin(), expectedIndices.end());
    std::sort(actualIndices.begin(), actualIndices.end());
    EXPECT_EQ(expectedIndices, actualIndices);
}

TEST_F(PathExploratorTest, hopFilterMatchesTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    ReferenceEnumerator reference(_adjacency, PathExplorationDir::BOTH, 0, unbounded);
    reference.setHopPredicate(&evenEdgesOnly);
    std::vector<PathRow> expected;
    reference.enumerate(input, expected);

    PredicateHopFilter filter(&evenEdgesOnly);
    ExplorationOptions options;
    options._hopFilter = &filter;
    options._maxCount = 1;

    std::vector<PathRow> actual;
    collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, options, actual);
    expectSameRows(expected, actual);
}

TEST_F(PathExploratorTest, hopFilterRejectingEveryFrameLeavesTheZeroLengthRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    PredicateHopFilter filter(&nothingPasses);
    ExplorationOptions options;
    options._hopFilter = &filter;

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, options, rows);
    EXPECT_EQ(rows.size(), input.size());
    for (const PathRow& row : rows) {
        EXPECT_TRUE(row._edges.empty());
        EXPECT_EQ(row._target, input[row._index].getValue());
    }

    collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, rows);
    EXPECT_TRUE(rows.empty());
}

TEST_F(PathExploratorTest, walksPatchEdgesOfEarlierNodes) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // 3->4 is a first-commit edge, 3->0 a second-commit patch edge of node 3
    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {3}, PathExplorationDir::FORWARD, 1, 1, ExplorationOptions {}, rows);
    expectSameRows({{0, 4, {edgeBetween(_adjacency, 3, 4)}}, {0, 0, {edgeBetween(_adjacency, 3, 0)}}}, rows);

    // 5->6 is a patch out-edge of node 5 towards the second commit's node
    collectPaths(view, ColumnNodeIDs {5}, PathExplorationDir::FORWARD, 1, 1, ExplorationOptions {}, rows);
    expectSameRows({{0, 6, {edgeBetween(_adjacency, 5, 6)}}}, rows);

    // Node 0's in-edges: two first-commit ones and two patched in by the second commit
    collectPaths(view, ColumnNodeIDs {0}, PathExplorationDir::BACKWARD, 1, 1, ExplorationOptions {}, rows);
    expectSameRows({{0, 2, {edgeBetween(_adjacency, 2, 0)}},
                    {0, 1, {edgeBetween(_adjacency, 1, 0)}},
                    {0, 6, {edgeBetween(_adjacency, 6, 0)}},
                    {0, 3, {edgeBetween(_adjacency, 3, 0)}}},
                   rows);
}

TEST_F(PathExploratorTest, twoCycleYieldsEachDirectionButNeverAnEdgeTwice) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const uint64_t zeroToOne = edgeBetween(_adjacency, 0, 1);
    const uint64_t oneToZero = edgeBetween(_adjacency, 1, 0);

    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {0}, PathExplorationDir::FORWARD, 1, unbounded, ExplorationOptions {}, rows);

    const PathRow there {0, 1, {zeroToOne}};
    const PathRow andBack {0, 0, {zeroToOne, oneToZero}};
    const PathRow again {0, 1, {zeroToOne, oneToZero, zeroToOne}};
    EXPECT_NE(std::find(rows.begin(), rows.end(), there), rows.end());
    EXPECT_NE(std::find(rows.begin(), rows.end(), andBack), rows.end());
    EXPECT_EQ(std::find(rows.begin(), rows.end(), again), rows.end());
}

TEST_F(PathExploratorTest, selfLoopIsEmittedTwiceInBothDirections) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const uint64_t loop = edgeBetween(_adjacency, 2, 2);

    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {2}, PathExplorationDir::BOTH, 1, 1, ExplorationOptions {}, rows);
    EXPECT_EQ(countRowsThrough(rows, loop), 2u);

    collectPaths(view, ColumnNodeIDs {2}, PathExplorationDir::FORWARD, 1, 1, ExplorationOptions {}, rows);
    EXPECT_EQ(countRowsThrough(rows, loop), 1u);
}

TEST_F(PathExploratorTest, tombstonedEdgesAreNeitherEmittedNorExpanded) {
    const uint64_t deleted = edgeBetween(_adjacency, 4, 5);
    deleteEdge(deleted);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    ASSERT_TRUE(view.tombstones().hasEdges());

    Adjacency deletedAdjacency;
    buildAdjacency(view, nodeCount, deletedAdjacency);

    ColumnNodeIDs input;
    allNodes(input);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        ReferenceEnumerator reference(deletedAdjacency, direction, 0, unbounded);
        std::vector<PathRow> expected;
        reference.enumerate(input, expected);

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, 0, unbounded, ExplorationOptions {}, actual);
        expectSameRows(expected, actual);
        EXPECT_EQ(countRowsThrough(actual, deleted), 0u);
    }

    // Node 4's only out-edge is the deleted one, so nothing leaves it
    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {4}, PathExplorationDir::FORWARD, 1, unbounded, ExplorationOptions {}, rows);
    EXPECT_TRUE(rows.empty());

    // And 3->4->5->6 no longer reaches past 4
    collectPaths(view, ColumnNodeIDs {3}, PathExplorationDir::FORWARD, 2, unbounded, ExplorationOptions {}, rows);
    for (const PathRow& row : rows) {
        EXPECT_NE(row._target, 5u);
        EXPECT_NE(row._target, 6u);
    }
}

TEST_F(PathExploratorTest, emptyInputYieldsNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input;
    ColumnVector<size_t> indices;

    PathExplorator explorator(view, &input, PathExplorationDir::BOTH, 0, unbounded);
    explorator.setIndices(&indices);
    EXPECT_FALSE(explorator.isValid());

    explorator.fill(ChunkConfig::CHUNK_SIZE);
    EXPECT_TRUE(indices.empty());
    EXPECT_FALSE(explorator.isValid());
}

TEST_F(PathExploratorTest, resetRestartsFromTheFirstSeed) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input {0, 1};
    ColumnVector<size_t> indices;

    PathExplorator explorator(view, &input, PathExplorationDir::FORWARD, 1, 2);
    explorator.setIndices(&indices);

    size_t firstRun = 0;
    while (explorator.isValid()) {
        explorator.fill(1);
        firstRun += indices.size();
    }

    explorator.reset();
    EXPECT_TRUE(explorator.isValid());

    size_t secondRun = 0;
    while (explorator.isValid()) {
        explorator.fill(ChunkConfig::CHUNK_SIZE);
        secondRun += indices.size();
    }

    EXPECT_GT(firstRun, 0u);
    EXPECT_EQ(firstRun, secondRun);
}

// A chain longer than the 64 signature bits: every edge of the single trail from its head
// is on the path together, so some pairs share a bit and only the exact scan tells them apart.
TEST(PathExploratorChainTest, signatureCollisionsAreResolvedByTheExactScan) {
    constexpr size_t chainNodes = 70;

    JobSystem jobSystem;
    jobSystem.init();
    auto graph = Graph::create();

    {
        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const EdgeTypeID link = metadata.getOrCreateEdgeType("A");

        for (size_t node = 0; node < chainNodes; node++) {
            builder.addNode(labelset);
        }
        for (size_t node = 0; node + 1 < chainNodes; node++) {
            builder.addEdge(link, node, node + 1);
        }

        const auto submitted = change->access().submit(jobSystem);
        ASSERT_TRUE(submitted);
    }

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // The final node IDs are not the insertion order, so the head is the node nothing enters
    Adjacency adjacency;
    buildAdjacency(view, chainNodes, adjacency);

    ColumnNodeIDs head;
    for (size_t node = 0; node < chainNodes; node++) {
        if (adjacency._ins[node].empty()) {
            head.push_back(NodeID(node));
        }
    }
    ASSERT_EQ(head.size(), 1u);

    {
        ExplorationOptions options;

        std::vector<PathRow> rows;
        collectPaths(view, head, PathExplorationDir::FORWARD, 1, unbounded, options, rows);
        ASSERT_EQ(rows.size(), chainNodes - 1);

        std::sort(rows.begin(), rows.end(), [](const PathRow& lhs, const PathRow& rhs) {
            return lhs._edges.size() < rhs._edges.size();
        });
        for (size_t depth = 1; depth <= rows.size(); depth++) {
            EXPECT_EQ(rows[depth - 1]._edges.size(), depth);
        }
    }

    jobSystem.terminate();
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
