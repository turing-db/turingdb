#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

bool nothingPasses(uint64_t, uint64_t, uint64_t) {
    return false;
}

}

class PathExploratorEndLabelsTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = HubGraph::nodeCount;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        buildHubGraph(*_graph, *_jobSystem, _hubGraph);
        _endLabels = LabelSet::fromList({_hubGraph._labelT});
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

    // The rows the unconstrained walk emits that end on a T node: what the end constraint
    // must leave, whether or not the index prunes the walk
    void expectEndRows(const GraphView& view,
                       const ColumnNodeIDs& input,
                       PathExplorationDir direction,
                       uint64_t minHops,
                       uint64_t maxHops,
                       ExplorationOptions options) {
        ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);
        reference.setEnds(&_hubGraph._ends);
        if (options._edgeType) {
            reference.setEdgeType(options._edgeType->getValue());
        }

        std::vector<PathRow> expected;
        reference.enumerate(input, expected);

        options._endLabels = &_endLabels;
        options._distanceIndex = nullptr;

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);

        PathDistanceIndex index;
        index.build(view, _endLabels, direction, options._edgeType, maxHops);
        options._distanceIndex = &index;

        collectPaths(view, input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
    LabelSet _endLabels;
};

TEST_F(PathExploratorEndLabelsTest, matchesTheReferenceWithAndWithoutTheIndex) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    const std::vector<std::pair<uint64_t, uint64_t>> bounds {
        {1, 1}, {1, 3}, {0, 2}, {2, unbounded}, {0, unbounded},
    };

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : bounds) {
            for (const size_t maxCount : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
                for (const size_t walkerCount : {size_t {1}, size_t {8}}) {
                    ExplorationOptions options;
                    options._maxCount = maxCount;
                    options._walkerCount = walkerCount;

                    expectEndRows(view, input, direction, minHops, maxHops, options);
                }
            }
        }
    }
}

TEST_F(PathExploratorEndLabelsTest, typeFilterAgreesWithTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    for (const EdgeTypeID edgeType : {_hubGraph._typeA, _hubGraph._typeB}) {
        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
            ExplorationOptions options;
            options._edgeType = edgeType;

            expectEndRows(view, input, direction, 0, unbounded, options);
        }
    }
}

TEST_F(PathExploratorEndLabelsTest, prunesTheDeadEndRegion) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    ExplorationOptions options;
    options._endLabels = &_endLabels;

    std::vector<PathRow> unpruned;
    const size_t unprunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, unpruned);

    PathDistanceIndex index;
    index.build(view, _endLabels, PathExplorationDir::FORWARD, std::nullopt, 3);
    options._distanceIndex = &index;

    std::vector<PathRow> pruned;
    const size_t prunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, pruned);

    expectSameRows(unpruned, pruned);
    EXPECT_FALSE(pruned.empty());

    // The dead branch holds most of the edges; the index never enters it
    EXPECT_LT(prunedChecks * 2, unprunedChecks) << prunedChecks << " of " << unprunedChecks;
}

TEST_F(PathExploratorEndLabelsTest, hopFilterRejectingEveryFrameLeavesTheZeroLengthEndRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    PredicateHopFilter filter(nothingPasses);
    ExplorationOptions options;
    options._hopFilter = &filter;
    options._endLabels = &_endLabels;

    const std::vector<PathRow> expected {
        {_hubGraph._target, _hubGraph._target, {}},
        {_hubGraph._secondTarget, _hubGraph._secondTarget, {}},
    };

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 0, unbounded, options, rows);
    expectSameRows(expected, rows);

    PathDistanceIndex index;
    index.build(view, _endLabels, PathExplorationDir::FORWARD, std::nullopt, unbounded);
    options._distanceIndex = &index;

    collectPaths(view, input, PathExplorationDir::FORWARD, 0, unbounded, options, rows);
    expectSameRows(expected, rows);

    collectPaths(view, input, PathExplorationDir::FORWARD, 1, unbounded, options, rows);
    EXPECT_TRUE(rows.empty());
}

TEST_F(PathExploratorEndLabelsTest, labelNoNodeCarriesEmitsNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    const LabelSet unused = LabelSet::fromList({LabelID(_hubGraph._labelT.getValue() + 1)});
    ExplorationOptions options;
    options._endLabels = &unused;

    std::vector<PathRow> rows;
    const size_t unprunedChecks = collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, options, rows);
    EXPECT_TRUE(rows.empty());
    EXPECT_GT(unprunedChecks, 0u);

    // No node is an end, so no seed is worth descending into
    PathDistanceIndex index;
    index.build(view, unused, PathExplorationDir::BOTH, std::nullopt, unbounded);
    EXPECT_EQ(index.getReachedCount(), 0u);
    options._distanceIndex = &index;

    const size_t prunedChecks = collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, options, rows);
    EXPECT_TRUE(rows.empty());
    EXPECT_EQ(prunedChecks, 0u);
}

TEST_F(PathExploratorEndLabelsTest, patchEdgeReachesTheSecondCommitEnd) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const Adjacency& adjacency = _hubGraph._adjacency;
    const uint64_t first = edgeBetween(adjacency, _hubGraph._hub, _hubGraph._chainOne);
    const uint64_t second = edgeBetween(adjacency, _hubGraph._chainOne, _hubGraph._chainTwo);
    const uint64_t last = edgeBetween(adjacency, _hubGraph._chainTwo, _hubGraph._target);
    const uint64_t patch = edgeBetween(adjacency, _hubGraph._chainTwo, _hubGraph._secondTarget);

    const std::vector<PathRow> expected {
        {0, _hubGraph._target, {first, second, last}},
        {0, _hubGraph._secondTarget, {first, second, patch}},
    };

    const ColumnNodeIDs input {NodeID(_hubGraph._hub)};
    ExplorationOptions options;
    options._endLabels = &_endLabels;

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, rows);
    expectSameRows(expected, rows);

    PathDistanceIndex index;
    index.build(view, _endLabels, PathExplorationDir::FORWARD, std::nullopt, 3);
    options._distanceIndex = &index;

    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, rows);
    expectSameRows(expected, rows);
    EXPECT_EQ(countRowsThrough(rows, patch), 1u);
}
