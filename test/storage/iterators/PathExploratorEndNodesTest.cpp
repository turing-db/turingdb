#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathTargetIndex.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

// The rows of the unconstrained walk that end on the seed row's own target
void keepRowsEndingOnTheirTarget(std::vector<PathRow>& rows, const ColumnNodeIDs& endNodes) {
    std::erase_if(rows, [&endNodes](const PathRow& row) {
        return row._target != endNodes[row._index].getValue();
    });
}

}

class PathExploratorEndNodesTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = HubGraph::nodeCount;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        buildHubGraph(*_graph, *_jobSystem, _hubGraph);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Every node as a seed three times over, each row aimed at a different end: the node
    // itself, the first T node, and a node spread over the graph
    void seedsWithTargets(ColumnNodeIDs& input, ColumnNodeIDs& endNodes) const {
        input.clear();
        endNodes.clear();
        for (size_t node = 0; node < nodeCount; node++) {
            input.push_back(NodeID(node));
            endNodes.push_back(NodeID(node));

            input.push_back(NodeID(node));
            endNodes.push_back(NodeID(_hubGraph._target));

            input.push_back(NodeID(node));
            endNodes.push_back(NodeID((node * 7 + 3) % nodeCount));
        }
    }

    void distinctTargets(const ColumnNodeIDs& endNodes, std::vector<NodeID>& targets) const {
        targets.assign(endNodes.begin(), endNodes.end());
        std::sort(targets.begin(), targets.end());
        targets.erase(std::unique(targets.begin(), targets.end()), targets.end());
    }

    void expectTargetRows(const GraphView& view,
                          const ColumnNodeIDs& input,
                          const ColumnNodeIDs& endNodes,
                          PathExplorationDir direction,
                          uint64_t minHops,
                          uint64_t maxHops,
                          ExplorationOptions options) {
        ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);
        if (options._edgeType) {
            reference.setEdgeType(options._edgeType->getValue());
        }

        std::vector<PathRow> expected;
        reference.enumerate(input, expected);
        keepRowsEndingOnTheirTarget(expected, endNodes);

        options._endNodes = &endNodes;
        options._targetIndex = nullptr;

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);

        std::vector<NodeID> targets;
        distinctTargets(endNodes, targets);

        PathTargetIndex index;
        index.build(view, targets, direction, options._edgeType, maxHops);
        options._targetIndex = &index;

        collectPaths(view, input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
};

TEST_F(PathExploratorEndNodesTest, matchesTheReferenceWithAndWithoutTheIndex) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    ColumnNodeIDs endNodes;
    seedsWithTargets(input, endNodes);

    const std::vector<std::pair<uint64_t, uint64_t>> bounds {
        {1, 1}, {1, 3}, {0, 2}, {2, unbounded}, {0, unbounded},
    };

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : bounds) {
            for (const size_t maxCount : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
                ExplorationOptions options;
                options._maxCount = maxCount;

                expectTargetRows(view, input, endNodes, direction, minHops, maxHops, options);
            }
        }
    }
}

TEST_F(PathExploratorEndNodesTest, typeFilterAgreesWithTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    ColumnNodeIDs endNodes;
    seedsWithTargets(input, endNodes);

    for (const EdgeTypeID edgeType : {_hubGraph._typeA, _hubGraph._typeB}) {
        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
            ExplorationOptions options;
            options._edgeType = edgeType;

            expectTargetRows(view, input, endNodes, direction, 0, unbounded, options);
        }
    }
}

TEST_F(PathExploratorEndNodesTest, endLabelsAndTargetsBothApply) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    ColumnNodeIDs endNodes;
    seedsWithTargets(input, endNodes);

    // Only the rows aimed at a T node survive both constraints
    ReferenceEnumerator reference(_hubGraph._adjacency, PathExplorationDir::FORWARD, 0, unbounded);
    reference.setEnds(&_hubGraph._ends);

    std::vector<PathRow> expected;
    reference.enumerate(input, expected);
    keepRowsEndingOnTheirTarget(expected, endNodes);

    const LabelSet endLabels = LabelSet::fromList({_hubGraph._labelT});
    ExplorationOptions options;
    options._endLabels = &endLabels;
    options._endNodes = &endNodes;

    std::vector<PathRow> actual;
    collectPaths(view, input, PathExplorationDir::FORWARD, 0, unbounded, options, actual);
    expectSameRows(expected, actual);
    EXPECT_FALSE(actual.empty());
}

TEST_F(PathExploratorEndNodesTest, hopFilterAgreesWithTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    ColumnNodeIDs endNodes;
    seedsWithTargets(input, endNodes);

    ReferenceEnumerator reference(_hubGraph._adjacency, PathExplorationDir::BOTH, 1, unbounded);
    reference.setHopPredicate(evenEdgesOnly);

    std::vector<PathRow> expected;
    reference.enumerate(input, expected);
    keepRowsEndingOnTheirTarget(expected, endNodes);

    PredicateHopFilter filter(evenEdgesOnly);
    ExplorationOptions options;
    options._hopFilter = &filter;
    options._endNodes = &endNodes;
    options._maxCount = 2;

    std::vector<PathRow> actual;
    collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, actual);
    expectSameRows(expected, actual);

    // The index ignores the predicate and stays a lower bound
    std::vector<NodeID> targets;
    distinctTargets(endNodes, targets);

    PathTargetIndex index;
    index.build(view, targets, PathExplorationDir::BOTH, std::nullopt, unbounded);
    options._targetIndex = &index;

    collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, actual);
    expectSameRows(expected, actual);
}

TEST_F(PathExploratorEndNodesTest, indexPrunesSeedsThatCannotReachTheirTarget) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // Every node aimed at the first T node: only the live branch can get there
    ColumnNodeIDs input;
    ColumnNodeIDs endNodes;
    for (size_t node = 0; node < nodeCount; node++) {
        input.push_back(NodeID(node));
        endNodes.push_back(NodeID(_hubGraph._target));
    }

    ExplorationOptions options;
    options._endNodes = &endNodes;

    std::vector<PathRow> unpruned;
    const size_t unprunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, unpruned);

    PathTargetIndex index;
    index.build(view, std::vector<NodeID> {NodeID(_hubGraph._target)}, PathExplorationDir::FORWARD, std::nullopt, 3);
    options._targetIndex = &index;

    std::vector<PathRow> pruned;
    const size_t prunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, pruned);

    expectSameRows(unpruned, pruned);
    EXPECT_FALSE(pruned.empty());
    EXPECT_LT(prunedChecks * 2, unprunedChecks) << prunedChecks << " of " << unprunedChecks;

    // A seed aimed at a node it cannot reach is never descended into
    const ColumnNodeIDs leaf {NodeID(_hubGraph._hub)};
    const ColumnNodeIDs unreachable {NodeID(_hubGraph._secondTarget)};
    options._endNodes = &unreachable;

    PathTargetIndex farIndex;
    farIndex.build(view, std::vector<NodeID> {NodeID(_hubGraph._secondTarget)}, PathExplorationDir::BACKWARD, std::nullopt, unbounded);
    options._targetIndex = &farIndex;

    std::vector<PathRow> rows;
    const size_t checks = collectPaths(view, leaf, PathExplorationDir::BACKWARD, 1, unbounded, options, rows);
    EXPECT_TRUE(rows.empty());
    EXPECT_EQ(checks, 0u);
}

TEST_F(PathExploratorEndNodesTest, setIndexAgreesWithTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // Every node a seed, all bound to the same three ends
    ColumnNodeIDs input;
    for (size_t node = 0; node < nodeCount; node++) {
        input.push_back(NodeID(node));
    }

    std::vector<NodeID> endSet {NodeID(_hubGraph._target), NodeID(_hubGraph._secondTarget), NodeID(_hubGraph._chainOne)};
    std::sort(endSet.begin(), endSet.end());

    const std::vector<std::pair<uint64_t, uint64_t>> bounds {
        {1, 1}, {1, 3}, {0, 2}, {2, unbounded}, {0, unbounded},
    };

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : bounds) {
            SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " hops " + std::to_string(minHops) + " to " + std::to_string(maxHops));

            ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);

            std::vector<PathRow> expected;
            reference.enumerate(input, expected);
            std::erase_if(expected, [&endSet](const PathRow& row) {
                return !std::binary_search(endSet.begin(), endSet.end(), NodeID(row._target));
            });

            ExplorationOptions options;
            options._endNodeSet = endSet;

            std::vector<PathRow> unpruned;
            const size_t unprunedChecks = collectPaths(view, input, direction, minHops, maxHops, options, unpruned);
            expectSameRows(expected, unpruned);

            PathTargetIndex index;
            index.buildSet(view, endSet, direction, std::nullopt, maxHops);
            options._targetIndex = &index;

            std::vector<PathRow> pruned;
            const size_t prunedChecks = collectPaths(view, input, direction, minHops, maxHops, options, pruned);
            expectSameRows(expected, pruned);
            EXPECT_LE(prunedChecks, unprunedChecks);
        }
    }

    // Aimed forward at the two T nodes alone, the dead branch is never descended into
    const std::vector<NodeID> ends {std::min(NodeID(_hubGraph._target), NodeID(_hubGraph._secondTarget)),
                                    std::max(NodeID(_hubGraph._target), NodeID(_hubGraph._secondTarget))};

    ExplorationOptions options;
    options._endNodeSet = ends;

    std::vector<PathRow> unpruned;
    const size_t unprunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, unpruned);

    PathTargetIndex index;
    index.buildSet(view, ends, PathExplorationDir::FORWARD, std::nullopt, 3);
    options._targetIndex = &index;

    std::vector<PathRow> pruned;
    const size_t prunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, pruned);
    expectSameRows(unpruned, pruned);
    EXPECT_FALSE(pruned.empty());
    EXPECT_LT(prunedChecks * 2, unprunedChecks) << prunedChecks << " of " << unprunedChecks;
}
