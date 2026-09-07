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
#include "metadata/LabelSetHandle.h"
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

bool nothingPasses(uint64_t, uint64_t, uint64_t) {
    return false;
}

}

// A hub with one live branch, hub->c1->c2->t ending on the T node t, and one dead one:
// hub->dead fans out to four nodes of three leaves each, none of which reaches a T node.
// t->hub is a type B edge closing a cycle. The second commit adds a node entering the hub
// and a second T node reached from c2 through a patch edge. The first commit's N nodes may
// be renumbered, so the hub and c2 are read off the graph before the second commit.
class PathExploratorEndLabelsTest : public TuringTest {
protected:
    static constexpr size_t firstCommitNodeCount = 21;
    static constexpr size_t nodeCount = 23;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _labelT = metadata.getOrCreateLabel("T");
            const LabelSet end = LabelSet::fromList({_labelT});
            _typeA = metadata.getOrCreateEdgeType("A");
            _typeB = metadata.getOrCreateEdgeType("B");

            const NodeID hub = builder.addNode(plain);
            const NodeID chainOne = builder.addNode(plain);
            const NodeID chainTwo = builder.addNode(plain);
            const NodeID dead = builder.addNode(plain);

            std::vector<NodeID> cluster;
            for (size_t member = 0; member < 4; member++) {
                cluster.push_back(builder.addNode(plain));
            }

            std::vector<NodeID> leaves;
            for (size_t leaf = 0; leaf < 12; leaf++) {
                leaves.push_back(builder.addNode(plain));
            }

            const NodeID target = builder.addNode(end);

            builder.addEdge(_typeA, hub, chainOne);
            builder.addEdge(_typeA, chainOne, chainTwo);
            builder.addEdge(_typeA, chainTwo, target);
            builder.addEdge(_typeB, target, hub);
            builder.addEdge(_typeA, hub, dead);

            for (size_t member = 0; member < cluster.size(); member++) {
                builder.addEdge(_typeA, dead, cluster[member]);
                for (size_t leaf = 0; leaf < 3; leaf++) {
                    builder.addEdge(_typeA, cluster[member], leaves[member * 3 + leaf]);
                }
            }

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        {
            const FrozenCommitTx transaction = _graph->openTransaction();
            const GraphReader reader = transaction.readGraph();
            ASSERT_EQ(reader.getNodeCount(), firstCommitNodeCount);

            Adjacency firstAdjacency;
            buildAdjacency(reader.getView(), firstCommitNodeCount, firstAdjacency);

            for (size_t node = 0; node < firstCommitNodeCount; node++) {
                if (reader.getNodeLabelSet(NodeID(node)).hasLabel(_labelT)) {
                    _target = node;
                }
            }

            // The end's one out-edge returns to the hub and its one in-edge comes from c2
            _hub = firstAdjacency._outs[_target].front()._other;
            _chainTwo = firstAdjacency._ins[_target].front()._other;
            _chainOne = firstAdjacency._ins[_chainTwo].front()._other;
        }

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            const LabelSet end = LabelSet::fromList({_labelT});

            const NodeID entrance = builder.addNode(plain);
            const NodeID secondTarget = builder.addNode(end);

            builder.addEdge(_typeA, entrance, NodeID(_hub));
            builder.addEdge(_typeA, NodeID(_chainTwo), secondTarget);

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        _endLabels = LabelSet::fromList({_labelT});

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        ASSERT_EQ(reader.getNodeCount(), nodeCount);
        buildAdjacency(reader.getView(), nodeCount, _adjacency);

        _ends.assign(nodeCount, false);
        for (size_t node = 0; node < nodeCount; node++) {
            _ends[node] = reader.getNodeLabelSet(NodeID(node)).hasLabel(_labelT);
            if (_ends[node] && node != _target) {
                _secondTarget = node;
            }
        }
        ASSERT_EQ(std::count(_ends.begin(), _ends.end(), true), 2);
        ASSERT_TRUE(_ends[_target]);
        ASSERT_TRUE(_ends[_secondTarget]);
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
        ReferenceEnumerator reference(_adjacency, direction, minHops, maxHops);
        reference.setEnds(&_ends);
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
    Adjacency _adjacency;
    std::vector<bool> _ends;
    LabelSet _endLabels;
    LabelID _labelT;
    EdgeTypeID _typeA;
    EdgeTypeID _typeB;
    uint64_t _hub {0};
    uint64_t _chainOne {0};
    uint64_t _chainTwo {0};
    uint64_t _target {0};
    uint64_t _secondTarget {0};
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

    for (const EdgeTypeID edgeType : {_typeA, _typeB}) {
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
        {_target, _target, {}},
        {_secondTarget, _secondTarget, {}},
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

    const LabelSet unused = LabelSet::fromList({LabelID(_labelT.getValue() + 1)});
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

    const uint64_t first = edgeBetween(_adjacency, _hub, _chainOne);
    const uint64_t second = edgeBetween(_adjacency, _chainOne, _chainTwo);
    const uint64_t last = edgeBetween(_adjacency, _chainTwo, _target);
    const uint64_t patch = edgeBetween(_adjacency, _chainTwo, _secondTarget);

    const std::vector<PathRow> expected {
        {0, _target, {first, second, last}},
        {0, _secondTarget, {first, second, patch}},
    };

    const ColumnNodeIDs input {NodeID(_hub)};
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
