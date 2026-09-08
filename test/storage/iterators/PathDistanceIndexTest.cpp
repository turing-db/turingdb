#include <algorithm>
#include <math.h>
#include <memory>
#include <optional>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
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

using Distances = std::vector<uint8_t>;

constexpr uint8_t unreachable = PathDistanceIndex::unreachable;

// A breadth-first search over the adjacency from the end nodes, against the exploration
// direction: the distances the index must reproduce
void referenceDistances(const Adjacency& adjacency,
                        const std::vector<bool>& ends,
                        PathExplorationDir direction,
                        std::optional<uint64_t> edgeType,
                        uint64_t maxHops,
                        Distances& distances) {
    distances.assign(ends.size(), unreachable);

    std::vector<uint64_t> frontier;
    for (size_t node = 0; node < ends.size(); node++) {
        if (ends[node]) {
            distances[node] = 0;
            frontier.push_back(node);
        }
    }

    const auto relax = [&](const std::vector<ReferenceEdge>& edges, uint8_t level, std::vector<uint64_t>& next) {
        for (const ReferenceEdge& edge : edges) {
            const bool wrongType = edgeType && edge._type != *edgeType;
            if (wrongType || distances[edge._other] != unreachable) {
                continue;
            }

            distances[edge._other] = level;
            next.push_back(edge._other);
        }
    };

    std::vector<uint64_t> next;
    for (uint64_t level = 1; level <= maxHops && !frontier.empty(); level++) {
        next.clear();
        for (const uint64_t node : frontier) {
            if (direction != PathExplorationDir::BACKWARD) {
                relax(adjacency._ins[node], static_cast<uint8_t>(level), next);
            }
            if (direction != PathExplorationDir::FORWARD) {
                relax(adjacency._outs[node], static_cast<uint8_t>(level), next);
            }
        }
        std::swap(frontier, next);
    }
}

void sortedDistances(const PathDistanceIndex& index, size_t nodeCount, Distances& distances) {
    distances.clear();
    for (size_t node = 0; node < nodeCount; node++) {
        distances.push_back(index.getDistance(NodeID(node)));
    }
    std::sort(distances.begin(), distances.end());
}

size_t reachedCount(const Distances& distances) {
    return static_cast<size_t>(std::count_if(distances.begin(), distances.end(), [](uint8_t distance) {
        return distance != unreachable;
    }));
}

}

// Two commits: the first has 0->1->2->6 and 0->3->4->5->7 leading to the two T nodes 6 and
// 7, with 3->4 and 5->7 of type B and 7->0 closing a cycle; the second adds node 8 with
// 8->0 and the patch edge 4->6 between two first-commit nodes. Every N node precedes the T
// nodes within a part, so the temporary IDs are the final ones.
class PathDistanceIndexTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = 9;

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

            for (size_t node = 0; node < 6; node++) {
                builder.addNode(plain);
            }
            builder.addNode(end);
            builder.addNode(end);

            builder.addEdge(_typeA, 0, 1);
            builder.addEdge(_typeA, 1, 2);
            builder.addEdge(_typeA, 2, 6);
            builder.addEdge(_typeA, 0, 3);
            builder.addEdge(_typeB, 3, 4);
            builder.addEdge(_typeA, 4, 5);
            builder.addEdge(_typeB, 5, 7);
            builder.addEdge(_typeA, 7, 0);

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            const NodeID eight = builder.addNode(plain);
            ASSERT_EQ(eight.getValue(), 8u);

            builder.addEdge(_typeA, eight, 0);
            builder.addEdge(_typeA, 4, 6);

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        _endLabels = LabelSet::fromList({_labelT});

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);

        _ends.assign(nodeCount, false);
        for (size_t node = 0; node < nodeCount; node++) {
            _ends[node] = reader.getNodeLabelSet(NodeID(node)).hasLabel(_labelT);
        }
        ASSERT_EQ(std::count(_ends.begin(), _ends.end(), true), 2);
        ASSERT_TRUE(_ends[6]);
        ASSERT_TRUE(_ends[7]);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void deleteEdge(uint64_t edge) {
        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        commitBuilder->writeBuffer().addDeletedEdge(EdgeID(edge));

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    void expectMatchesTheReference(const GraphView& view,
                                   const Adjacency& adjacency,
                                   PathExplorationDir direction,
                                   std::optional<EdgeTypeID> edgeType,
                                   uint64_t maxHops) {
        std::optional<uint64_t> referenceType;
        if (edgeType) {
            referenceType = edgeType->getValue();
        }

        Distances expected;
        referenceDistances(adjacency, _ends, direction, referenceType, maxHops, expected);

        PathDistanceIndex index;
        ASSERT_FALSE(index.isBuilt());
        index.build(view, _endLabels, direction, edgeType, maxHops);
        ASSERT_TRUE(index.isBuilt());

        for (size_t node = 0; node < nodeCount; node++) {
            EXPECT_EQ(index.getDistance(NodeID(node)), expected[node])
                << "node " << node << " direction " << static_cast<int>(direction) << " max " << maxHops;
        }
        EXPECT_EQ(index.getReachedCount(), reachedCount(expected));
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    std::vector<bool> _ends;
    LabelSet _endLabels;
    LabelID _labelT;
    EdgeTypeID _typeA;
    EdgeTypeID _typeB;
};

TEST_F(PathDistanceIndexTest, matchesTheReferenceInEveryConfiguration) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<std::optional<EdgeTypeID>> edgeTypes {std::nullopt, _typeA, _typeB};

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t maxHops : {uint64_t {1}, uint64_t {2}, unbounded}) {
            for (const std::optional<EdgeTypeID>& edgeType : edgeTypes) {
                expectMatchesTheReference(view, _adjacency, direction, edgeType, maxHops);
            }
        }
    }
}

TEST_F(PathDistanceIndexTest, distancesFollowTheReverseOfEachDirection) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    PathDistanceIndex index;
    Distances distances;

    // Forward: 2, 4 and 5 are one hop from an end, 1 and 3 two, 0 three, 8 four
    index.build(view, _endLabels, PathExplorationDir::FORWARD, std::nullopt, unbounded);
    sortedDistances(index, nodeCount, distances);
    EXPECT_EQ(distances, (Distances {0, 0, 1, 1, 1, 2, 2, 3, 4}));

    // Backward: 7->0 puts 0 one hop away, 5 is four hops down 0->3->4->5, nothing enters 8
    index.build(view, _endLabels, PathExplorationDir::BACKWARD, std::nullopt, unbounded);
    sortedDistances(index, nodeCount, distances);
    EXPECT_EQ(distances, (Distances {0, 0, 1, 2, 2, 3, 3, 4, unreachable}));

    // Both: every node is within two hops of an end
    index.build(view, _endLabels, PathExplorationDir::BOTH, std::nullopt, unbounded);
    sortedDistances(index, nodeCount, distances);
    EXPECT_EQ(distances, (Distances {0, 0, 1, 1, 1, 1, 2, 2, 2}));
}

TEST_F(PathDistanceIndexTest, boundsAndTypeFilterCutTheReach) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    PathDistanceIndex index;
    Distances distances;

    // Two hops leave 0 and 8 out of reach
    index.build(view, _endLabels, PathExplorationDir::FORWARD, std::nullopt, 2);
    sortedDistances(index, nodeCount, distances);
    EXPECT_EQ(distances, (Distances {0, 0, 1, 1, 1, 2, 2, unreachable, unreachable}));
    EXPECT_EQ(index.getReachedCount(), 7u);

    // Type A alone: 3->4 and 5->7 are gone, so 3 and 5 reach no end
    index.build(view, _endLabels, PathExplorationDir::FORWARD, _typeA, unbounded);
    sortedDistances(index, nodeCount, distances);
    EXPECT_EQ(distances, (Distances {0, 0, 1, 1, 2, 3, 4, unreachable, unreachable}));
    EXPECT_EQ(index.getDistance(NodeID(3)), unreachable);
    EXPECT_EQ(index.getDistance(NodeID(5)), unreachable);
}

TEST_F(PathDistanceIndexTest, tombstonedEdgesAreNotWalked) {
    deleteEdge(edgeBetween(_adjacency, 2, 6));

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    ASSERT_TRUE(view.tombstones().hasEdges());

    Adjacency deletedAdjacency;
    buildAdjacency(view, nodeCount, deletedAdjacency);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        expectMatchesTheReference(view, deletedAdjacency, direction, std::nullopt, unbounded);
        expectMatchesTheReference(view, deletedAdjacency, direction, _typeA, unbounded);
    }

    // With 2->6 gone the type A edges reach an end from 4 alone
    PathDistanceIndex index;
    index.build(view, _endLabels, PathExplorationDir::FORWARD, _typeA, unbounded);
    EXPECT_EQ(index.getReachedCount(), 3u);
    EXPECT_EQ(index.getDistance(NodeID(4)), 1);
    EXPECT_EQ(index.getDistance(NodeID(1)), unreachable);
}

TEST_F(PathDistanceIndexTest, answersReachabilityWithinABudget) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    PathDistanceIndex index;
    index.build(view, _endLabels, PathExplorationDir::FORWARD, std::nullopt, unbounded);

    EXPECT_TRUE(index.isEnd(NodeID(6)));
    EXPECT_TRUE(index.canReachEndWithin(NodeID(6), 0));
    EXPECT_FALSE(index.isEnd(NodeID(0)));
    EXPECT_FALSE(index.canReachEndWithin(NodeID(0), 2));
    EXPECT_TRUE(index.canReachEndWithin(NodeID(0), 3));
    EXPECT_TRUE(index.canReachEndWithin(NodeID(0), unbounded));

    // A node no part owns is nowhere near an end
    EXPECT_EQ(index.getDistance(NodeID(1000)), unreachable);
    EXPECT_FALSE(index.canReachEndWithin(NodeID(1000), unbounded));

    index.build(view, _endLabels, PathExplorationDir::FORWARD, _typeA, unbounded);
    EXPECT_FALSE(index.canReachEndWithin(NodeID(3), unbounded));
}

TEST_F(PathDistanceIndexTest, costGateNeedsEnoughSeeds) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    EXPECT_FALSE(PathDistanceIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, 1, 4));
    EXPECT_TRUE(PathDistanceIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, 100000, 4));
    EXPECT_TRUE(PathDistanceIndex::isWorthBuilding(view, PathExplorationDir::BOTH, std::nullopt, 100000, unbounded));
    EXPECT_FALSE(PathDistanceIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, 100000, 0));
    EXPECT_FALSE(PathDistanceIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, 0, 4));
}

TEST_F(PathDistanceIndexTest, estimatesAnUnboundedWalkFinitely) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const double bounded = PathDistanceIndex::estimatedEnumerationChecks(parts, PathExplorationDir::FORWARD, std::nullopt, 1, 4);
    const double unboundedChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, PathExplorationDir::FORWARD, std::nullopt, 1, unbounded);

    EXPECT_TRUE(std::isfinite(unboundedChecks));
    EXPECT_GT(unboundedChecks, bounded);

    // The estimate charges the levels the index itself would build and no more
    const double capped = PathDistanceIndex::estimatedEnumerationChecks(parts,
                                                                        PathExplorationDir::FORWARD,
                                                                        std::nullopt,
                                                                        1,
                                                                        PathDistanceIndex::farthest);
    EXPECT_DOUBLE_EQ(unboundedChecks, capped);
}

TEST_F(PathDistanceIndexTest, estimatesTheWalkOfTheTypeItFollows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    // Over both directions a walk on every edge reaches more per hop than one restricted to
    // the two B edges, whose frontier never leaves the nodes carrying them
    const double untyped = PathDistanceIndex::estimatedEnumerationChecks(parts, PathExplorationDir::BOTH, std::nullopt, 100, 6);
    const double typedB = PathDistanceIndex::estimatedEnumerationChecks(parts, PathExplorationDir::BOTH, _typeB, 100, 6);

    EXPECT_GT(untyped, typedB);
}

TEST_F(PathDistanceIndexTest, chargesTheFrontierWhileItGrowsAndNotAfter) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const auto checks = [&parts](uint64_t maxHops) {
        return PathDistanceIndex::estimatedEnumerationChecks(parts, PathExplorationDir::BOTH, std::nullopt, 1, maxHops);
    };

    // A deeper bound costs more only while the frontier can still grow
    EXPECT_GT(checks(3), checks(2));

    // Once it covers every node carrying the type there is nothing left to predict, so the
    // estimate must not multiply that frontier by levels the walk may never reach
    EXPECT_DOUBLE_EQ(checks(unbounded), checks(PathDistanceIndex::farthest));
    EXPECT_DOUBLE_EQ(checks(unbounded), checks(PathDistanceIndex::farthest / 2));
}
