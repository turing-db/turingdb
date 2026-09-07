#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathTargetIndex.h"
#include "metadata/LabelSet.h"
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

constexpr uint64_t unreached = std::numeric_limits<uint64_t>::max();

// A breadth-first search over the adjacency from one target, against the exploration
// direction: the hop count each node needs to reach the target
void referenceDistances(const Adjacency& adjacency,
                        uint64_t target,
                        PathExplorationDir direction,
                        std::optional<uint64_t> edgeType,
                        std::vector<uint64_t>& distances) {
    distances.assign(adjacency._outs.size(), unreached);
    distances[target] = 0;

    std::vector<uint64_t> frontier {target};
    std::vector<uint64_t> next;
    for (uint64_t level = 1; !frontier.empty(); level++) {
        next.clear();
        for (const uint64_t node : frontier) {
            const auto relax = [&](const std::vector<ReferenceEdge>& edges) {
                for (const ReferenceEdge& edge : edges) {
                    const bool wrongType = edgeType && edge._type != *edgeType;
                    if (wrongType || distances[edge._other] != unreached) {
                        continue;
                    }

                    distances[edge._other] = level;
                    next.push_back(edge._other);
                }
            };

            if (direction != PathExplorationDir::BACKWARD) {
                relax(adjacency._ins[node]);
            }
            if (direction != PathExplorationDir::FORWARD) {
                relax(adjacency._outs[node]);
            }
        }
        std::swap(frontier, next);
    }
}

}

class PathTargetIndexTest : public TuringTest {
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

    // Every node's reachability of every indexed target within every budget agrees with the
    // reference distances
    void expectMatchesTheReference(const GraphView& view,
                                   const std::vector<NodeID>& targets,
                                   PathExplorationDir direction,
                                   std::optional<EdgeTypeID> edgeType,
                                   uint64_t maxHops) {
        PathTargetIndex index;
        index.build(view, targets, direction, edgeType, maxHops);
        ASSERT_TRUE(index.isBuilt());

        std::optional<uint64_t> referenceType;
        if (edgeType) {
            referenceType = edgeType->getValue();
        }

        std::vector<uint64_t> distances;
        for (const NodeID target : targets) {
            referenceDistances(_hubGraph._adjacency, target.getValue(), direction, referenceType, distances);

            const PathTargetHandle handle = index.find(target);
            ASSERT_TRUE(handle.isValid());

            for (size_t node = 0; node < nodeCount; node++) {
                for (const uint64_t hops : {uint64_t {0}, uint64_t {1}, uint64_t {2}, uint64_t {3}, uint64_t {5}, unbounded}) {
                    const uint64_t distance = distances[node];
                    const bool expected = distance != unreached && distance <= hops && distance <= maxHops;
                    EXPECT_EQ(handle.canReachWithin(NodeID(node), hops), expected)
                        << "node " << node << " target " << target.getValue() << " within " << hops;
                }
            }
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
};

TEST_F(PathTargetIndexTest, matchesTheReferenceInEveryConfiguration) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    std::vector<NodeID> targets;
    for (size_t node = 0; node < nodeCount; node++) {
        targets.push_back(NodeID(node));
    }

    const std::vector<std::optional<EdgeTypeID>> edgeTypes {std::nullopt, _hubGraph._typeA, _hubGraph._typeB};

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t maxHops : {uint64_t {1}, uint64_t {3}, unbounded}) {
            for (const std::optional<EdgeTypeID>& edgeType : edgeTypes) {
                expectMatchesTheReference(view, targets, direction, edgeType, maxHops);
            }
        }
    }
}

TEST_F(PathTargetIndexTest, batchesSixtyFourTargetsPerWord) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // Every node three times over: three batches, the same target in each of them
    std::vector<NodeID> targets;
    for (size_t repeat = 0; repeat < 3; repeat++) {
        for (size_t node = 0; node < nodeCount; node++) {
            targets.push_back(NodeID(node));
        }
    }
    ASSERT_GT(targets.size(), PathTargetIndex::targetsPerBatch);

    PathTargetIndex index;
    index.build(view, targets, PathExplorationDir::FORWARD, std::nullopt, 3);
    EXPECT_EQ(index.getBatchCount(), 2u);

    expectMatchesTheReference(view, targets, PathExplorationDir::FORWARD, std::nullopt, 3);
}

TEST_F(PathTargetIndexTest, unindexedTargetsPruneNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<NodeID> targets {NodeID(_hubGraph._target)};

    PathTargetIndex index;
    EXPECT_FALSE(index.isBuilt());
    index.build(view, targets, PathExplorationDir::FORWARD, std::nullopt, unbounded);

    const PathTargetHandle other = index.find(NodeID(_hubGraph._hub));
    EXPECT_FALSE(other.isValid());
    EXPECT_TRUE(other.canReachWithin(NodeID(_hubGraph._hub), 0));
    EXPECT_TRUE(other.canReachWithin(NodeID(1000), 0));

    // The hub reaches the end in three hops and the leaves never do
    const PathTargetHandle handle = index.find(NodeID(_hubGraph._target));
    ASSERT_TRUE(handle.isValid());
    EXPECT_FALSE(handle.canReachWithin(NodeID(_hubGraph._hub), 2));
    EXPECT_TRUE(handle.canReachWithin(NodeID(_hubGraph._hub), 3));
    EXPECT_TRUE(handle.canReachWithin(NodeID(_hubGraph._target), 0));
    EXPECT_FALSE(handle.canReachWithin(NodeID(1000), unbounded));

    size_t unreachable = 0;
    for (size_t node = 0; node < nodeCount; node++) {
        unreachable += handle.canReachWithin(NodeID(node), unbounded) ? 0 : 1;
    }
    EXPECT_EQ(unreachable, 18u);
}

TEST_F(PathTargetIndexTest, costGateChargesEveryBatch) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, 1, 1, 4));
    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, 100000, 0, 4));
    EXPECT_TRUE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, 100000, 1, 4));
    EXPECT_TRUE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, 100000, 64, 4));

    // A thousand batches cost more than a hundred thousand seeds fanning out
    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, 100000, 64000, 4));
}
