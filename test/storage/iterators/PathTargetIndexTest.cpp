#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
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
        SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " max " + std::to_string(maxHops) + " type " + std::to_string(edgeType ? edgeType->getValue() : 999));

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

TEST_F(PathTargetIndexTest, choosesTheLayoutByTheBoundTheGraphAndTheTargets) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // One target reaches a handful of nodes at any bound: a table of as many slots
    const std::vector<NodeID> target {NodeID(_hubGraph._target)};

    PathTargetIndex shallow;
    shallow.build(view, target, PathExplorationDir::FORWARD, std::nullopt, 3);
    EXPECT_FALSE(shallow.isDense());

    PathTargetIndex deep;
    deep.build(view, target, PathExplorationDir::FORWARD, std::nullopt, unbounded);
    EXPECT_FALSE(deep.isDense());

    // Both count the nodes they reach the same way
    EXPECT_EQ(shallow.getReachedCount(), 4u);
    EXPECT_EQ(deep.getReachedCount(), 5u);

    // A batch of every node reaches the graph: at a shallow bound a few words per node are
    // cheaper than a probe per node reached, at an unbounded one a word per node per level is not
    std::vector<NodeID> everyNode;
    for (size_t node = 0; node < nodeCount; node++) {
        everyNode.push_back(NodeID(node));
    }

    PathTargetIndex wide;
    wide.build(view, everyNode, PathExplorationDir::FORWARD, std::nullopt, 3);
    EXPECT_TRUE(wide.isDense());

    PathTargetIndex wideAndDeep;
    wideAndDeep.build(view, everyNode, PathExplorationDir::FORWARD, std::nullopt, unbounded);
    EXPECT_FALSE(wideAndDeep.isDense());
}

TEST_F(PathTargetIndexTest, growsTheTablePastItsFirstCapacity) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // The bound is deep enough for one batch to reach every node, more slots than a fresh
    // table holds when the graph is repeated
    std::vector<NodeID> targets;
    for (size_t node = 0; node < nodeCount; node++) {
        targets.push_back(NodeID(node));
    }

    PathTargetIndex index;
    index.build(view, targets, PathExplorationDir::BOTH, std::nullopt, unbounded);
    EXPECT_EQ(index.getBatchCount(), 1u);
    EXPECT_EQ(index.getReachedCount(), nodeCount);

    expectMatchesTheReference(view, targets, PathExplorationDir::BOTH, std::nullopt, unbounded);
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

    // The end, the three nodes of the live branch and the entrance: nothing else gets a slot
    EXPECT_EQ(index.getReachedCount(), 5u);

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

    std::vector<NodeID> seeds;
    for (size_t node = 0; node < nodeCount; node++) {
        seeds.push_back(NodeID(node));
    }

    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(PartDirectory(view), PathExplorationDir::FORWARD, std::nullopt, seeds, expansion);

    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 1, 1, 4));
    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 0, 4));
    EXPECT_TRUE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 1, 4));
    EXPECT_TRUE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 64, 4));

    // Fifty thousand batches of words cost more than a hundred thousand seeds fanning out
    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 3000000, 4));

    // And ten million batches would not fit in memory, whatever the walk costs
    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 640000000, unbounded));
}

TEST_F(PathTargetIndexTest, setModeMatchesTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<NodeID> targets {NodeID(_hubGraph._target), NodeID(_hubGraph._secondTarget), NodeID(_hubGraph._hub)};
    const std::vector<std::optional<EdgeTypeID>> edgeTypes {std::nullopt, _hubGraph._typeA, _hubGraph._typeB};

    std::vector<uint64_t> distances;
    std::vector<uint64_t> nearest;
    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t maxHops : {uint64_t {1}, uint64_t {3}, unbounded}) {
            for (const std::optional<EdgeTypeID>& edgeType : edgeTypes) {
                SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " max " + std::to_string(maxHops) + " type " + std::to_string(edgeType ? edgeType->getValue() : 999));

                std::optional<uint64_t> referenceType;
                if (edgeType) {
                    referenceType = edgeType->getValue();
                }

                nearest.assign(nodeCount, unreached);
                for (const NodeID target : targets) {
                    referenceDistances(_hubGraph._adjacency, target.getValue(), direction, referenceType, distances);
                    for (size_t node = 0; node < nodeCount; node++) {
                        nearest[node] = std::min(nearest[node], distances[node]);
                    }
                }

                PathTargetIndex index;
                index.buildSet(view, targets, direction, edgeType, maxHops);
                ASSERT_TRUE(index.isBuilt());
                EXPECT_TRUE(index.isDense());
                EXPECT_EQ(index.getBatchCount(), 0u);
                EXPECT_FALSE(index.find(NodeID(_hubGraph._target)).isValid());

                for (size_t node = 0; node < nodeCount; node++) {
                    for (const uint64_t hops : {uint64_t {0}, uint64_t {1}, uint64_t {2}, uint64_t {3}, uint64_t {5}, unbounded}) {
                        const uint64_t distance = nearest[node];
                        const bool expected = distance != unreached && distance <= hops && distance <= maxHops;
                        EXPECT_EQ(index.canReachAnyWithin(NodeID(node), hops), expected) << "node " << node << " within " << hops;
                    }
                }
            }
        }
    }
}

TEST_F(PathTargetIndexTest, pricesTheSetAsOneSearch) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    std::vector<NodeID> seeds;
    for (size_t node = 0; node < nodeCount; node++) {
        seeds.push_back(NodeID(node));
    }

    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(PartDirectory(view), PathExplorationDir::FORWARD, std::nullopt, seeds, expansion);

    // Two thousand targets are 32 batches of words over the graph; the set is one search
    // over it, so the fewest seeds that pay for the set are far from paying for the batches
    size_t seedCount = 1;
    while (!PathTargetIndex::isWorthBuildingSet(view, PathExplorationDir::FORWARD, std::nullopt, expansion, seedCount, 2000, 3)) {
        seedCount++;
        ASSERT_LT(seedCount, 100000u);
    }
    EXPECT_FALSE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, seedCount, 2000, 3));
    EXPECT_TRUE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 2000, 3));

    EXPECT_FALSE(PathTargetIndex::isWorthBuildingSet(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 0, 3));
    EXPECT_FALSE(PathTargetIndex::isWorthBuildingSet(view, PathExplorationDir::FORWARD, std::nullopt, expansion, 100000, 2000, 0));
}

// Pseudo-random out-edges over enough nodes for a set's table of reached nodes to weigh less
// than a byte per node of the graph, which is where the set is laid out as that table
class PathTargetIndexGeneratedGraphTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = 4096;
    static constexpr size_t outDegree = 2;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const EdgeTypeID type = metadata.getOrCreateEdgeType("A");

        std::vector<NodeID> nodes;
        for (size_t node = 0; node < nodeCount; node++) {
            nodes.push_back(builder.addNode(plain));
        }

        uint64_t state = 424242;
        for (const NodeID source : nodes) {
            for (size_t edge = 0; edge < outDegree; edge++) {
                state = state * 6364136223846793005ull + 1442695040888963407ull;
                builder.addEdge(type, source, nodes[(state >> 33) % nodeCount]);
            }
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Every node's reach of the set within every budget agrees with the nearest target's
    // reference distance
    void expectSetMatchesTheReference(const GraphView& view, const std::vector<NodeID>& targets, uint64_t maxHops, bool dense) {
        PathTargetIndex index;
        index.buildSet(view, targets, PathExplorationDir::FORWARD, std::nullopt, maxHops);
        ASSERT_TRUE(index.isBuilt());
        EXPECT_EQ(index.isDense(), dense);
        EXPECT_FALSE(index.find(targets.front()).isValid());

        std::vector<uint64_t> distances;
        std::vector<uint64_t> nearest(nodeCount, unreached);
        for (const NodeID target : targets) {
            referenceDistances(_adjacency, target.getValue(), PathExplorationDir::FORWARD, std::nullopt, distances);
            for (size_t node = 0; node < nodeCount; node++) {
                nearest[node] = std::min(nearest[node], distances[node]);
            }
        }

        size_t reached = 0;
        for (size_t node = 0; node < nodeCount; node++) {
            for (const uint64_t hops : {uint64_t {0}, uint64_t {1}, uint64_t {2}, uint64_t {4}, unbounded}) {
                const uint64_t distance = nearest[node];
                const bool expected = distance != unreached && distance <= hops && distance <= maxHops;
                EXPECT_EQ(index.canReachAnyWithin(NodeID(node), hops), expected) << "node " << node << " within " << hops;
            }

            reached += nearest[node] != unreached && nearest[node] <= maxHops ? 1 : 0;
        }
        EXPECT_EQ(index.getReachedCount(), reached);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
};

TEST_F(PathTargetIndexGeneratedGraphTest, laysTheSetOutByItsFootprint) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // One target two hops out reaches a handful of nodes: a table of as many slots
    expectSetMatchesTheReference(view, {NodeID(1234)}, 2, false);

    // A hundred targets reach more than a byte per node weighs
    std::vector<NodeID> hundred;
    for (size_t target = 0; target < 100; target++) {
        hundred.push_back(NodeID(target * 37 % nodeCount));
    }
    expectSetMatchesTheReference(view, hundred, 4, true);

    // And so does one target with no bound on the search
    expectSetMatchesTheReference(view, {NodeID(1234)}, unbounded, true);
}
