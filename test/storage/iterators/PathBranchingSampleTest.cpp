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

// A sample that measured no level of its own, so every level of the bound grows by the one
// ratio: the plain geometric walk the estimate tests here are about
void flatExpansion(double fanOut, PathDistanceIndex::SeedExpansion& expansion) {
    expansion = PathDistanceIndex::SeedExpansion {};
    expansion._tailFanOut = fanOut;
}

}

// Two edge types over one graph, each held by a small share of its nodes, as every relation
// of a real schema is: a cascade whose nodes each continue along three edges, and a path
// whose nodes continue along one. What a walk of either branches by is a property of the
// nodes it reaches, not of the graph it is embedded in.
class PathBranchingSampleTest : public TuringTest {
protected:
    static constexpr size_t cascadeNodeCount = 40;
    static constexpr size_t chainNodeCount = 60;
    static constexpr size_t nodeCount = 2000;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        _cascade = metadata.getOrCreateEdgeType("CASCADE");
        _chain = metadata.getOrCreateEdgeType("CHAIN");

        for (size_t node = 0; node < nodeCount; node++) {
            builder.addNode(plain);
        }

        for (size_t node = 0; node + 3 < cascadeNodeCount; node++) {
            builder.addEdge(_cascade, node, node + 1);
            builder.addEdge(_cascade, node, node + 2);
            builder.addEdge(_cascade, node, node + 3);
        }

        const size_t chainFirst = cascadeNodeCount;
        for (size_t node = chainFirst; node + 1 < chainFirst + chainNodeCount; node++) {
            builder.addEdge(_chain, node, node + 1);
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    EdgeTypeID _cascade {0};
    EdgeTypeID _chain {0};
};

TEST_F(PathBranchingSampleTest, branchesByTheNodesTheWalkReaches) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    PathDistanceIndex::TypeBranching cascade;
    PathDistanceIndex::TypeBranching chain;
    PathDistanceIndex::sampleBranching(parts, PathExplorationDir::FORWARD, _cascade, cascade);
    PathDistanceIndex::sampleBranching(parts, PathExplorationDir::FORWARD, _chain, chain);

    // A node the cascade reaches continues along three edges of its own, and one the chain
    // reaches along a single one. Spread over the two thousand nodes of the graph instead,
    // the same edges would report 0.05 and 0.03 - every walk a chain, or less.
    EXPECT_GT(cascade._fanOut, 2.0);
    EXPECT_NEAR(chain._fanOut, 1.0, 0.05);

    // And only their own nodes can hold a frontier
    EXPECT_LT(cascade._supportNodes, 2.0 * cascadeNodeCount);
    EXPECT_LT(chain._supportNodes, 2.0 * chainNodeCount);
}

// The fan-out a walk meets is the fan-out where it starts: seeded in the cascade it branches
// by three, seeded in the chain by one.
TEST_F(PathBranchingSampleTest, measuresTheBranchingWhereTheSeedsAre) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    Adjacency adjacency;
    buildAdjacency(reader.getView(), nodeCount, adjacency);

    const auto seedsOf = [&adjacency](EdgeTypeID edgeType, std::vector<NodeID>& seeds) {
        seeds.clear();
        for (size_t node = 0; node < nodeCount; node++) {
            for (const ReferenceEdge& edge : adjacency._outs[node]) {
                if (edge._type == edgeType.getValue()) {
                    seeds.push_back(NodeID(node));
                    break;
                }
            }
        }
    };

    std::vector<NodeID> cascadeSeeds;
    std::vector<NodeID> chainSeeds;
    seedsOf(_cascade, cascadeSeeds);
    seedsOf(_chain, chainSeeds);
    ASSERT_FALSE(cascadeSeeds.empty());
    ASSERT_FALSE(chainSeeds.empty());

    std::vector<NodeID> everyNode;
    for (size_t node = 0; node < nodeCount; node++) {
        everyNode.push_back(NodeID(node));
    }

    const auto fanOut = [&parts](const std::vector<NodeID>& seeds) {
        PathDistanceIndex::SeedExpansion expansion;
        PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, std::nullopt, seeds, expansion);

        return expansion._levels == 0 ? 0.0 : expansion._tailFanOut;
    };

    EXPECT_GT(fanOut(cascadeSeeds), 2.0);
    EXPECT_NEAR(fanOut(chainSeeds), 1.0, 0.1);

    // Seeded over the whole graph almost every seed dies at once, and the levels past them
    // reach the cascade the survivors run into. That is what such a walk would meet, not the
    // graph's twentieth of an edge per node.
    EXPECT_GT(fanOut(everyNode), 2.0);
}

TEST_F(PathBranchingSampleTest, chargesACascadeMoreThanAChainPerHop) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const auto checks = [&parts](std::optional<EdgeTypeID> edgeType, uint64_t maxHops) {
        PathDistanceIndex::TypeBranching branching;
        PathDistanceIndex::sampleBranching(parts, PathExplorationDir::FORWARD, edgeType, branching);

        PathDistanceIndex::SeedExpansion expansion;
        flatExpansion(branching._fanOut, expansion);

        return PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, 1, maxHops);
    };

    // A frontier that trebles every hop outgrows one that holds, which is the whole of what
    // the two gates reading this estimate have to tell apart
    EXPECT_GT(checks(_cascade, 4), 3.0 * checks(_chain, 4));
}

TEST_F(PathBranchingSampleTest, theSearchFrontierStopsAtTheNodesCarryingTheType) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const auto checks = [&parts](std::optional<EdgeTypeID> edgeType, uint64_t maxHops) {
        return PathDistanceIndex::estimatedSearchChecks(parts, PathExplorationDir::FORWARD, edgeType, 1, maxHops);
    };

    // The cascade spans forty nodes, so a search covers them within a few hops
    EXPECT_GT(checks(_cascade, 3), checks(_cascade, 2));
    EXPECT_DOUBLE_EQ(checks(_cascade, 100), checks(_cascade, PathDistanceIndex::farthest));

    // A search reaches a node once, so past that there is nothing left for a deeper bound to
    // cost: an index over the cascade costs the cascade, whatever bound the pattern carries
    EXPECT_LT(checks(_cascade, PathDistanceIndex::farthest), static_cast<double>(nodeCount));
}

// The two estimates part company here, and the parting is what lets a gate buy an index. A
// search reaches a node once, so its frontier holds at the nodes the type carries. A trail
// arrives at that node once per path that reaches it, so the enumeration's frontier does not
// hold, and every deeper bound goes on costing more.
TEST_F(PathBranchingSampleTest, theEnumerationFrontierOutgrowsTheNodesCarryingTheType) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    PathDistanceIndex::TypeBranching cascade;
    PathDistanceIndex::sampleBranching(parts, PathExplorationDir::FORWARD, _cascade, cascade);

    const auto searchChecks = [&parts, this](uint64_t maxHops) {
        return PathDistanceIndex::estimatedSearchChecks(parts, PathExplorationDir::FORWARD, _cascade, 1, maxHops);
    };

    const auto walkChecks = [&parts, &cascade](uint64_t maxHops) {
        PathDistanceIndex::SeedExpansion expansion;
        flatExpansion(cascade._fanOut, expansion);

        return PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, 1, maxHops);
    };

    EXPECT_DOUBLE_EQ(searchChecks(24), searchChecks(12));
    EXPECT_GT(walkChecks(24), walkChecks(12));

    // Which is what a walk of a forty-node cascade costs against an index over it
    EXPECT_LT(searchChecks(24), static_cast<double>(nodeCount));
    EXPECT_GT(walkChecks(24), static_cast<double>(nodeCount));
}
