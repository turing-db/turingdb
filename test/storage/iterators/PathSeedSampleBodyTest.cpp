#include <math.h>
#include <memory>
#include <vector>

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

// Each seed has one RARE edge to its hub, each hub eight DENSE edges to its leaves, and each
// leaf one RARE edge back to its hub: a body of a RARE step then a DENSE one branches by
// 1, 8, 1, 8, ... while RARE alone dies at the hubs
class PathSeedSampleBodyTest : public TuringTest {
protected:
    static constexpr size_t SEED_COUNT = 16;
    static constexpr size_t LEAVES_PER_HUB = 8;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        _rare = metadata.getOrCreateEdgeType("RARE");
        _dense = metadata.getOrCreateEdgeType("DENSE");

        const size_t nodeCount = 2 * SEED_COUNT + SEED_COUNT * LEAVES_PER_HUB;
        for (size_t node = 0; node < nodeCount; node++) {
            builder.addNode(plain);
        }

        for (size_t seed = 0; seed < SEED_COUNT; seed++) {
            const size_t hub = SEED_COUNT + seed;
            builder.addEdge(_rare, seed, hub);

            for (size_t leafIndex = 0; leafIndex < LEAVES_PER_HUB; leafIndex++) {
                const size_t leaf = 2 * SEED_COUNT + seed * LEAVES_PER_HUB + leafIndex;
                builder.addEdge(_dense, hub, leaf);
                builder.addEdge(_rare, leaf, hub);
            }
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);

        for (size_t seed = 0; seed < SEED_COUNT; seed++) {
            _seeds.push_back(NodeID(seed));
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    EdgeTypeID _rare {0};
    EdgeTypeID _dense {0};
    std::vector<NodeID> _seeds;
};

TEST_F(PathSeedSampleBodyTest, readsEachLevelOffTheStepTakingIt) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const PathDistanceIndex::SampleStep steps[] = {
        {._direction = PathExplorationDir::FORWARD, ._edgeTypes = {&_rare, 1}},
        {._direction = PathExplorationDir::FORWARD, ._edgeTypes = {&_dense, 1}},
    };

    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(parts, steps, _seeds, expansion);

    ASSERT_GE(expansion._levels, 4u);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[0], 1.0);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[1], 8.0);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[2], 8.0);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[3], 64.0);
    EXPECT_NEAR(expansion._tailFanOut, sqrt(8.0), 1e-9);
}

TEST_F(PathSeedSampleBodyTest, readsEveryLevelOffTheOneStepOfABodyOfOne) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {&_rare, 1}, _seeds, expansion);

    ASSERT_GE(expansion._levels, 2u);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[0], 1.0);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[1], 0.0);
}
