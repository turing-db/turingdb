#include <memory>
#include <random>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
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

struct GeneratedArc {
    size_t _source {0};
    size_t _target {0};
};

}

class PathIndexGateTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Every node carries N, the ones listed carry T as well; the arcs name nodes by the IDs
    // the first commit gives them
    void build(size_t nodeCount, const std::vector<size_t>& endNodes, const std::vector<GeneratedArc>& arcs) {
        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _labelT = metadata.getOrCreateLabel("T");
            _type = metadata.getOrCreateEdgeType("A");

            for (size_t node = 0; node < nodeCount; node++) {
                builder.addNode(plain);
            }

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();

            for (const GeneratedArc& arc : arcs) {
                builder.addEdge(_type, NodeID(arc._source), NodeID(arc._target));
            }

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        if (endNodes.empty()) {
            return;
        }

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        const LabelSet end = LabelSet::fromList({_labelT});

        _endNodes.clear();
        for (size_t node = 0; node < endNodes.size(); node++) {
            const NodeID endNode = builder.addNode(end);
            _endNodes.push_back(endNode);
            builder.addEdge(_type, NodeID(endNodes[node]), endNode);
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    LabelID _labelT;
    EdgeTypeID _type;
    std::vector<NodeID> _endNodes;
};

// The seed's two children lead to two halves of the next level, one branching by 1 and one by
// 8, which is more than one level of the sample can take in: the fan-out it reads has to be
// the level's, not the half it happened to start with
TEST_F(PathIndexGateTest, samplesALevelAcrossItsWholeFrontier) {
    const size_t halfSize = 2000;
    const size_t denseFanOut = 8;

    const size_t seed = 0;
    const size_t sparseRoot = 1;
    const size_t denseRoot = 2;
    const size_t firstSparse = 3;
    const size_t firstDense = firstSparse + halfSize;
    const size_t firstSink = firstDense + halfSize;
    const size_t nodeCount = firstSink + denseFanOut;

    std::vector<GeneratedArc> arcs {{seed, sparseRoot}, {seed, denseRoot}};
    for (size_t child = 0; child < halfSize; child++) {
        arcs.push_back({sparseRoot, firstSparse + child});
        arcs.push_back({firstSparse + child, firstSink});

        arcs.push_back({denseRoot, firstDense + child});
        for (size_t sink = 0; sink < denseFanOut; sink++) {
            arcs.push_back({firstDense + child, firstSink + sink});
        }
    }
    build(nodeCount, {}, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const std::vector<NodeID> seeds {NodeID(seed)};
    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, expansion);

    ASSERT_GE(expansion._levels, 3u);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[1], 2.0 * halfSize);

    const double thirdLevel = static_cast<double>(halfSize + halfSize * denseFanOut);
    EXPECT_NEAR(expansion._frontierPerSeed[2], thirdLevel, 0.15 * thirdLevel);
}

// A tree walk from one seed inside a graph a hundred times its size, with two T nodes hanging
// off it. The walk costs a small fraction of the graph, and the search from the T nodes a
// small fraction of the walk
TEST_F(PathIndexGateTest, buildsAnIndexWhoseSearchCostsLessThanTheWalk) {
    const size_t treeFanOut = 3;
    const uint64_t treeDepth = 6;
    const size_t backgroundSize = 30000;
    const size_t backgroundDegree = 4;

    std::vector<GeneratedArc> arcs;
    size_t nextNode = 1;
    std::vector<size_t> level {0};
    for (uint64_t depth = 0; depth < treeDepth; depth++) {
        std::vector<size_t> children;
        for (const size_t parent : level) {
            for (size_t child = 0; child < treeFanOut; child++) {
                arcs.push_back({parent, nextNode});
                children.push_back(nextNode);
                nextNode++;
            }
        }
        level = children;
    }

    const size_t treeSize = nextNode;
    std::mt19937_64 generator(7);
    std::uniform_int_distribution<size_t> backgroundNode(treeSize, treeSize + backgroundSize - 1);
    for (size_t node = treeSize; node < treeSize + backgroundSize; node++) {
        for (size_t edge = 0; edge < backgroundDegree; edge++) {
            arcs.push_back({node, backgroundNode(generator)});
        }
    }

    build(treeSize + backgroundSize, {level.front(), level.back()}, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    const PartDirectory parts(view);

    const std::vector<NodeID> seeds {NodeID(0)};
    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, expansion);

    const uint64_t maxHops = treeDepth + 1;
    const double walkChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, 1, maxHops);
    const double graphSize = static_cast<double>(parts.getAllocatedNodeCount() + parts.getAllocatedEdgeCount());
    EXPECT_LT(walkChecks, 0.1 * graphSize);

    const LabelSet endLabels = LabelSet::fromList({_labelT});
    PathDistanceIndex index;
    EXPECT_TRUE(index.buildWithin(view, endLabels, PathExplorationDir::FORWARD, {}, maxHops, walkChecks));
    EXPECT_TRUE(index.isBuilt());
    EXPECT_TRUE(index.canReachEndWithin(NodeID(0), maxHops));
    EXPECT_TRUE(index.isEnd(_endNodes.front()));
}

// Every background node reaches the one T node in a hop, so the search from it covers the
// graph, and a budget the size of a small walk cannot pay for it
TEST_F(PathIndexGateTest, refusesAnIndexWhoseSearchOverrunsTheWalk) {
    const size_t backgroundSize = 20000;

    std::vector<GeneratedArc> arcs;
    for (size_t node = 1; node < backgroundSize; node++) {
        arcs.push_back({node, 0});
    }
    build(backgroundSize, {0}, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const LabelSet endLabels = LabelSet::fromList({_labelT});
    PathDistanceIndex index;
    EXPECT_FALSE(index.buildWithin(view, endLabels, PathExplorationDir::FORWARD, {}, 4, 1000.0));
    EXPECT_FALSE(index.isBuilt());

    EXPECT_TRUE(index.buildWithin(view, endLabels, PathExplorationDir::FORWARD, {}, 4, 1.0e9));
    EXPECT_TRUE(index.isBuilt());
    EXPECT_TRUE(index.canReachEndWithin(NodeID(1), 2));
}
