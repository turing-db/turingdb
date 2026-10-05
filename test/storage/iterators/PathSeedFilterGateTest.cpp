#include <memory>
#include <random>
#include <span>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathHopFilter.h"
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

constexpr uint64_t hubNode = 9;

bool endNotHub(uint64_t, uint64_t, uint64_t end) {
    return end != hubNode;
}

struct FilteredHop {
    size_t _seedRow {0};
    NodeID _source;
};

class RejectingRowHopFilter : public PathHopFilter {
public:
    explicit RejectingRowHopFilter(size_t rejectedRow)
        : _rejectedRow(rejectedRow)
    {
    }

    ~RejectingRowHopFilter() override {
    }

    size_t filter(std::span<PathHopFrame> frames, std::span<NodeID> candidateNodes, std::span<EdgeID> candidateEdges) override {
        size_t candidate = 0;
        size_t kept = 0;
        for (PathHopFrame& frame : frames) {
            _hops.push_back(FilteredHop {._seedRow = frame._seedRow, ._source = frame._source});

            const size_t frameEnd = candidate + frame._candidateCount;
            if (frame._seedRow == _rejectedRow) {
                frame._candidateCount = 0;
                candidate = frameEnd;
                continue;
            }

            for (; candidate < frameEnd; candidate++) {
                candidateNodes[kept] = candidateNodes[candidate];
                candidateEdges[kept] = candidateEdges[candidate];
                kept++;
            }
        }

        return kept;
    }

    const std::vector<FilteredHop>& getHops() const { return _hops; }

private:
    size_t _rejectedRow {0};
    std::vector<FilteredHop> _hops;
};

}

class PathSeedFilterGateTest : public TuringTest {
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

        for (const size_t endNode : endNodes) {
            const NodeID added = builder.addNode(end);
            builder.addEdge(_type, NodeID(endNode), added);
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    LabelID _labelT;
    EdgeTypeID _type;
};

// The seed leaves by eight dead ends and a hub of twenty thousand children, and the hop
// predicate rejects the hub. The walk checks the seed's nine candidates and stops, so the
// search from the T node, which covers the background, costs far more than it spares.
TEST_F(PathSeedFilterGateTest, pricesAWalkByTheCandidatesItsHopFilterKeeps) {
    const size_t seed = 0;
    const size_t firstLeaf = 1;
    const size_t leafCount = 8;
    const size_t firstBackground = hubNode + 1;
    const size_t backgroundSize = 20000;
    const size_t backgroundDegree = 4;
    const uint64_t maxHops = 5;

    std::vector<GeneratedArc> arcs;
    for (size_t leaf = 0; leaf < leafCount; leaf++) {
        arcs.push_back({seed, firstLeaf + leaf});
    }
    arcs.push_back({seed, hubNode});

    std::mt19937_64 generator(11);
    std::uniform_int_distribution<size_t> backgroundNode(firstBackground, firstBackground + backgroundSize - 1);
    for (size_t node = firstBackground; node < firstBackground + backgroundSize; node++) {
        arcs.push_back({hubNode, node});
        for (size_t edge = 0; edge < backgroundDegree; edge++) {
            arcs.push_back({node, backgroundNode(generator)});
        }
    }

    build(firstBackground + backgroundSize, {firstBackground}, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    const PartDirectory parts(view);

    const std::vector<NodeID> seeds {NodeID(seed)};
    PredicateHopFilter hopFilter(endNotHub);

    PathDistanceIndex::SeedExpansion filtered;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, filtered, &hopFilter);

    PathDistanceIndex::SeedExpansion unfiltered;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, unfiltered);

    ASSERT_GE(filtered._levels, 1u);
    EXPECT_DOUBLE_EQ(filtered._frontierPerSeed[0], static_cast<double>(leafCount + 1));

    const double filteredChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, filtered, 1, maxHops);
    const double unfilteredChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, unfiltered, 1, maxHops);
    EXPECT_DOUBLE_EQ(filteredChecks, static_cast<double>(leafCount + 1));
    EXPECT_GT(unfilteredChecks, 1000.0 * filteredChecks);

    const LabelSet endLabels = LabelSet::fromList({_labelT});
    PathDistanceIndex index;
    EXPECT_FALSE(index.buildWithin(view, endLabels, PathExplorationDir::FORWARD, {}, maxHops, filteredChecks));
    EXPECT_FALSE(index.isBuilt());
}

// Two seeds of five children each, and a hop predicate that rejects every candidate of the
// second seed's row: the first seed's children are filtered under its row, and no child of
// the second is expanded at all
TEST_F(PathSeedFilterGateTest, filtersEachSampledSeedUnderItsOwnRow) {
    const size_t childCount = 5;
    const size_t firstSeed = 0;
    const size_t secondSeed = 1;
    const size_t firstChildrenStart = 2;
    const size_t secondChildrenStart = firstChildrenStart + childCount;
    const size_t sink = secondChildrenStart + childCount;

    std::vector<GeneratedArc> arcs;
    for (size_t child = 0; child < childCount; child++) {
        arcs.push_back({firstSeed, firstChildrenStart + child});
        arcs.push_back({firstChildrenStart + child, sink});
        arcs.push_back({secondSeed, secondChildrenStart + child});
        arcs.push_back({secondChildrenStart + child, sink});
    }

    build(sink + 1, {}, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const std::vector<NodeID> seeds {NodeID(firstSeed), NodeID(secondSeed)};
    RejectingRowHopFilter hopFilter(1);

    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, expansion, &hopFilter);

    ASSERT_GE(expansion._levels, 2u);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[0], static_cast<double>(childCount));
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[1], 0.5 * static_cast<double>(childCount));

    size_t firstRowChildren = 0;
    for (const FilteredHop& hop : hopFilter.getHops()) {
        const size_t source = hop._source.getValue();
        const bool secondRowChild = source >= secondChildrenStart && source < sink;
        EXPECT_FALSE(secondRowChild);

        const bool firstRowChild = source >= firstChildrenStart && source < secondChildrenStart;
        if (firstRowChild) {
            EXPECT_EQ(hop._seedRow, 0u);
            firstRowChildren++;
        }

        if (source == secondSeed) {
            EXPECT_EQ(hop._seedRow, 1u);
        }
    }

    EXPECT_EQ(firstRowChildren, childCount);
}
