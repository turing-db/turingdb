#include <memory>
#include <span>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
#include "columns/ColumnIDs.h"
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

constexpr size_t seedCount = 64;

struct GeneratedArc {
    size_t _source {0};
    size_t _target {0};
};

class CountingHopFilter : public PathHopFilter {
public:
    explicit CountingHopFilter(bool passes)
        : _passes(passes)
    {
    }

    ~CountingHopFilter() override {
    }

    size_t filter(std::span<PathHopFrame> frames, std::span<NodeID> candidateNodes, std::span<EdgeID> candidateEdges) override {
        _framesPerCall.push_back(frames.size());

        if (_passes) {
            return candidateNodes.size();
        }

        for (PathHopFrame& frame : frames) {
            frame._candidateCount = 0;
        }

        return 0;
    }

    const std::vector<size_t>& getFramesPerCall() const { return _framesPerCall; }

private:
    bool _passes {true};
    std::vector<size_t> _framesPerCall;
};

}

class PathHopFilterBatchTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void build(size_t nodeCount, const std::vector<GeneratedArc>& arcs) {
        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _type = metadata.getOrCreateEdgeType("A");

            for (size_t node = 0; node < nodeCount; node++) {
                builder.addNode(plain);
            }

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();

        for (const GeneratedArc& arc : arcs) {
            builder.addEdge(_type, NodeID(arc._source), NodeID(arc._target));
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    // Every seed leaves by childCount edges to nodes of its own, and each of those by
    // grandchildCount more
    void buildTree(size_t childCount, size_t grandchildCount) {
        std::vector<GeneratedArc> arcs;
        size_t nextNode = seedCount;
        for (size_t seed = 0; seed < seedCount; seed++) {
            for (size_t child = 0; child < childCount; child++) {
                const size_t childNode = nextNode++;
                arcs.push_back({seed, childNode});

                for (size_t grandchild = 0; grandchild < grandchildCount; grandchild++) {
                    arcs.push_back({childNode, nextNode++});
                }
            }
        }

        build(nextNode, arcs);
    }

    void sample(CountingHopFilter* hopFilter, PathDistanceIndex::SeedExpansion& expansion) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const PartDirectory parts(reader.getView());

        std::vector<NodeID> seeds;
        for (size_t seed = 0; seed < seedCount; seed++) {
            seeds.push_back(NodeID(seed));
        }

        PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, expansion, hopFilter);
    }

    // The distinct ends of the 64 seeds within one to three hops, which the level search answers
    void searchDistinctEnds(PathExplorationDir direction, CountingHopFilter* hopFilter, std::vector<PathRow>& rows) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        ColumnNodeIDs input;
        for (size_t seed = 0; seed < seedCount; seed++) {
            input.push_back(NodeID(seed));
        }

        ExplorationOptions options;
        options._hopFilter = hopFilter;
        options._distinctEnds = true;
        options._collectPaths = false;

        collectPaths(reader.getView(), input, direction, 1, 3, options, rows);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    EdgeTypeID _type;
};

// 64 seeds of 3 children of 3 leaves each: one call filters the 64 seeds' frames and one the
// 192 children's, and the leaves offer no candidate to filter
TEST_F(PathHopFilterBatchTest, filtersEachSampledLevelInOneCall) {
    buildTree(3, 3);

    CountingHopFilter hopFilter(true);
    PathDistanceIndex::SeedExpansion expansion;
    sample(&hopFilter, expansion);

    EXPECT_EQ(hopFilter.getFramesPerCall(), (std::vector<size_t> {seedCount, 3 * seedCount}));
    ASSERT_GE(expansion._levels, 2u);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[0], 3.0);
    EXPECT_DOUBLE_EQ(expansion._frontierPerSeed[1], 9.0);
}

// 64 seeds of 100 leaves each against a budget of 4096 nodes a level. Passing every candidate,
// the 41st seed takes the level past the budget, so the sample stops there, after one call.
// Rejecting every candidate, the budget is never reached and all 64 seeds are filtered.
TEST_F(PathHopFilterBatchTest, stopsABatchedLevelWhereTheBudgetStops) {
    buildTree(100, 0);

    CountingHopFilter passing(true);
    PathDistanceIndex::SeedExpansion passed;
    sample(&passing, passed);

    EXPECT_EQ(passing.getFramesPerCall(), (std::vector<size_t> {41}));

    CountingHopFilter rejecting(false);
    PathDistanceIndex::SeedExpansion rejected;
    sample(&rejecting, rejected);

    const std::vector<size_t>& framesPerCall = rejecting.getFramesPerCall();
    size_t filteredFrames = 0;
    for (const size_t frames : framesPerCall) {
        filteredFrames += frames;
    }

    EXPECT_EQ(filteredFrames, seedCount);
    EXPECT_LE(framesPerCall.size(), 2u);
    EXPECT_DOUBLE_EQ(rejected._tailPassRate, 0.0);
}

// The 64 seeds of 3 children of 3 leaves each make one batch of the level search, which
// filters the frames of each level in one call: the 64 seeds', then the 192 children's. The
// leaves have no edge out.
TEST_F(PathHopFilterBatchTest, filtersEachLevelOfTheDistinctSearchInOneCall) {
    buildTree(3, 3);

    CountingHopFilter hopFilter(true);
    std::vector<PathRow> rows;
    searchDistinctEnds(PathExplorationDir::FORWARD, &hopFilter, rows);

    EXPECT_EQ(hopFilter.getFramesPerCall(), (std::vector<size_t> {seedCount, 3 * seedCount}));
    EXPECT_EQ(rows.size(), 12 * seedCount);
}

// Undirected, the search for cycles through the seeds runs first and filters by levels too:
// the seeds, the 192 children and the 576 leaves, each in one call, and then the level search
// does the same. No cycle closes in a tree, so a seed is no end of its own.
TEST_F(PathHopFilterBatchTest, filtersEachLevelOfTheUndirectedCycleSearchInOneCall) {
    buildTree(3, 3);

    CountingHopFilter hopFilter(true);
    std::vector<PathRow> rows;
    searchDistinctEnds(PathExplorationDir::BOTH, &hopFilter, rows);

    const std::vector<size_t> levelFrames {seedCount, 3 * seedCount, 9 * seedCount};
    std::vector<size_t> expectedFrames = levelFrames;
    expectedFrames.insert(expectedFrames.end(), levelFrames.begin(), levelFrames.end());

    EXPECT_EQ(hopFilter.getFramesPerCall(), expectedFrames);
    EXPECT_EQ(rows.size(), 12 * seedCount);
}
