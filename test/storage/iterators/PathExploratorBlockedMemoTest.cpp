#include <algorithm>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathExplorator.h"
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

void randomArcs(size_t nodeCount, size_t outDegree, uint64_t seed, std::vector<GeneratedArc>& arcs) {
    std::mt19937_64 generator(seed);
    std::uniform_int_distribution<size_t> node(0, nodeCount - 1);

    arcs.clear();
    for (size_t source = 0; source < nodeCount; source++) {
        for (size_t edge = 0; edge < outDegree; edge++) {
            arcs.push_back(GeneratedArc {._source = source, ._target = node(generator)});
        }
    }
}

void completeArcs(size_t nodeCount, std::vector<GeneratedArc>& arcs) {
    arcs.clear();
    for (size_t source = 0; source < nodeCount; source++) {
        for (size_t target = 0; target < nodeCount; target++) {
            if (source != target) {
                arcs.push_back(GeneratedArc {._source = source, ._target = target});
            }
        }
    }
}

void distinctPairs(const std::vector<PathRow>& rows, std::vector<PathRow>& pairs) {
    pairs.clear();
    for (const PathRow& row : rows) {
        pairs.push_back({row._index, row._target, {}});
    }

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
}

}

// A subtree of the distinct walk is remembered only when no edge held above it was in its way.
// An edge held above that leads to a node already emitted, and on into a subtree already
// remembered at that depth, keeps nothing from the subtree, so it must not stop it being
// remembered. Deep walks around cycles are where such edges are met.
class PathExploratorBlockedMemoTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void build(size_t nodeCount, std::span<const GeneratedArc> arcs) {
        _graph = Graph::create();
        _nodeCount = nodeCount;

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labels = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _type = metadata.getOrCreateEdgeType("A");

            for (size_t node = 0; node < nodeCount; node++) {
                builder.addNode(labels);
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

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);
    }

    void expectReferenceEnds(PathExplorationDir direction, uint64_t minHops, uint64_t maxHops) {
        SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction))
                     + " hops " + std::to_string(minHops) + " to " + std::to_string(maxHops));

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        for (size_t node = 0; node < _nodeCount; node++) {
            input.push_back(NodeID(node));
        }

        ReferenceEnumerator reference(_adjacency, direction, minHops, maxHops);
        std::vector<PathRow> rows;
        reference.enumerate(input, rows);

        std::vector<PathRow> expected;
        distinctPairs(rows, expected);

        ExplorationOptions options;
        options._distinctEnds = true;
        options._collectPaths = false;

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);
    }

    size_t countDistinctChecks(uint64_t hops, size_t& ends) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        input.push_back(NodeID {0});

        ExplorationOptions options;
        options._distinctEnds = true;
        options._collectPaths = false;

        std::vector<PathRow> rows;
        const size_t checks = collectPaths(view, input, PathExplorationDir::FORWARD, hops, hops, options, rows);
        ends = rows.size();

        return checks;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    EdgeTypeID _type;
    size_t _nodeCount {0};
};

TEST_F(PathExploratorBlockedMemoTest, deepDistinctEndsAgreeWithTheReferenceOnRandomCyclicGraphs) {
    for (uint64_t seed = 1; seed <= 6; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<GeneratedArc> arcs;
        randomArcs(9, 2, seed, arcs);
        build(9, arcs);

        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD,
                                                   PathExplorationDir::BACKWARD,
                                                   PathExplorationDir::BOTH}) {
            for (const uint64_t hops : {uint64_t {5}, uint64_t {6}, uint64_t {7}}) {
                expectReferenceEnds(direction, hops, hops);
                expectReferenceEnds(direction, 2, hops);
            }

            expectReferenceEnds(direction, 3, unbounded);
        }
    }
}

TEST_F(PathExploratorBlockedMemoTest, deepDistinctEndsAgreeWithTheReferenceOnACompleteGraph) {
    std::vector<GeneratedArc> arcs;
    completeArcs(5, arcs);
    build(5, arcs);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t hops : {uint64_t {4}, uint64_t {5}, uint64_t {6}}) {
            expectReferenceEnds(direction, hops, hops);
        }
    }
}

// On a complete graph every node is an end from the second hop on, so past that the only
// work left is proving no new end exists. Each (node, depth) pair owes one visit of its
// edges; a walk re-entering subtrees behind edges held above pays for every trail instead.
TEST_F(PathExploratorBlockedMemoTest, remembersSubtreesBlockedOnlyByEdgesLeadingToCoveredNodes) {
    const size_t nodeCount = 7;
    const uint64_t hops = 7;

    std::vector<GeneratedArc> arcs;
    completeArcs(nodeCount, arcs);
    build(nodeCount, arcs);

    size_t ends = 0;
    const size_t checks = countDistinctChecks(hops, ends);

    const size_t degree = nodeCount - 1;
    const size_t visitBound = nodeCount * hops * degree * degree;

    EXPECT_EQ(ends, nodeCount);
    EXPECT_LE(checks, visitBound) << checks << " checks";
}

// The seed's only edge leads into a fan of m nodes that all enter y, and every branch below y
// returns to the seed, where it would take that first edge again. That edge is on every path
// through the fan, so the walk below y can stand in for every arrival that holds it: the
// subtree is walked once, not once per fan node.
TEST_F(PathExploratorBlockedMemoTest, reusesASubtreeBlockedOnlyByAnEdgeEveryArrivalHolds) {
    const size_t fanCount = 10;
    const size_t branchCount = 10;

    const size_t seed = 0;
    const size_t fanRoot = 1;
    const size_t firstFan = 2;
    const size_t hub = firstFan + fanCount;
    const size_t firstBranch = hub + 1;
    const size_t nodeCount = firstBranch + branchCount;

    std::vector<GeneratedArc> arcs {{seed, fanRoot}, {hub, seed}};
    for (size_t fan = 0; fan < fanCount; fan++) {
        arcs.push_back({fanRoot, firstFan + fan});
        arcs.push_back({firstFan + fan, hub});
    }

    for (size_t branch = 0; branch < branchCount; branch++) {
        arcs.push_back({hub, firstBranch + branch});
        arcs.push_back({firstBranch + branch, seed});
    }

    build(nodeCount, arcs);
    expectReferenceEnds(PathExplorationDir::FORWARD, 6, 6);

    size_t ends = 0;
    const size_t checks = countDistinctChecks(6, ends);

    const size_t hubSubtreeChecks = 3 * branchCount + 2;
    const size_t walkedOnceBound = 1 + 2 * fanCount + hubSubtreeChecks;

    EXPECT_LE(checks, walkedOnceBound) << checks << " checks";
}

TEST_F(PathExploratorBlockedMemoTest, deepDistinctEndsAgreeWithTheReferenceOnDenserCyclicGraphs) {
    for (uint64_t seed = 1; seed <= 4; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<GeneratedArc> arcs;
        randomArcs(14, 3, seed, arcs);
        build(14, arcs);

        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD}) {
            expectReferenceEnds(direction, 8, 8);
            expectReferenceEnds(direction, 3, 8);
        }

        expectReferenceEnds(PathExplorationDir::BOTH, 6, 6);
        expectReferenceEnds(PathExplorationDir::BOTH, 2, 6);
    }
}
