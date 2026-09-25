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

bool endNotMultipleOfThree(uint64_t, uint64_t, uint64_t end) {
    return end % 3 != 0;
}

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

constexpr uint64_t excludedNode = 6;

bool endNotExcluded(uint64_t, uint64_t, uint64_t end) {
    return end != excludedNode;
}

}

// A hop predicate that reads nothing outside the hop keeps the same candidates wherever the
// walk reaches a node, so the distinct walk remembers subtrees under one as it does without
class PathExploratorHopFilterMemoTest : public TuringTest {
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

    void expectReferenceEnds(HopPredicate predicate, PathExplorationDir direction, uint64_t minHops, uint64_t maxHops) {
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
        reference.setHopPredicate(predicate);

        std::vector<PathRow> rows;
        reference.enumerate(input, rows);

        std::vector<PathRow> expected;
        distinctPairs(rows, expected);

        PredicateHopFilter filter(predicate);

        ExplorationOptions options;
        options._distinctEnds = true;
        options._collectPaths = false;
        options._hopFilter = &filter;

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    EdgeTypeID _type;
    size_t _nodeCount {0};
};

TEST_F(PathExploratorHopFilterMemoTest, deepDistinctEndsUnderAHopFilterAgreeWithTheReference) {
    for (uint64_t seed = 1; seed <= 6; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<GeneratedArc> arcs;
        randomArcs(9, 3, seed, arcs);
        build(9, arcs);

        for (const HopPredicate predicate : {endNotMultipleOfThree, evenEdgesOnly}) {
            for (const PathExplorationDir direction : {PathExplorationDir::FORWARD,
                                                       PathExplorationDir::BACKWARD,
                                                       PathExplorationDir::BOTH}) {
                for (const uint64_t hops : {uint64_t {4}, uint64_t {5}, uint64_t {6}}) {
                    expectReferenceEnds(predicate, direction, hops, hops);
                    expectReferenceEnds(predicate, direction, 2, hops);
                }

                expectReferenceEnds(predicate, direction, 3, unbounded);
            }
        }
    }
}

// The filter shuts one node of a complete graph out, which leaves a complete graph of the
// others: every (node, depth) pair still owes one visit of its edges, the hop filter or not
TEST_F(PathExploratorHopFilterMemoTest, remembersSubtreesUnderAHopFilter) {
    const size_t nodeCount = excludedNode + 1;
    const uint64_t hops = 7;

    std::vector<GeneratedArc> arcs;
    completeArcs(nodeCount, arcs);
    build(nodeCount, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    input.push_back(NodeID {0});

    PredicateHopFilter filter(endNotExcluded);

    ExplorationOptions options;
    options._distinctEnds = true;
    options._collectPaths = false;
    options._hopFilter = &filter;

    std::vector<PathRow> rows;
    const size_t checks = collectPaths(view, input, PathExplorationDir::FORWARD, hops, hops, options, rows);

    const size_t degree = nodeCount - 1;
    const size_t visitBound = nodeCount * hops * degree * degree;

    EXPECT_EQ(rows.size(), nodeCount - 1);
    EXPECT_LE(checks, visitBound) << checks << " checks";
}
