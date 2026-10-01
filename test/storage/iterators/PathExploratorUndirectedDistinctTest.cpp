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

void distinctPairs(const std::vector<PathRow>& rows, std::vector<PathRow>& pairs) {
    pairs.clear();
    for (const PathRow& row : rows) {
        pairs.push_back({row._index, row._target, {}});
    }

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
}

bool everyHopPasses(uint64_t, uint64_t, uint64_t) {
    return true;
}

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

bool towardsHigherNodes(uint64_t source, uint64_t, uint64_t end) {
    return end > source;
}

bool towardsHigherNodesOrOddEdges(uint64_t source, uint64_t edge, uint64_t end) {
    return end >= source || edge % 2 == 1;
}

bool intoEvenNodes(uint64_t, uint64_t, uint64_t end) {
    return end % 2 == 0;
}

}

// MATCH (s)-[*]-(m) WITH DISTINCT m: undirected with a minimum of one hop, every end but the
// seed is a node the walk reaches, and the seed is an end only through a cycle around it
class PathExploratorUndirectedDistinctTest : public TuringTest {
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

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet labels = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const EdgeTypeID type = metadata.getOrCreateEdgeType("A");

        for (size_t node = 0; node < nodeCount; node++) {
            builder.addNode(labels);
        }

        for (const GeneratedArc& arc : arcs) {
            builder.addEdge(type, NodeID(arc._source), NodeID(arc._target));
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);
    }

    // Every node over and over, more rows than one batch of the search holds
    static void repeatedNodes(size_t nodeCount, ColumnNodeIDs& input) {
        const size_t repeats = PathTargetIndex::targetsPerBatch / nodeCount + 2;

        input.clear();
        for (size_t repeat = 0; repeat < repeats; repeat++) {
            for (size_t node = 0; node < nodeCount; node++) {
                input.push_back(NodeID(node));
            }
        }
    }

    static void expectDistinctRows(const GraphView& view,
                                   const Adjacency& adjacency,
                                   const ColumnNodeIDs& input,
                                   uint64_t maxHops,
                                   ExplorationOptions options,
                                   const std::vector<bool>* ends = nullptr,
                                   HopPredicate predicate = nullptr) {
        SCOPED_TRACE("max hops " + std::to_string(maxHops) + " chunk " + std::to_string(options._maxCount));

        ReferenceEnumerator reference(adjacency, PathExplorationDir::BOTH, 1, maxHops);
        if (options._edgeType) {
            reference.setEdgeType(options._edgeType->getValue());
        }
        reference.setEnds(ends);
        reference.setHopPredicate(predicate);

        std::vector<PathRow> rows;
        reference.enumerate(input, rows);

        if (options._endNodes) {
            std::erase_if(rows, [&options](const PathRow& row) {
                return row._target != (*options._endNodes)[row._index].getValue();
            });
        }

        std::vector<PathRow> expected;
        distinctPairs(rows, expected);

        options._distinctEnds = true;
        options._collectPaths = false;

        std::vector<PathRow> actual;
        collectPaths(view, input, PathExplorationDir::BOTH, 1, maxHops, options, actual);
        expectSameRows(expected, actual);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    size_t _nodeCount {0};
};

TEST_F(PathExploratorUndirectedDistinctTest, matchesTheDeduplicatedEnumerationOnRandomMultigraphs) {
    for (uint64_t seed = 1; seed <= 8; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<GeneratedArc> arcs;
        randomArcs(9, seed % 2 == 0 ? 1 : 2, seed, arcs);
        build(9, arcs);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        for (size_t node = 0; node < _nodeCount; node++) {
            input.push_back(NodeID(node));
        }

        for (const uint64_t maxHops : {uint64_t {1}, uint64_t {2}, uint64_t {3}, uint64_t {4}, uint64_t {5}, unbounded}) {
            for (const size_t maxCount : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
                ExplorationOptions options;
                options._maxCount = maxCount;

                expectDistinctRows(view, _adjacency, input, maxHops, options);
            }
        }
    }
}

// A hop predicate can let an edge be crossed one way only, so a cycle back to the seed must be
// one the predicate lets the walk go round
TEST_F(PathExploratorUndirectedDistinctTest, matchesTheDeduplicatedEnumerationUnderHopPredicates) {
    for (uint64_t seed = 1; seed <= 6; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<GeneratedArc> arcs;
        randomArcs(9, seed % 2 == 0 ? 1 : 2, seed, arcs);
        build(9, arcs);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        for (size_t node = 0; node < _nodeCount; node++) {
            input.push_back(NodeID(node));
        }

        for (const HopPredicate predicate : {evenEdgesOnly, towardsHigherNodes, towardsHigherNodesOrOddEdges, intoEvenNodes}) {
            PredicateHopFilter filter(predicate);

            for (const uint64_t maxHops : {uint64_t {1}, uint64_t {2}, uint64_t {3}, uint64_t {4}, unbounded}) {
                ExplorationOptions options;
                options._hopFilter = &filter;

                expectDistinctRows(view, _adjacency, input, maxHops, options, nullptr, predicate);
            }
        }
    }
}

TEST_F(PathExploratorUndirectedDistinctTest, closesOnTheSeedThroughSelfLoopsParallelEdgesAndCyclesOnly) {
    // 0-1-2-0 is a triangle, 2-3 a bridge to a tree 3-4, 3-5, 5 carries a self-loop, and 6=7
    // are joined by two parallel edges
    const std::vector<GeneratedArc> arcs {
        {0, 1}, {1, 2}, {2, 0}, {2, 3}, {3, 4}, {3, 5}, {5, 5}, {6, 7}, {7, 6},
    };
    build(8, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    for (size_t node = 0; node < _nodeCount; node++) {
        input.push_back(NodeID(node));
    }

    for (const uint64_t maxHops : {uint64_t {1}, uint64_t {2}, uint64_t {3}, uint64_t {4}, unbounded}) {
        expectDistinctRows(view, _adjacency, input, maxHops, ExplorationOptions {});
    }

    ExplorationOptions options;
    options._distinctEnds = true;
    options._collectPaths = false;

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, rows);

    std::vector<uint64_t> closed;
    for (const PathRow& row : rows) {
        if (row._target == input[row._index].getValue()) {
            closed.push_back(row._target);
        }
    }
    std::sort(closed.begin(), closed.end());

    EXPECT_EQ(closed, (std::vector<uint64_t> {0, 1, 2, 5, 6, 7}));
}

TEST_F(PathExploratorUndirectedDistinctTest, endConstraintsAndTypeFilterAgreeWithTheEnumeration) {
    _graph = Graph::create();

    HubGraph hubGraph;
    buildHubGraph(*_graph, *_jobSystem, hubGraph);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    repeatedNodes(HubGraph::nodeCount, input);

    const LabelSet endLabels = LabelSet::fromList({hubGraph._labelT});

    ColumnNodeIDs endNodes;
    for (size_t row = 0; row < input.size(); row++) {
        endNodes.push_back(NodeID((row * 5 + 1) % HubGraph::nodeCount));
    }

    for (const uint64_t maxHops : {uint64_t {1}, uint64_t {3}, uint64_t {4}, unbounded}) {
        expectDistinctRows(view, hubGraph._adjacency, input, maxHops, ExplorationOptions {});

        for (const EdgeTypeID edgeType : {hubGraph._typeA, hubGraph._typeB}) {
            ExplorationOptions typed;
            typed._edgeType = edgeType;
            expectDistinctRows(view, hubGraph._adjacency, input, maxHops, typed);
        }

        ExplorationOptions labelled;
        labelled._endLabels = &endLabels;
        expectDistinctRows(view, hubGraph._adjacency, input, maxHops, labelled, &hubGraph._ends);

        ExplorationOptions bound;
        bound._endNodes = &endNodes;
        bound._maxCount = 1;
        expectDistinctRows(view, hubGraph._adjacency, input, maxHops, bound);
    }
}

// The trails of a cyclic graph multiply with its size, its ends do not: the search reads each
// node's edges once to reach them and at most twice more to find a cycle back to the seed
TEST_F(PathExploratorUndirectedDistinctTest, readsEachEdgeAtMostSixTimes) {
    const size_t nodeCount = 10;

    std::vector<GeneratedArc> arcs;
    randomArcs(nodeCount, 2, 7, arcs);
    build(nodeCount, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input {NodeID(0)};

    PredicateHopFilter filter(everyHopPasses);
    for (PathHopFilter* hopFilter : {static_cast<PathHopFilter*>(nullptr), static_cast<PathHopFilter*>(&filter)}) {
        ExplorationOptions options;
        options._hopFilter = hopFilter;
        options._distinctEnds = true;
        options._collectPaths = false;

        std::vector<PathRow> rows;
        const size_t checks = collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, rows);

        EXPECT_GT(rows.size(), nodeCount / 2);
        EXPECT_LE(checks, 6 * arcs.size()) << checks << " checks";
    }
}

// In a tree no seed closes on itself, and finding that out walks the whole tree: the first
// seed that does labels every node of its component, so the others cost a lookup each
TEST_F(PathExploratorUndirectedDistinctTest, walksAComponentWithoutCyclesOnceForAllItsSeeds) {
    const size_t nodeCount = 1023;

    std::vector<GeneratedArc> arcs;
    for (size_t node = 1; node < nodeCount; node++) {
        arcs.push_back(GeneratedArc {._source = (node - 1) / 2, ._target = node});
    }
    build(nodeCount, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    for (size_t node = 0; node < nodeCount; node++) {
        input.push_back(NodeID(node));
    }

    ExplorationOptions options;
    options._distinctEnds = true;
    options._collectPaths = false;

    std::vector<PathRow> searched;
    const size_t searchChecks = collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, options, searched);

    std::vector<PathRow> rows;
    const size_t checks = collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, rows);

    EXPECT_EQ(rows.size(), searched.size() - nodeCount);
    EXPECT_LE(checks, searchChecks + 8 * arcs.size()) << checks << " checks against " << searchChecks;
}

TEST_F(PathExploratorUndirectedDistinctTest, labelsParallelEdgesSelfLoopsAndCyclesOfAComponent) {
    // 0-1 is a bridge, 1=2 two parallel edges, 2-3 a bridge to a self-loop on 3, 3-4 a bridge to
    // the triangle 4-5-6. The pendant 0 comes first, and its labelling decides every other seed.
    const std::vector<GeneratedArc> arcs {
        {0, 1}, {1, 2}, {2, 1}, {2, 3}, {3, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 4},
    };
    build(7, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    for (size_t node = 0; node < _nodeCount; node++) {
        input.push_back(NodeID(node));
    }

    expectDistinctRows(view, _adjacency, input, unbounded, ExplorationOptions {});

    ExplorationOptions options;
    options._distinctEnds = true;
    options._collectPaths = false;

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, rows);

    std::vector<uint64_t> closed;
    for (const PathRow& row : rows) {
        if (row._target == input[row._index].getValue()) {
            closed.push_back(row._target);
        }
    }
    std::sort(closed.begin(), closed.end());

    EXPECT_EQ(closed, (std::vector<uint64_t> {1, 2, 3, 4, 5, 6}));
}

// Every leaf of a star reaches every other through the center, and none lies on a cycle: a
// search per leaf would read the center's edges once per leaf, a search per batch once per batch
TEST_F(PathExploratorUndirectedDistinctTest, sharesABoundedSearchAcrossTheSeedsOfABatch) {
    const size_t leafCount = 2000;

    std::vector<GeneratedArc> arcs;
    for (size_t leaf = 1; leaf <= leafCount; leaf++) {
        arcs.push_back(GeneratedArc {._source = 0, ._target = leaf});
    }
    build(leafCount + 1, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    for (size_t leaf = 1; leaf <= leafCount; leaf++) {
        input.push_back(NodeID(leaf));
    }

    ExplorationOptions options;
    options._distinctEnds = true;
    options._collectPaths = false;

    std::vector<PathRow> searched;
    const size_t searchChecks = collectPaths(view, input, PathExplorationDir::BOTH, 0, 3, options, searched);

    std::vector<PathRow> rows;
    const size_t checks = collectPaths(view, input, PathExplorationDir::BOTH, 1, 3, options, rows);

    EXPECT_EQ(rows.size(), searched.size() - leafCount);
    EXPECT_LE(checks, 3 * searchChecks) << checks << " checks against " << searchChecks;
}

TEST_F(PathExploratorUndirectedDistinctTest, matchesTheDeduplicatedEnumerationOfBoundedWalksFromManySeeds) {
    for (uint64_t seed = 1; seed <= 4; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<GeneratedArc> arcs;
        randomArcs(40, seed % 2 == 0 ? 2 : 3, seed, arcs);
        build(40, arcs);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        repeatedNodes(_nodeCount, input);
        ASSERT_GT(input.size(), PathTargetIndex::targetsPerBatch);

        for (const uint64_t maxHops : {uint64_t {2}, uint64_t {3}, uint64_t {4}}) {
            expectDistinctRows(view, _adjacency, input, maxHops, ExplorationOptions {});

            for (const HopPredicate predicate : {towardsHigherNodesOrOddEdges, intoEvenNodes}) {
                PredicateHopFilter filter(predicate);

                ExplorationOptions options;
                options._hopFilter = &filter;
                expectDistinctRows(view, _adjacency, input, maxHops, options, nullptr, predicate);
            }
        }
    }
}
