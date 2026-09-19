#include <algorithm>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathExplorator.h"
#include "iterators/PathTargetIndex.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

const PathExplorationDir everyDirection[] = {PathExplorationDir::FORWARD,
                                             PathExplorationDir::BACKWARD,
                                             PathExplorationDir::BOTH};

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

// What a generated adjacency actually holds, so a differential test cannot pass by
// comparing two empty enumerations of a graph without the structure it claims
struct Structure {
    size_t _selfLoops {0};
    size_t _parallelPairs {0};
    size_t _twoCycles {0};
};

void describeStructure(const Adjacency& adjacency, Structure& structure) {
    for (size_t node = 0; node < adjacency._outs.size(); node++) {
        const std::vector<ReferenceEdge>& outs = adjacency._outs[node];

        for (size_t first = 0; first < outs.size(); first++) {
            if (outs[first]._other == node) {
                structure._selfLoops++;
            }

            for (size_t second = first + 1; second < outs.size(); second++) {
                if (outs[second]._other == outs[first]._other) {
                    structure._parallelPairs++;
                }
            }

            for (const ReferenceEdge& back : adjacency._outs[outs[first]._other]) {
                if (back._other == node && back._edge != outs[first]._edge) {
                    structure._twoCycles++;
                }
            }
        }
    }
}

// The (seed row, end) pairs of the enumerated rows, each once and without a path
void distinctPairs(const std::vector<PathRow>& rows, std::vector<PathRow>& pairs) {
    pairs.clear();
    for (const PathRow& row : rows) {
        pairs.push_back({row._index, row._target, {}});
    }

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
}

}

// Graphs the hand-written fixtures cannot cover: pseudo-random adjacency where cycles,
// self-loops and parallel edges between one pair all arise together, and the structural
// extremes of a complete graph and of a cycle longer than the trail signature is wide.
class PathExploratorCyclicTest : public TuringTest {
protected:
    struct Arc {
        size_t _source {0};
        size_t _target {0};
        bool _typeB {false};
    };

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Submits the graph in two commits, the first holding firstCommitNodes nodes and every
    // arc between them: an arc the second commit adds between two first-commit nodes lands
    // as a patch edge, which the walk has to pick up from a part that does not own the node.
    // Every endStride-th node carries the end label T.
    void build(size_t nodeCount, size_t firstCommitNodes, std::span<const Arc> arcs, size_t endStride) {
        _graph = Graph::create();
        _nodeCount = nodeCount;

        addCommit(0, firstCommitNodes, arcs, endStride, true);
        addCommit(firstCommitNodes, nodeCount, arcs, endStride, false);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);

        _ends.assign(nodeCount, false);
        for (size_t node = 0; node < nodeCount; node++) {
            _ends[node] = reader.getNodeLabelSet(NodeID(node)).hasLabel(_labelT);
        }
    }

    // Nodes in the first commit and every arc in the second, over the IDs the first commit
    // actually assigned: a part sorts its nodes by label set, so arcs numbered by insertion
    // order would connect other nodes than intended once the ties outnumber a handful. Every
    // arc is a patch edge this way, which the walk has to reach through the owning part.
    void buildOverAssignedIDs(size_t nodeCount, std::span<const Arc> arcs) {
        _graph = Graph::create();
        _nodeCount = nodeCount;

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labels = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _labelT = metadata.getOrCreateLabel("T");
            _typeA = metadata.getOrCreateEdgeType("A");
            _typeB = metadata.getOrCreateEdgeType("B");
            _endLabels = LabelSet::fromList({_labelT});

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

            for (const Arc& arc : arcs) {
                builder.addEdge(arc._typeB ? _typeB : _typeA, NodeID(arc._source), NodeID(arc._target));
            }

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);

        _ends.assign(nodeCount, false);
    }

    // Out-degree arcs per node drawn uniformly over the nodes, so a small node count makes
    // short cycles, self-loops and repeated pairs certain rather than incidental
    static void randomArcs(size_t nodeCount, size_t outDegree, uint64_t seed, std::vector<Arc>& arcs) {
        std::mt19937_64 generator(seed);
        std::uniform_int_distribution<size_t> node(0, nodeCount - 1);
        std::uniform_int_distribution<int> type(0, 1);

        arcs.clear();
        for (size_t source = 0; source < nodeCount; source++) {
            for (size_t edge = 0; edge < outDegree; edge++) {
                arcs.push_back(Arc {._source = source, ._target = node(generator), ._typeB = type(generator) == 1});
            }
        }
    }

    void allNodes(ColumnNodeIDs& input) const {
        input.clear();
        for (size_t node = 0; node < _nodeCount; node++) {
            input.push_back(NodeID(node));
        }
    }

    void deleteEdge(uint64_t edge) {
        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        commitBuilder->writeBuffer().addDeletedEdge(EdgeID(edge));

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    // Reruns the reference over the current adjacency and requires the explorator to emit
    // the same rows, whatever configuration the options carry
    void expectSameAsReference(PathExplorationDir direction,
                               uint64_t minHops,
                               uint64_t maxHops,
                               ExplorationOptions options,
                               HopPredicate predicate = nullptr) {
        SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction))
                     + " hops " + std::to_string(minHops) + " to " + std::to_string(maxHops)
                     + " chunk " + std::to_string(options._maxCount));

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        allNodes(input);

        ReferenceEnumerator reference(_adjacency, direction, minHops, maxHops);
        if (options._edgeType) {
            reference.setEdgeType(options._edgeType->getValue());
        }
        if (options._endLabels) {
            reference.setEnds(&_ends);
        }
        reference.setHopPredicate(predicate);

        std::vector<PathRow> expected;
        reference.enumerate(input, expected);

        if (options._distinctEnds) {
            std::vector<PathRow> pairs;
            distinctPairs(expected, pairs);
            expected.swap(pairs);
        }

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, minHops, maxHops, options, actual);

        expectSameRows(expected, actual);
        _rowsCompared += expected.size();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    std::vector<bool> _ends;
    LabelSet _endLabels;
    LabelID _labelT;
    EdgeTypeID _typeA;
    EdgeTypeID _typeB;
    size_t _nodeCount {0};
    size_t _rowsCompared {0};

private:
    void addCommit(size_t firstNode,
                   size_t lastNode,
                   std::span<const Arc> arcs,
                   size_t endStride,
                   bool firstCommit) {
        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID plain = metadata.getOrCreateLabel("N");
        _labelT = metadata.getOrCreateLabel("T");
        _typeA = metadata.getOrCreateEdgeType("A");
        _typeB = metadata.getOrCreateEdgeType("B");
        _endLabels = LabelSet::fromList({_labelT});

        for (size_t node = firstNode; node < lastNode; node++) {
            const bool isEnd = endStride != 0 && node % endStride == 0;
            const LabelSet labels = isEnd ? LabelSet::fromList({plain, _labelT}) : LabelSet::fromList({plain});
            builder.addNode(labels);
        }

        for (const Arc& arc : arcs) {
            const bool inFirstCommit = arc._source < lastNode && arc._target < lastNode;
            const bool alreadyAdded = firstCommit ? false : arc._source < firstNode && arc._target < firstNode;
            if (!inFirstCommit || alreadyAdded) {
                continue;
            }

            builder.addEdge(arc._typeB ? _typeB : _typeA, arc._source, arc._target);
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }
};

TEST_F(PathExploratorCyclicTest, matchesTheReferenceOnRandomCyclicGraphs) {
    for (uint64_t seed = 1; seed <= 4; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<Arc> arcs;
        randomArcs(10, 3, seed, arcs);
        build(10, 7, arcs, 4);

        for (const PathExplorationDir direction : everyDirection) {
            for (uint64_t minHops = 0; minHops <= 2; minHops++) {
                for (uint64_t maxHops = minHops; maxHops <= 3; maxHops++) {
                    expectSameAsReference(direction, minHops, maxHops, ExplorationOptions {});
                }
            }
        }

        Structure structure;
        describeStructure(_adjacency, structure);
        EXPECT_GT(structure._selfLoops + structure._parallelPairs + structure._twoCycles, 0u);
    }

    // The sweep weighs about forty thousand rows: a guard against the comparison passing
    // because both sides came out empty
    EXPECT_GT(_rowsCompared, 20000u);
}

TEST_F(PathExploratorCyclicTest, generatesTheStructureTheSweepsRelyOn) {
    Structure total;
    for (uint64_t seed = 1; seed <= 4; seed++) {
        std::vector<Arc> arcs;
        randomArcs(10, 3, seed, arcs);
        build(10, 7, arcs, 4);

        describeStructure(_adjacency, total);
    }

    // Over the seeds the sweeps use, the generator has produced each of the shapes a
    // hand-written cycle fixture would have to spell out one at a time
    EXPECT_GT(total._selfLoops, 0u);
    EXPECT_GT(total._parallelPairs, 0u);
    EXPECT_GT(total._twoCycles, 0u);
}

TEST_F(PathExploratorCyclicTest, terminatesUnboundedOnAFunctionalGraphOfCycles) {
    // One out-edge per node makes every walk a chain running into a cycle, so an unbounded
    // enumeration stays finite and its trails end exactly where an edge would repeat
    for (uint64_t seed = 1; seed <= 3; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<Arc> arcs;
        randomArcs(16, 1, seed, arcs);
        build(16, 9, arcs, 5);

        for (const PathExplorationDir direction : everyDirection) {
            expectSameAsReference(direction, 0, unbounded, ExplorationOptions {});
            expectSameAsReference(direction, 1, unbounded, ExplorationOptions {});
        }
    }
}

TEST_F(PathExploratorCyclicTest, typeFilterAndHopFilterMatchTheReferenceOnACyclicGraph) {
    std::vector<Arc> arcs;
    randomArcs(10, 3, 7, arcs);
    build(10, 6, arcs, 3);

    for (const PathExplorationDir direction : everyDirection) {
        ExplorationOptions typed;
        typed._edgeType = _typeB;
        expectSameAsReference(direction, 0, 3, typed);
        expectSameAsReference(direction, 2, 4, typed);

        PredicateHopFilter filter(evenEdgesOnly);
        ExplorationOptions filtered;
        filtered._hopFilter = &filter;
        expectSameAsReference(direction, 0, 3, filtered, evenEdgesOnly);
        expectSameAsReference(direction, 1, 3, filtered, evenEdgesOnly);
    }
}

TEST_F(PathExploratorCyclicTest, chunkSizeAndLookaheadDoNotChangeTheRowsOfACyclicGraph) {
    std::vector<Arc> arcs;
    randomArcs(12, 2, 11, arcs);
    build(12, 8, arcs, 4);

    for (const size_t chunk : {size_t {1}, size_t {2}, size_t {ChunkConfig::CHUNK_SIZE}}) {
        for (const size_t lookahead : {size_t {0}, size_t {1}}) {
            ExplorationOptions options;
            options._maxCount = chunk;
            options._lookahead = lookahead;

            expectSameAsReference(PathExplorationDir::BOTH, 0, 3, options);
        }
    }
}

TEST_F(PathExploratorCyclicTest, endLabelsAgreeWithThePruningIndexOnACyclicGraph) {
    std::vector<Arc> arcs;
    randomArcs(10, 3, 5, arcs);
    build(10, 6, arcs, 3);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    for (const PathExplorationDir direction : everyDirection) {
        for (uint64_t maxHops = 1; maxHops <= 4; maxHops++) {
            ExplorationOptions filtered;
            filtered._endLabels = &_endLabels;

            // The rows must not depend on whether the index was built, only the work
            expectSameAsReference(direction, 0, maxHops, filtered);

            PathDistanceIndex index;
            index.build(view, _endLabels, direction, std::nullopt, maxHops);

            ExplorationOptions pruned = filtered;
            pruned._distanceIndex = &index;
            expectSameAsReference(direction, 0, maxHops, pruned);
        }
    }
}

TEST_F(PathExploratorCyclicTest, boundEndsAgreeWithTheTargetIndexOnACyclicGraph) {
    std::vector<Arc> arcs;
    randomArcs(10, 3, 13, arcs);
    build(10, 6, arcs, 0);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    // Every seed aimed at the node three along, so a target is reachable only around a cycle
    ColumnNodeIDs targets;
    std::vector<NodeID> distinctTargets;
    for (size_t node = 0; node < _nodeCount; node++) {
        const NodeID target((node + 3) % _nodeCount);
        targets.push_back(target);
        distinctTargets.push_back(target);
    }

    std::sort(distinctTargets.begin(), distinctTargets.end());
    distinctTargets.erase(std::unique(distinctTargets.begin(), distinctTargets.end()), distinctTargets.end());

    for (uint64_t maxHops = 1; maxHops <= 4; maxHops++) {
        SCOPED_TRACE("max hops " + std::to_string(maxHops));

        ReferenceEnumerator reference(_adjacency, PathExplorationDir::FORWARD, 0, maxHops);

        std::vector<PathRow> everyRow;
        reference.enumerate(input, everyRow);

        std::vector<PathRow> expected;
        for (const PathRow& row : everyRow) {
            if (row._target == targets.getRaw()[row._index].getValue()) {
                expected.push_back(row);
            }
        }

        ExplorationOptions bound;
        bound._endNodes = &targets;

        std::vector<PathRow> actual;
        collectPaths(view, input, PathExplorationDir::FORWARD, 0, maxHops, bound, actual);
        expectSameRows(expected, actual);

        PathTargetIndex index;
        index.build(view, distinctTargets, PathExplorationDir::FORWARD, std::nullopt, maxHops);

        ExplorationOptions indexed = bound;
        indexed._targetIndex = &index;

        std::vector<PathRow> indexedRows;
        collectPaths(view, input, PathExplorationDir::FORWARD, 0, maxHops, indexed, indexedRows);
        expectSameRows(expected, indexedRows);
    }
}

TEST_F(PathExploratorCyclicTest, distinctEndsMatchTheDeduplicatedEnumerationOnACyclicGraph) {
    std::vector<Arc> arcs;
    randomArcs(10, 3, 17, arcs);
    build(10, 6, arcs, 3);

    for (const PathExplorationDir direction : everyDirection) {
        ExplorationOptions distinct;
        distinct._distinctEnds = true;
        distinct._collectPaths = false;

        expectSameAsReference(direction, 0, 3, distinct);

        // A minimum of one hop is exact only when the walk keeps to one direction
        if (direction != PathExplorationDir::BOTH) {
            expectSameAsReference(direction, 1, 3, distinct);
        }
    }
}

// Past a minimum of one hop the walk answers the distinct ends itself, remembering a subtree
// only when no edge it held above that subtree was in its way. Cycles are where that bites: a
// trail can be forced to re-enter a node the walk has already left.
TEST_F(PathExploratorCyclicTest, distinctEndsAtADeepMinimumAgreeWithTheReferenceOnACyclicGraph) {
    for (uint64_t seed = 1; seed <= 4; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<Arc> arcs;
        randomArcs(10, 3, seed, arcs);
        build(10, 7, arcs, 4);

        for (const PathExplorationDir direction : everyDirection) {
            for (const uint64_t hops : {uint64_t {2}, uint64_t {3}, uint64_t {4}}) {
                for (const size_t chunk : {size_t {1}, size_t {2}, ChunkConfig::CHUNK_SIZE}) {
                    ExplorationOptions exact;
                    exact._distinctEnds = true;
                    exact._collectPaths = false;
                    exact._maxCount = chunk;

                    expectSameAsReference(direction, hops, hops, exact);
                    expectSameAsReference(direction, 2, hops, exact);
                }
            }
        }
    }
}

// Four prefixes reach the middle node at the same remaining depth. Enumerating walks the
// subtree below it four times; asked only for the ends, the walk owes it one visit.
TEST_F(PathExploratorCyclicTest, remembersASubtreeReachedByManyPrefixes) {
    const std::vector<Arc> arcs {
        {0, 1}, {0, 2}, {0, 3}, {0, 4},
        {1, 5}, {2, 5}, {3, 5}, {4, 5},
        {5, 6}, {5, 7}, {5, 8},
    };
    build(9, 5, arcs, 3);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    input.push_back(NodeID {0});

    ExplorationOptions enumerating;
    std::vector<PathRow> paths;
    const size_t enumerationChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 3, 3, enumerating, paths);

    ExplorationOptions distinct;
    distinct._distinctEnds = true;
    distinct._collectPaths = false;
    std::vector<PathRow> pairs;
    const size_t distinctChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 3, 3, distinct, pairs);

    EXPECT_EQ(paths.size(), 12u);
    EXPECT_EQ(pairs.size(), 3u);
    EXPECT_LT(distinctChecks, enumerationChecks) << distinctChecks << " of " << enumerationChecks;
}

TEST_F(PathExploratorCyclicTest, tombstonedEdgesLeaveTheRemainingCyclesIntact) {
    std::vector<Arc> arcs;
    randomArcs(10, 3, 19, arcs);
    build(10, 6, arcs, 3);

    deleteEdge(0);
    deleteEdge(4);
    deleteEdge(11);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    buildAdjacency(reader.getView(), _nodeCount, _adjacency);

    for (const PathExplorationDir direction : everyDirection) {
        expectSameAsReference(direction, 0, 3, ExplorationOptions {});
        expectSameAsReference(direction, 2, 3, ExplorationOptions {});
    }
}

TEST_F(PathExploratorCyclicTest, enumeratesEveryTrailOfACompleteGraph) {
    // Every ordered pair of five nodes: the densest cycle structure there is, and the one
    // that puts the most edges on a single trail for the signature to keep apart
    std::vector<Arc> arcs;
    for (size_t source = 0; source < 5; source++) {
        for (size_t target = 0; target < 5; target++) {
            if (source != target) {
                arcs.push_back(Arc {._source = source, ._target = target, ._typeB = false});
            }
        }
    }

    build(5, 3, arcs, 2);

    for (uint64_t maxHops = 1; maxHops <= 4; maxHops++) {
        expectSameAsReference(PathExplorationDir::FORWARD, 0, maxHops, ExplorationOptions {});
    }

    ExplorationOptions options;
    options._maxCount = 3;
    expectSameAsReference(PathExplorationDir::BOTH, 1, 3, options);
}

TEST_F(PathExploratorCyclicTest, walksACycleLongerThanTheTrailSignature) {
    // Seventy edges on one cycle: past the sixty-fourth every signature bit is set, so the
    // walk can only tell a repeated edge from a fresh one by scanning the path itself
    constexpr size_t cycleLength = 70;

    std::vector<Arc> arcs;
    for (size_t node = 0; node < cycleLength; node++) {
        arcs.push_back(Arc {._source = node, ._target = (node + 1) % cycleLength, ._typeB = false});
    }

    buildOverAssignedIDs(cycleLength, arcs);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    input.push_back(NodeID(0));

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, unbounded, ExplorationOptions {}, rows);

    // One trail per prefix of the cycle, the longest closing it back on the seed
    ASSERT_EQ(rows.size(), cycleLength);

    std::sort(rows.begin(), rows.end(), [](const PathRow& left, const PathRow& right) {
        return left._edges.size() < right._edges.size();
    });

    for (size_t hops = 1; hops <= cycleLength; hops++) {
        const PathRow& row = rows[hops - 1];
        EXPECT_EQ(row._edges.size(), hops);
        EXPECT_EQ(row._target, hops % cycleLength);
    }

    ReferenceEnumerator reference(_adjacency, PathExplorationDir::FORWARD, 1, unbounded);
    std::vector<PathRow> expected;
    reference.enumerate(input, expected);
    EXPECT_EQ(expected.size(), cycleLength);
}

TEST_F(PathExploratorCyclicTest, keepsParallelEdgesBetweenOnePairApart) {
    // Two edges each way between the same pair, so every trail has a twin differing only in
    // which of the parallel edges it took
    std::vector<Arc> arcs {
        Arc {._source = 0, ._target = 1, ._typeB = false},
        Arc {._source = 0, ._target = 1, ._typeB = false},
        Arc {._source = 1, ._target = 0, ._typeB = false},
        Arc {._source = 1, ._target = 0, ._typeB = true},
        Arc {._source = 1, ._target = 1, ._typeB = false},
    };

    build(2, 2, arcs, 0);

    for (const PathExplorationDir direction : everyDirection) {
        for (uint64_t maxHops = 1; maxHops <= 5; maxHops++) {
            expectSameAsReference(direction, 0, maxHops, ExplorationOptions {});
        }
    }

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    input.push_back(NodeID(0));

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 1, ExplorationOptions {}, rows);

    // Both parallel edges are their own row, and the two rows differ only by the edge
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0]._target, 1u);
    EXPECT_EQ(rows[1]._target, 1u);
    EXPECT_NE(rows[0]._edges.front(), rows[1]._edges.front());
}

TEST_F(PathExploratorCyclicTest, walksTwoCyclesSharingOneNode) {
    // A figure of eight: 0->1->2->0 and 0->3->4->0 meet at node 0, so a trail may run both
    // loops in either order but neither of them twice
    std::vector<Arc> arcs {
        Arc {._source = 0, ._target = 1, ._typeB = false},
        Arc {._source = 1, ._target = 2, ._typeB = false},
        Arc {._source = 2, ._target = 0, ._typeB = false},
        Arc {._source = 0, ._target = 3, ._typeB = true},
        Arc {._source = 3, ._target = 4, ._typeB = true},
        Arc {._source = 4, ._target = 0, ._typeB = true},
    };

    build(5, 3, arcs, 0);

    for (const PathExplorationDir direction : everyDirection) {
        expectSameAsReference(direction, 0, unbounded, ExplorationOptions {});
    }

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    input.push_back(NodeID(0));

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 6, 6, ExplorationOptions {}, rows);

    // Six hops is both loops, and there are exactly two orders to run them in
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0]._target, 0u);
    EXPECT_EQ(rows[1]._target, 0u);
    EXPECT_NE(rows[0]._edges, rows[1]._edges);
}
