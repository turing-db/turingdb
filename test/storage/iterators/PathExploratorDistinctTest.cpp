#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathTargetIndex.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

bool nothingPasses(uint64_t, uint64_t, uint64_t) {
    return false;
}

// The configurations the distinct mode is exact for: a minimum of one hop only when the
// walk is directed, since undirected the one-edge backtrack is a closed walk with no trail
bool exactDistinct(PathExplorationDir direction, uint64_t minHops) {
    return minHops == 0 || direction != PathExplorationDir::BOTH;
}

// The (seed row, end) pairs of the enumerated rows, each once and without a path: what the
// distinct mode must emit
void distinctPairs(const std::vector<PathRow>& rows, std::vector<PathRow>& pairs) {
    pairs.clear();
    for (const PathRow& row : rows) {
        pairs.push_back({row._index, row._target, {}});
    }

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
}

}

class PathExploratorDistinctTest : public TuringTest {
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

    // Every node three times over, more rows than one batch of the search holds
    void repeatedNodes(ColumnNodeIDs& input) const {
        input.clear();
        for (size_t repeat = 0; repeat < 3; repeat++) {
            for (size_t node = 0; node < nodeCount; node++) {
                input.push_back(NodeID(node));
            }
        }
    }

    void expectDistinctRows(const GraphView& view,
                            const ColumnNodeIDs& input,
                            PathExplorationDir direction,
                            uint64_t minHops,
                            uint64_t maxHops,
                            ExplorationOptions options,
                            HopPredicate predicate = nullptr) {
        SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " hops " + std::to_string(minHops) + " to " + std::to_string(maxHops) + " chunk " + std::to_string(options._maxCount));

        ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);
        if (options._edgeType) {
            reference.setEdgeType(options._edgeType->getValue());
        }
        if (options._endLabels) {
            reference.setEnds(&_hubGraph._ends);
        }
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

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
};

TEST_F(PathExploratorDistinctTest, matchesTheDeduplicatedEnumeration) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    repeatedNodes(input);
    ASSERT_GT(input.size(), PathTargetIndex::targetsPerBatch);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t minHops : {uint64_t {0}, uint64_t {1}}) {
            for (const uint64_t maxHops : {uint64_t {0}, uint64_t {1}, uint64_t {2}, uint64_t {4}, unbounded}) {
                if (maxHops < minHops || !exactDistinct(direction, minHops)) {
                    continue;
                }

                for (const size_t maxCount : {size_t {1}, size_t {2}, ChunkConfig::CHUNK_SIZE}) {
                    ExplorationOptions options;
                    options._maxCount = maxCount;

                    expectDistinctRows(view, input, direction, minHops, maxHops, options);
                }
            }
        }
    }
}

TEST_F(PathExploratorDistinctTest, typeFilterAgreesWithTheDeduplicatedEnumeration) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    repeatedNodes(input);

    for (const EdgeTypeID edgeType : {_hubGraph._typeA, _hubGraph._typeB}) {
        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
            for (const uint64_t minHops : {uint64_t {0}, uint64_t {1}}) {
                if (!exactDistinct(direction, minHops)) {
                    continue;
                }

                ExplorationOptions options;
                options._edgeType = edgeType;

                expectDistinctRows(view, input, direction, minHops, unbounded, options);
            }
        }
    }
}

TEST_F(PathExploratorDistinctTest, hopFilterAgreesWithTheDeduplicatedEnumeration) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    repeatedNodes(input);

    PredicateHopFilter evenFilter(evenEdgesOnly);
    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t minHops : {uint64_t {0}, uint64_t {1}}) {
            if (!exactDistinct(direction, minHops)) {
                continue;
            }

            ExplorationOptions options;
            options._hopFilter = &evenFilter;
            options._maxCount = 1;

            expectDistinctRows(view, input, direction, minHops, unbounded, options, evenEdgesOnly);
        }
    }

    // Nothing passes: the zero-length rows alone, and none at all past a minimum of one
    PredicateHopFilter noneFilter(nothingPasses);
    ExplorationOptions options;
    options._hopFilter = &noneFilter;
    options._distinctEnds = true;

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 0, unbounded, options, rows);
    EXPECT_EQ(rows.size(), input.size());

    collectPaths(view, input, PathExplorationDir::FORWARD, 1, unbounded, options, rows);
    EXPECT_TRUE(rows.empty());
}

TEST_F(PathExploratorDistinctTest, endLabelsAndEndNodesGateTheEmission) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    repeatedNodes(input);

    const LabelSet endLabels = LabelSet::fromList({_hubGraph._labelT});

    ColumnNodeIDs endNodes;
    for (size_t row = 0; row < input.size(); row++) {
        endNodes.push_back(NodeID((row * 5 + 1) % nodeCount));
    }

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t minHops : {uint64_t {0}, uint64_t {1}}) {
            if (!exactDistinct(direction, minHops)) {
                continue;
            }

            ExplorationOptions labelled;
            labelled._endLabels = &endLabels;
            expectDistinctRows(view, input, direction, minHops, unbounded, labelled);

            ExplorationOptions bound;
            bound._endNodes = &endNodes;
            bound._maxCount = 1;
            expectDistinctRows(view, input, direction, minHops, 3, bound);

            ExplorationOptions both;
            both._endLabels = &endLabels;
            both._endNodes = &endNodes;
            expectDistinctRows(view, input, direction, minHops, unbounded, both);
        }
    }
}

TEST_F(PathExploratorDistinctTest, reportsAClosedTrailBackToTheSeedOnce) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // hub->c1->c2->t->hub is the one cycle: the hub is its own end after four hops, and
    // with a minimum of one that row comes out exactly once
    const ColumnNodeIDs input {NodeID(_hubGraph._hub)};

    ExplorationOptions options;
    options._distinctEnds = true;

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, unbounded, options, rows);

    size_t backToTheSeed = 0;
    for (const PathRow& row : rows) {
        backToTheSeed += row._target == _hubGraph._hub ? 1 : 0;
    }
    EXPECT_EQ(backToTheSeed, 1u);

    // Three hops are one short of the cycle
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, rows);
    for (const PathRow& row : rows) {
        EXPECT_NE(row._target, _hubGraph._hub);
    }

    // With a minimum of zero the seed is its own end at level zero and nowhere else
    collectPaths(view, input, PathExplorationDir::FORWARD, 0, unbounded, options, rows);
    backToTheSeed = 0;
    for (const PathRow& row : rows) {
        backToTheSeed += row._target == _hubGraph._hub ? 1 : 0;
    }
    EXPECT_EQ(backToTheSeed, 1u);
}

TEST_F(PathExploratorDistinctTest, expandsEachReachedNodeOncePerBatch) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    repeatedNodes(input);

    ExplorationOptions enumerating;
    std::vector<PathRow> paths;
    const size_t enumerationChecks = collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, enumerating, paths);

    ExplorationOptions distinct;
    distinct._distinctEnds = true;
    std::vector<PathRow> pairs;
    const size_t distinctChecks = collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, distinct, pairs);

    // The trails through the cycle and the dead branch multiply; the search reads each
    // node's edges once per level per batch
    EXPECT_LT(distinctChecks, enumerationChecks) << distinctChecks << " of " << enumerationChecks;
    EXPECT_LT(pairs.size(), paths.size());
}
