#include <algorithm>
#include <memory>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathExplorator.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

void distinctPairs(const std::vector<PathRow>& rows, std::vector<PathRow>& pairs) {
    pairs.clear();
    for (const PathRow& row : rows) {
        pairs.push_back({row._index, row._target, {}});
    }

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
}

}

// A minimum above one hop takes the distinct walk down the depth-first path rather than the
// level search, where a subtree no held edge constrained stands in for every later arrival
// at its node. The stand-in is keyed by node and remaining budget, so it must not let one
// node answer for another.
class PathExploratorDeepDistinctTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        buildHubGraph(*_graph, *_jobSystem, _hubGraph);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void expectDistinctRows(const GraphView& view,
                            const ColumnNodeIDs& input,
                            PathExplorationDir direction,
                            uint64_t minHops,
                            uint64_t maxHops) {
        SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " hops " + std::to_string(minHops) + " to " + std::to_string(maxHops));

        ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);

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

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
};

TEST_F(PathExploratorDeepDistinctTest, reportsEveryEndPastAMinimumOfTwoHops) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    for (size_t node = 0; node < HubGraph::nodeCount; node++) {
        input.push_back(NodeID(node));
    }

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD}) {
        for (const uint64_t minHops : {uint64_t {2}, uint64_t {3}}) {
            for (const uint64_t maxHops : {uint64_t {3}, uint64_t {5}, unbounded}) {
                if (maxHops < minHops) {
                    continue;
                }

                expectDistinctRows(view, input, direction, minHops, maxHops);
            }
        }
    }
}
