#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "iterators/ChunkConfig.h"
#include "iterators/ExcludedEdges.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathStep.h"
#include "iterators/ReversedPathExplorator.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "PathExplorationReference.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using EndRow = std::pair<size_t, uint64_t>;

}

// MATCH (x)-[e1]-(s)-[*]->(t) with t in an end set, walked from the ends: the trail found
// from an end serves every input row holding its seed, and is a row of only those that did
// not bind one of its edges before it
class ReversedPathExploratorExcludedEdgesTest : public TuringTest {
protected:
    static constexpr size_t NODE_COUNT = HubGraph::nodeCount;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        buildHubGraph(*_graph, *_jobSystem, _hubGraph);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Every node on three rows, each row excluding up to two edges, one of them often
    // touching its seed, so the rows of one seed exclude different edges
    void excludingRows() {
        const Adjacency& adjacency = _hubGraph._adjacency;
        std::mt19937_64 generator(42);

        std::vector<uint64_t> edges;
        for (const std::vector<ReferenceEdge>& outs : adjacency._outs) {
            for (const ReferenceEdge& out : outs) {
                edges.push_back(out._edge);
            }
        }

        _input.clear();
        _rowExcluded.clear();
        _offsets.assign(1, 0);
        _excludedEdges.clear();

        for (size_t repeat = 0; repeat < 3; repeat++) {
            for (size_t node = 0; node < NODE_COUNT; node++) {
                _input.push_back(NodeID(node));

                std::vector<uint64_t>& excluded = _rowExcluded.emplace_back();
                const std::vector<ReferenceEdge>& touching = repeat % 2 == 0 ? adjacency._outs[node] : adjacency._ins[node];
                if (!touching.empty() && repeat != 2) {
                    excluded.push_back(touching[generator() % touching.size()]._edge);
                }
                if (generator() % 2 == 0) {
                    excluded.push_back(edges[generator() % edges.size()]);
                }

                for (const uint64_t edge : excluded) {
                    _excludedEdges.push_back(EdgeID(edge));
                }
                _offsets.push_back(_excludedEdges.size());
            }
        }
    }

    ExcludedEdges excludedEdges() const {
        return ExcludedEdges {._offsets = _offsets, ._edges = _excludedEdges};
    }

    void expectedRows(std::span<const NodeID> endNodeSet,
                      PathExplorationDir direction,
                      uint64_t minHops,
                      uint64_t maxHops,
                      bool distinctEnds,
                      std::vector<EndRow>& rows) const {
        std::vector<bool> ends(NODE_COUNT, false);
        for (const NodeID end : endNodeSet) {
            ends[end.getValue()] = true;
        }

        ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);
        reference.setEnds(&ends);
        reference.setExcludedEdges(&_rowExcluded);

        std::vector<PathRow> paths;
        reference.enumerate(_input, paths);

        rows.clear();
        for (const PathRow& path : paths) {
            rows.emplace_back(path._index, path._target);
        }

        std::sort(rows.begin(), rows.end());
        if (distinctEnds) {
            rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
        }
    }

    void reversedRows(const GraphView& view,
                      std::span<const NodeID> endNodeSet,
                      PathExplorationDir direction,
                      uint64_t minHops,
                      uint64_t maxHops,
                      bool distinctEnds,
                      size_t maxCount,
                      std::vector<EndRow>& rows) const {
        ColumnVector<size_t> indices;
        ColumnNodeIDs targets;

        const std::vector<PathStep> body {PathStep {._direction = direction}};
        ReversedPathExplorator explorator(view, &_input, endNodeSet, nullptr, body, minHops, maxHops);
        explorator.setIndices(&indices);
        explorator.setTargets(&targets);
        explorator.setDistinctEnds(distinctEnds);
        explorator.setExcludedEdges(excludedEdges());

        rows.clear();
        while (explorator.isValid()) {
            explorator.fill(maxCount);
            ASSERT_LE(indices.size(), maxCount);
            ASSERT_EQ(targets.size(), indices.size());

            for (size_t row = 0; row < indices.size(); row++) {
                rows.emplace_back(indices[row], targets[row].getValue());
            }
        }

        std::sort(rows.begin(), rows.end());
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;

    ColumnNodeIDs _input;
    std::vector<std::vector<uint64_t>> _rowExcluded;
    std::vector<size_t> _offsets;
    std::vector<EdgeID> _excludedEdges;
};

TEST_F(ReversedPathExploratorExcludedEdgesTest, keepsTheRowsWhoseTrailTakesNoneOfTheirEdges) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    excludingRows();

    std::vector<NodeID> spread;
    for (size_t node = 0; node < NODE_COUNT; node += 4) {
        spread.push_back(NodeID(node));
    }

    const std::vector<std::pair<uint64_t, uint64_t>> bounds {
        {1, 1}, {1, 3}, {0, 2}, {2, unbounded}, {0, unbounded},
    };

    size_t comparedRows = 0;
    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : bounds) {
            for (const bool distinctEnds : {false, true}) {
                std::vector<EndRow> expected;
                expectedRows(spread, direction, minHops, maxHops, distinctEnds, expected);

                for (const size_t maxCount : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
                    std::vector<EndRow> actual;
                    reversedRows(view, spread, direction, minHops, maxHops, distinctEnds, maxCount, actual);

                    EXPECT_EQ(actual, expected) << "direction " << static_cast<int>(direction)
                                                << " hops " << minHops << " to " << maxHops
                                                << " distinct " << distinctEnds
                                                << " chunk " << maxCount;
                }

                comparedRows += expected.size();
            }
        }
    }

    EXPECT_GT(comparedRows, 0u);
}
