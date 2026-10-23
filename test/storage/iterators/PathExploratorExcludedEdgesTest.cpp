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
#include "iterators/ExcludedEdges.h"
#include "iterators/PathExplorationDir.h"
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

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

}

// MATCH (a)-[e1]-(s)-[*]-(m): the walk from each input row may not take the edges the
// earlier hops of its row bound, which the explorator reads as one exclusion set per row.
// Rows of one node exclude different edges, so seeds sharing a batch of the distinct search
// exclude different edges.
class PathExploratorExcludedEdgesTest : public TuringTest {
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

    // Every node over and over, more rows than one batch of the distinct search holds, each
    // row excluding up to three edges, one of them often touching its seed
    void excludingRows(uint64_t seed) {
        std::mt19937_64 generator(seed);

        std::vector<uint64_t> edges;
        for (const std::vector<ReferenceEdge>& outs : _adjacency._outs) {
            for (const ReferenceEdge& out : outs) {
                edges.push_back(out._edge);
            }
        }

        const size_t repeats = PathTargetIndex::targetsPerBatch / _nodeCount + 2;

        _input.clear();
        _endNodes.clear();
        _rowExcluded.clear();
        _offsets.assign(1, 0);
        _excludedEdges.clear();

        std::uniform_int_distribution<size_t> anyNode(0, _nodeCount - 1);
        std::uniform_int_distribution<size_t> anyEdge(0, edges.size() - 1);

        for (size_t repeat = 0; repeat < repeats; repeat++) {
            for (size_t node = 0; node < _nodeCount; node++) {
                _input.push_back(NodeID(node));
                _endNodes.push_back(NodeID(anyNode(generator)));

                std::vector<uint64_t>& excluded = _rowExcluded.emplace_back();
                const std::vector<ReferenceEdge>& touching = repeat % 2 == 0 ? _adjacency._outs[node] : _adjacency._ins[node];
                if (!touching.empty() && generator() % 3 != 0) {
                    excluded.push_back(touching[generator() % touching.size()]._edge);
                }

                const size_t others = generator() % 3;
                for (size_t other = 0; other < others; other++) {
                    excluded.push_back(edges[anyEdge(generator)]);
                }

                std::sort(excluded.begin(), excluded.end());
                excluded.erase(std::unique(excluded.begin(), excluded.end()), excluded.end());

                for (const uint64_t edge : excluded) {
                    _excludedEdges.push_back(EdgeID(edge));
                }
                _offsets.push_back(_excludedEdges.size());
            }
        }
    }

    void expectSameAsReference(const GraphView& view,
                               PathExplorationDir direction,
                               uint64_t minHops,
                               uint64_t maxHops,
                               bool distinct,
                               bool constrainsEnds,
                               HopPredicate predicate,
                               size_t maxCount) {
        SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction))
                     + " hops " + std::to_string(minHops) + ".." + std::to_string(maxHops)
                     + (distinct ? " distinct" : " paths")
                     + (constrainsEnds ? " end nodes" : "")
                     + (predicate ? " predicate" : "")
                     + " chunk " + std::to_string(maxCount));

        ReferenceEnumerator reference(_adjacency, direction, minHops, maxHops);
        reference.setHopPredicate(predicate);
        reference.setExcludedEdges(&_rowExcluded);

        std::vector<PathRow> expected;
        reference.enumerate(_input, expected);

        if (constrainsEnds) {
            std::erase_if(expected, [this](const PathRow& row) {
                return row._target != _endNodes[row._index].getValue();
            });
        }

        if (distinct) {
            std::vector<PathRow> pairs;
            distinctPairs(expected, pairs);
            expected = pairs;
        }

        PredicateHopFilter hopFilter(predicate);

        ExplorationOptions options;
        options._maxCount = maxCount;
        options._distinctEnds = distinct;
        options._collectPaths = !distinct;
        options._hopFilter = predicate ? &hopFilter : nullptr;
        options._endNodes = constrainsEnds ? &_endNodes : nullptr;
        options._excludedEdges = ExcludedEdges {._offsets = _offsets, ._edges = _excludedEdges};

        std::vector<PathRow> actual;
        collectPaths(view, _input, direction, minHops, maxHops, options, actual);
        expectSameRows(expected, actual);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    size_t _nodeCount {0};

    ColumnNodeIDs _input;
    ColumnNodeIDs _endNodes;
    std::vector<std::vector<uint64_t>> _rowExcluded;
    std::vector<size_t> _offsets;
    std::vector<EdgeID> _excludedEdges;
};

TEST_F(PathExploratorExcludedEdgesTest, matchesTheEnumerationOnRandomMultigraphs) {
    const PathExplorationDir directions[] = {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH};

    for (uint64_t seed = 1; seed <= 6; seed++) {
        SCOPED_TRACE("seed " + std::to_string(seed));

        std::vector<GeneratedArc> arcs;
        randomArcs(9, seed % 2 == 0 ? 1 : 2, seed, arcs);
        build(9, arcs);
        excludingRows(seed);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        for (const PathExplorationDir direction : directions) {
            for (const uint64_t minHops : {uint64_t {0}, uint64_t {1}, uint64_t {2}}) {
                for (const uint64_t maxHops : {uint64_t {1}, uint64_t {2}, uint64_t {3}, uint64_t {4}, unbounded}) {
                    if (maxHops < minHops) {
                        continue;
                    }

                    for (const bool distinct : {false, true}) {
                        if (!distinct && maxHops > 3) {
                            continue;
                        }

                        for (const bool constrainsEnds : {false, true}) {
                            for (const HopPredicate predicate : {static_cast<HopPredicate>(nullptr), &evenEdgesOnly}) {
                                for (const size_t maxCount : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
                                    expectSameAsReference(view, direction, minHops, maxHops, distinct, constrainsEnds, predicate, maxCount);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

TEST_F(PathExploratorExcludedEdgesTest, walksNothingFromASeedWhoseEveryEdgeIsExcluded) {
    std::vector<GeneratedArc> arcs;
    randomArcs(9, 2, 7, arcs);
    build(9, arcs);

    _input.clear();
    _rowExcluded.clear();
    _offsets.assign(1, 0);
    _excludedEdges.clear();

    for (size_t node = 0; node < _nodeCount; node++) {
        _input.push_back(NodeID(node));

        std::vector<uint64_t>& excluded = _rowExcluded.emplace_back();
        for (const ReferenceEdge& out : _adjacency._outs[node]) {
            excluded.push_back(out._edge);
        }
        for (const ReferenceEdge& in : _adjacency._ins[node]) {
            excluded.push_back(in._edge);
        }

        std::sort(excluded.begin(), excluded.end());
        excluded.erase(std::unique(excluded.begin(), excluded.end()), excluded.end());

        for (const uint64_t edge : excluded) {
            _excludedEdges.push_back(EdgeID(edge));
        }
        _offsets.push_back(_excludedEdges.size());
    }

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    for (const bool distinct : {false, true}) {
        ExplorationOptions options;
        options._distinctEnds = distinct;
        options._collectPaths = !distinct;
        options._excludedEdges = ExcludedEdges {._offsets = _offsets, ._edges = _excludedEdges};

        std::vector<PathRow> rows;
        collectPaths(reader.getView(), _input, PathExplorationDir::BOTH, 1, unbounded, options, rows);
        EXPECT_TRUE(rows.empty()) << (distinct ? "distinct" : "paths");
    }
}
