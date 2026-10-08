#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathExplorator.h"
#include "iterators/PathHopFilter.h"
#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "list/PathTrie.h"
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

struct BodyStep {
    PathExplorationDir _direction {PathExplorationDir::FORWARD};
    std::optional<EdgeTypeID> _edgeType;
};

struct BodyOptions {
    PathHopFilter* _secondStepFilter {nullptr};
    bool _distinct {false};
    size_t _maxCount {ChunkConfig::CHUNK_SIZE};
    const LabelSet* _endLabels {nullptr};
    const PathDistanceIndex* _distanceIndex {nullptr};
    bool* _searchesLevels {nullptr};
};

// Rejects the hops of the body's second step that land back on the node the repetition
// started from
class NoReturnHopFilter : public PathHopFilter {
public:
    NoReturnHopFilter() {
    }

    ~NoReturnHopFilter() override {
    }

    size_t filter(std::span<PathHopFrame> frames, std::span<NodeID> nodes, std::span<EdgeID> edges) override {
        size_t candidate = 0;
        size_t kept = 0;
        for (PathHopFrame& frame : frames) {
            EXPECT_EQ(frame._repetitionNodes.size(), 1);
            EXPECT_EQ(frame._repetitionEdges.size(), 1);

            const size_t frameEnd = candidate + frame._candidateCount;
            const size_t frameBegin = kept;

            for (; candidate < frameEnd; candidate++) {
                if (nodes[candidate] != frame._repetitionNodes.front()) {
                    nodes[kept] = nodes[candidate];
                    edges[kept] = edges[candidate];
                    kept++;
                }
            }

            frame._candidateCount = kept - frameBegin;
        }

        return kept;
    }

    bool readsRepetition() const override {
        return true;
    }
};

// The trails repeating a body of steps, enumerated recursively: the oracle of the explorator
class BodyReference {
public:
    BodyReference(const Adjacency& adjacency, std::span<const BodyStep> steps, uint64_t minRepetitions, uint64_t maxRepetitions)
        : _adjacency(adjacency),
        _steps(steps),
        _minRepetitions(minRepetitions),
        _maxRepetitions(maxRepetitions)
    {
    }

    void setRefusesReturns() { _refusesReturns = true; }
    void setEnds(const std::vector<bool>* ends) { _ends = ends; }

    void enumerate(const ColumnNodeIDs& seeds, std::vector<PathRow>& rows) {
        rows.clear();
        for (size_t row = 0; row < seeds.size(); row++) {
            std::vector<uint64_t> edges;
            std::vector<uint64_t> nodes;
            walk(row, seeds[row].getValue(), edges, nodes, rows);
        }
    }

private:
    const Adjacency& _adjacency;
    std::span<const BodyStep> _steps;
    uint64_t _minRepetitions {0};
    uint64_t _maxRepetitions {0};
    bool _refusesReturns {false};
    const std::vector<bool>* _ends {nullptr};

    void walk(size_t seedRow, uint64_t node, std::vector<uint64_t>& edges, std::vector<uint64_t>& nodes, std::vector<PathRow>& rows) {
        const size_t stepCount = _steps.size();
        const uint64_t depth = edges.size();
        const uint64_t repetitions = depth / stepCount;
        const bool endsRepetition = depth % stepCount == 0;

        const bool ends = !_ends || (*_ends)[node];
        if (endsRepetition && repetitions >= _minRepetitions && ends) {
            rows.push_back({seedRow, node, edges});
        }

        if (endsRepetition && repetitions >= _maxRepetitions) {
            return;
        }

        const BodyStep& step = _steps[depth % stepCount];
        nodes.push_back(node);

        if (step._direction != PathExplorationDir::BACKWARD) {
            descend(seedRow, step, _adjacency._outs[node], edges, nodes, rows);
        }

        if (step._direction != PathExplorationDir::FORWARD) {
            descend(seedRow, step, _adjacency._ins[node], edges, nodes, rows);
        }

        nodes.pop_back();
    }

    void descend(size_t seedRow,
                 const BodyStep& step,
                 const std::vector<ReferenceEdge>& candidates,
                 std::vector<uint64_t>& edges,
                 std::vector<uint64_t>& nodes,
                 std::vector<PathRow>& rows) {
        const size_t position = edges.size() % _steps.size();
        const uint64_t repetitionStart = nodes[nodes.size() - 1 - position];

        for (const ReferenceEdge& candidate : candidates) {
            const bool wrongType = step._edgeType && candidate._type != step._edgeType->getValue();
            const bool onTrail = std::find(edges.begin(), edges.end(), candidate._edge) != edges.end();
            const bool returns = _refusesReturns && position == 1 && candidate._other == repetitionStart;
            if (wrongType || onTrail || returns) {
                continue;
            }

            edges.push_back(candidate._edge);
            walk(seedRow, candidate._other, edges, nodes, rows);
            edges.pop_back();
        }
    }
};

// Edges of two types alternating around two cycles, 0 -A-> 1 -B-> 2 -A-> 3 -B-> 0 and
// 3 -A-> 4 -B-> 5 -A-> 1, with B shortcuts 1 -> 4 and 5 -> 2 and an A shortcut 5 -> 0, and a
// cycle of A edges alone, 6 -> 7 -> 8 -> 6
struct BraidGraph {
    static constexpr size_t nodeCount = 9;

    EdgeTypeID _typeA;
    EdgeTypeID _typeB;
};

void buildBraidGraph(Graph& graph, JobSystem& jobSystem, BraidGraph& braid) {
    auto change = graph.newChange();
    auto* commitBuilder = change->access().getTip();
    auto& builder = commitBuilder->newBuilder();
    auto& metadata = builder.getMetadata();

    const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
    braid._typeA = metadata.getOrCreateEdgeType("A");
    braid._typeB = metadata.getOrCreateEdgeType("B");

    std::vector<NodeID> nodes;
    for (size_t node = 0; node < BraidGraph::nodeCount; node++) {
        nodes.push_back(builder.addNode(plain));
    }

    const std::vector<std::pair<size_t, size_t>> edgesA {{0, 1}, {2, 3}, {3, 4}, {5, 1}, {5, 0}, {6, 7}, {7, 8}, {8, 6}};
    const std::vector<std::pair<size_t, size_t>> edgesB {{1, 2}, {3, 0}, {4, 5}, {1, 4}, {5, 2}};

    for (const auto& [source, target] : edgesA) {
        builder.addEdge(braid._typeA, nodes[source], nodes[target]);
    }

    for (const auto& [source, target] : edgesB) {
        builder.addEdge(braid._typeB, nodes[source], nodes[target]);
    }

    ASSERT_TRUE(change->access().submit(jobSystem));
}

uint64_t edgeBound(uint64_t repetitions, size_t stepCount) {
    return repetitions == unbounded ? unbounded : repetitions * stepCount;
}

void readIDs(const ListView& list, std::vector<uint64_t>& ids) {
    ids.clear();
    for (const ListElementView& element : list) {
        ids.push_back(element.getAs<uint64_t>());
    }
}

}

class PathExploratorBodyTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = HubGraph::nodeCount;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        buildHubGraph(*_graph, *_jobSystem, _hubGraph);

        for (size_t node = 0; node < nodeCount; node++) {
            _input.push_back(NodeID(node));
        }

        _braidGraph = Graph::create();
        buildBraidGraph(*_braidGraph, *_jobSystem, _braid);

        for (size_t node = 0; node < BraidGraph::nodeCount; node++) {
            _braidInput.push_back(NodeID(node));
        }

        const FrozenCommitTx transaction = _braidGraph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), BraidGraph::nodeCount, _braidAdjacency);

        _adjacency = &_hubGraph._adjacency;
        _seeds = &_input;
    }

    // Points the oracle and the seeds at the braid, for a test reading the braid's own view
    void useBraid() {
        _adjacency = &_braidAdjacency;
        _seeds = &_braidInput;
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void explore(const GraphView& view,
                 std::span<const BodyStep> steps,
                 uint64_t minRepetitions,
                 uint64_t maxRepetitions,
                 const BodyOptions& options,
                 std::vector<PathRow>& rows) {
        const bool distinct = options._distinct;
        const size_t stepCount = steps.size();

        ColumnVector<size_t> indices;
        ColumnNodeIDs targets;
        ColumnVector<PathRef> paths;
        PathTrie trie;
        ListBuffer<> buffer;

        PathExplorator explorator(view,
                                  _seeds,
                                  edgeBound(minRepetitions, stepCount),
                                  edgeBound(maxRepetitions, stepCount));
        for (const BodyStep& step : steps) {
            explorator.addStep(step._direction);
        }

        std::vector<EdgeTypeID> edgeTypes(stepCount);
        for (size_t step = 0; step < stepCount; step++) {
            if (steps[step]._edgeType) {
                edgeTypes[step] = *steps[step]._edgeType;
                explorator.setEdgeTypeFilter(step, std::span<const EdgeTypeID>(&edgeTypes[step], 1));
            }
        }

        if (options._secondStepFilter) {
            explorator.setHopFilter(1, options._secondStepFilter);
        }

        if (options._endLabels) {
            explorator.setEndLabels(options._endLabels);
            explorator.setDistanceIndex(options._distanceIndex);
        }

        explorator.setIndices(&indices);
        explorator.setTargets(&targets);
        explorator.setDistinctEnds(distinct);
        if (!distinct) {
            explorator.setPaths(&paths, &trie);
        }

        if (options._searchesLevels) {
            *options._searchesLevels = explorator.searchesLevels();
        }

        std::vector<uint64_t> ids;
        rows.clear();
        while (explorator.isValid()) {
            explorator.fill(options._maxCount);

            for (size_t row = 0; row < indices.size(); row++) {
                PathRow& emitted = rows.emplace_back();
                emitted._index = indices[row];
                emitted._target = targets[row].getValue();

                if (!distinct) {
                    readIDs(trie.expandEdges(paths[row], buffer, false), emitted._edges);
                }
            }
        }
    }

    void expectReference(const GraphView& view,
                         std::span<const BodyStep> steps,
                         uint64_t minRepetitions,
                         uint64_t maxRepetitions) {
        BodyReference reference(*_adjacency, steps, minRepetitions, maxRepetitions);
        std::vector<PathRow> expected;
        reference.enumerate(*_seeds, expected);
        EXPECT_FALSE(expected.empty());

        for (const size_t maxCount : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
            std::vector<PathRow> actual;
            explore(view, steps, minRepetitions, maxRepetitions, BodyOptions {._maxCount = maxCount}, actual);
            expectSameRows(expected, actual);
        }
    }

    // A distinct walk emits each (seed, end) pair the reference reaches once, and no path
    void expectDistinctReference(const GraphView& view,
                                 std::span<const BodyStep> steps,
                                 uint64_t minRepetitions,
                                 uint64_t maxRepetitions,
                                 bool refusesReturns,
                                 bool searchesLevels = false) {
        BodyReference reference(*_adjacency, steps, minRepetitions, maxRepetitions);
        if (refusesReturns) {
            reference.setRefusesReturns();
        }

        std::vector<PathRow> expected;
        reference.enumerate(*_seeds, expected);
        for (PathRow& row : expected) {
            row._edges.clear();
        }

        std::sort(expected.begin(), expected.end());
        expected.erase(std::unique(expected.begin(), expected.end()), expected.end());
        EXPECT_FALSE(expected.empty());

        NoReturnHopFilter filter;
        bool searchedLevels = false;
        const BodyOptions options {._secondStepFilter = refusesReturns ? &filter : nullptr,
                                   ._distinct = true,
                                   ._searchesLevels = &searchedLevels};

        std::vector<PathRow> actual;
        explore(view, steps, minRepetitions, maxRepetitions, options, actual);
        expectSameRows(expected, actual);
        EXPECT_EQ(searchedLevels, searchesLevels);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
    ColumnNodeIDs _input;

    std::unique_ptr<Graph> _braidGraph;
    BraidGraph _braid;
    Adjacency _braidAdjacency;
    ColumnNodeIDs _braidInput;

    const Adjacency* _adjacency {nullptr};
    const ColumnNodeIDs* _seeds {nullptr};
};

TEST_F(PathExploratorBodyTest, endsOnlyAfterWholeRepetitions) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<BodyStep> steps {{PathExplorationDir::FORWARD, {}}, {PathExplorationDir::FORWARD, {}}};

    const std::vector<std::pair<uint64_t, uint64_t>> bounds {
        {1, 1}, {0, 2}, {2, 3}, {1, unbounded}, {0, unbounded},
    };

    for (const auto& [minRepetitions, maxRepetitions] : bounds) {
        expectReference(view, steps, minRepetitions, maxRepetitions);
    }
}

TEST_F(PathExploratorBodyTest, takesEachStepInItsOwnDirection) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<std::vector<BodyStep>> bodies {
        {{PathExplorationDir::FORWARD, {}}, {PathExplorationDir::BACKWARD, {}}},
        {{PathExplorationDir::BACKWARD, {}}, {PathExplorationDir::BOTH, {}}},
        {{PathExplorationDir::BOTH, {}}, {PathExplorationDir::FORWARD, {}}, {PathExplorationDir::BACKWARD, {}}},
    };

    for (const std::vector<BodyStep>& steps : bodies) {
        expectReference(view, steps, 1, 2);
        expectReference(view, steps, 0, unbounded);
    }
}

TEST_F(PathExploratorBodyTest, filtersEachStepByItsOwnType) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<std::vector<BodyStep>> bodies {
        {{PathExplorationDir::FORWARD, _hubGraph._typeA}, {PathExplorationDir::FORWARD, _hubGraph._typeB}},
        {{PathExplorationDir::FORWARD, _hubGraph._typeA}, {PathExplorationDir::BOTH, {}}},
        {{PathExplorationDir::BOTH, {}}, {PathExplorationDir::FORWARD, _hubGraph._typeB}},
    };

    for (const std::vector<BodyStep>& steps : bodies) {
        expectReference(view, steps, 1, 3);
        expectReference(view, steps, 0, unbounded);
    }
}

TEST_F(PathExploratorBodyTest, aHopFilterReadsTheRepetitionItIsIn) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<BodyStep> steps {{PathExplorationDir::FORWARD, {}}, {PathExplorationDir::BOTH, {}}};

    BodyReference reference(*_adjacency, steps, 1, 3);
    reference.setRefusesReturns();
    std::vector<PathRow> expected;
    reference.enumerate(*_seeds, expected);
    EXPECT_FALSE(expected.empty());

    NoReturnHopFilter filter;
    std::vector<PathRow> actual;
    explore(view, steps, 1, 3, BodyOptions {._secondStepFilter = &filter}, actual);

    expectSameRows(expected, actual);
}

TEST_F(PathExploratorBodyTest, prunesByAnIndexOverEveryStepTogether) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<BodyStep> steps {{PathExplorationDir::FORWARD, {}}, {PathExplorationDir::BACKWARD, {}}};
    const LabelSet endLabels = LabelSet::fromList({_hubGraph._labelT});

    for (const auto& [minRepetitions, maxRepetitions] : std::vector<std::pair<uint64_t, uint64_t>> {{1, 2}, {0, 3}, {1, unbounded}}) {
        BodyReference reference(*_adjacency, steps, minRepetitions, maxRepetitions);
        reference.setEnds(&_hubGraph._ends);
        std::vector<PathRow> expected;
        reference.enumerate(*_seeds, expected);
        EXPECT_FALSE(expected.empty());

        const uint64_t maxHops = edgeBound(maxRepetitions, steps.size());
        PathDistanceIndex index;
        index.build(view, endLabels, PathExplorationDir::BOTH, {}, maxHops);

        std::vector<PathRow> actual;
        explore(view, steps, minRepetitions, maxRepetitions, BodyOptions {._endLabels = &endLabels, ._distanceIndex = &index}, actual);
        expectSameRows(expected, actual);
    }
}

TEST_F(PathExploratorBodyTest, emitsEachEndOnceWhenDistinct) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<BodyStep> forward {{PathExplorationDir::FORWARD, {}}, {PathExplorationDir::FORWARD, {}}};
    const std::vector<BodyStep> mixed {{PathExplorationDir::FORWARD, {}}, {PathExplorationDir::BOTH, {}}};

    expectDistinctReference(view, forward, 1, 2, false);
    expectDistinctReference(view, forward, 0, unbounded, false);
    expectDistinctReference(view, mixed, 1, unbounded, false);
    expectDistinctReference(view, mixed, 2, unbounded, false);
    expectDistinctReference(view, mixed, 1, unbounded, true);
}

TEST_F(PathExploratorBodyTest, searchesLevelsWhenNoEdgeFitsTwoSteps) {
    useBraid();
    const FrozenCommitTx transaction = _braidGraph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const EdgeTypeID typeA = _braid._typeA;
    const EdgeTypeID typeB = _braid._typeB;

    const std::vector<std::vector<BodyStep>> bodies {
        {{PathExplorationDir::FORWARD, typeA}, {PathExplorationDir::FORWARD, typeB}},
        {{PathExplorationDir::BACKWARD, typeA}, {PathExplorationDir::BACKWARD, typeB}},
        {{PathExplorationDir::FORWARD, typeB}, {PathExplorationDir::BACKWARD, typeA}},
    };

    for (const std::vector<BodyStep>& steps : bodies) {
        expectDistinctReference(view, steps, 0, unbounded, false, true);
        expectDistinctReference(view, steps, 1, 2, false, true);
        expectDistinctReference(view, steps, 1, unbounded, false, true);
        expectDistinctReference(view, steps, 2, unbounded, false, false);
    }
}

// Two A hops from 6 reach 8, and two more would reach 7 only over the edge 6 -> 7 again
TEST_F(PathExploratorBodyTest, walksWhenAnEdgeFitsTwoSteps) {
    useBraid();
    const FrozenCommitTx transaction = _braidGraph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const EdgeTypeID typeA = _braid._typeA;
    const EdgeTypeID typeB = _braid._typeB;

    const std::vector<std::vector<BodyStep>> bodies {
        {{PathExplorationDir::FORWARD, typeA}, {PathExplorationDir::FORWARD, typeA}},
        {{PathExplorationDir::FORWARD, typeA}, {PathExplorationDir::BACKWARD, typeA}},
        {{PathExplorationDir::FORWARD, typeA}, {PathExplorationDir::BOTH, typeB}},
        {{PathExplorationDir::FORWARD, typeA}, {PathExplorationDir::FORWARD, {}}},
    };

    for (const std::vector<BodyStep>& steps : bodies) {
        expectDistinctReference(view, steps, 1, unbounded, false, false);
    }
}

TEST_F(PathExploratorBodyTest, readsOneHopOfEachRepetition) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<BodyStep> steps {{PathExplorationDir::FORWARD, {}}, {PathExplorationDir::BOTH, {}}};
    const size_t stepCount = steps.size();

    ColumnVector<size_t> indices;
    ColumnNodeIDs targets;
    ColumnVector<PathRef> paths;
    PathTrie trie;
    ListBuffer<> buffer;

    PathExplorator explorator(view, &_input, 0, 3 * stepCount);

    explorator.addStep(PathExplorationDir::FORWARD);
    explorator.addStep(PathExplorationDir::BOTH);
    explorator.setIndices(&indices);
    explorator.setTargets(&targets);
    explorator.setPaths(&paths, &trie);

    size_t checkedPaths = 0;
    std::vector<uint64_t> edges;
    std::vector<uint64_t> nodes;
    std::vector<uint64_t> read;
    std::vector<uint64_t> expected;

    while (explorator.isValid()) {
        explorator.fill(ChunkConfig::CHUNK_SIZE);

        for (size_t row = 0; row < indices.size(); row++) {
            const NodeID seed = _input[indices[row]];
            readIDs(trie.expandEdges(paths[row], buffer, false), edges);
            readIDs(trie.expandNodes(paths[row], seed, buffer, false), nodes);

            const size_t repetitions = edges.size() / stepCount;
            ASSERT_EQ(edges.size() % stepCount, 0);

            for (size_t offset = 0; offset < stepCount; offset++) {
                const PathHopStride hops {._offset = offset, ._stride = stepCount};

                for (const bool reversed : {false, true}) {
                    const auto expectEvery = [&](const std::vector<uint64_t>& all, size_t shift) {
                        expected.clear();
                        for (size_t repetition = 0; repetition < repetitions; repetition++) {
                            expected.push_back(all[offset + shift + repetition * stepCount]);
                        }

                        if (reversed) {
                            std::reverse(expected.begin(), expected.end());
                        }
                    };

                    readIDs(trie.expandEdges(paths[row], buffer, reversed, hops), read);
                    expectEvery(edges, 0);
                    EXPECT_EQ(read, expected);

                    readIDs(trie.expandSources(paths[row], seed, buffer, reversed, hops), read);
                    expectEvery(nodes, 0);
                    EXPECT_EQ(read, expected);

                    readIDs(trie.expandEnds(paths[row], buffer, reversed, hops), read);
                    expectEvery(nodes, 1);
                    EXPECT_EQ(read, expected);
                }
            }

            checkedPaths += repetitions > 1 ? 1 : 0;
        }
    }

    EXPECT_GT(checkedPaths, 0);
}
