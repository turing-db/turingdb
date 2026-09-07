#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
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
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr uint64_t unbounded = std::numeric_limits<uint64_t>::max();

struct ReferenceEdge {
    uint64_t _edge {0};
    uint64_t _other {0};
    uint64_t _type {0};
};

// The graph as the single-hop writers see it, the ground truth the explorator is compared to
struct Adjacency {
    std::vector<std::vector<ReferenceEdge>> _outs;
    std::vector<std::vector<ReferenceEdge>> _ins;
};

// One emitted row flattened to raw values: the input row, the end node and the path's edges
struct PathRow {
    size_t _index {0};
    uint64_t _target {0};
    std::vector<uint64_t> _edges;

    auto operator<=>(const PathRow&) const = default;
};

using HopPredicate = bool (*)(uint64_t source, uint64_t edge, uint64_t end);

void buildAdjacency(const GraphView& view, size_t nodeCount, Adjacency& adjacency) {
    ColumnNodeIDs input;
    for (size_t node = 0; node < nodeCount; node++) {
        input.push_back(NodeID(node));
    }

    adjacency._outs.assign(nodeCount, {});
    adjacency._ins.assign(nodeCount, {});

    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs others;
    ColumnEdgeTypes types;

    GetOutEdgesChunkWriter outWriter(view, &input);
    outWriter.setIndices(&indices);
    outWriter.setEdgeIDs(&edgeIDs);
    outWriter.setTgtIDs(&others);
    outWriter.setEdgeTypes(&types);

    while (outWriter.isValid()) {
        outWriter.fill(ChunkConfig::CHUNK_SIZE);
        for (size_t row = 0; row < indices.size(); row++) {
            adjacency._outs[indices[row]].push_back({edgeIDs[row].getValue(), others[row].getValue(), types[row].getValue()});
        }
    }

    GetInEdgesChunkWriter inWriter(view, &input);
    inWriter.setIndices(&indices);
    inWriter.setEdgeIDs(&edgeIDs);
    inWriter.setSrcIDs(&others);
    inWriter.setEdgeTypes(&types);

    while (inWriter.isValid()) {
        inWriter.fill(ChunkConfig::CHUNK_SIZE);
        for (size_t row = 0; row < indices.size(); row++) {
            adjacency._ins[indices[row]].push_back({edgeIDs[row].getValue(), others[row].getValue(), types[row].getValue()});
        }
    }
}

uint64_t edgeBetween(const Adjacency& adjacency, uint64_t source, uint64_t target) {
    for (const ReferenceEdge& edge : adjacency._outs[source]) {
        if (edge._other == target) {
            return edge._edge;
        }
    }

    throw std::runtime_error("No such edge in the fixture");
}

// A plain recursive trail enumerator over the adjacency, the oracle of every configuration
class ReferenceEnumerator {
public:
    ReferenceEnumerator(const Adjacency& adjacency,
                        PathExplorationDir direction,
                        uint64_t minHops,
                        uint64_t maxHops)
        : _adjacency(adjacency),
        _direction(direction),
        _minHops(minHops),
        _maxHops(maxHops)
    {
    }

    void setEdgeType(uint64_t type) { _edgeType = type; }
    void setHopPredicate(HopPredicate predicate) { _predicate = predicate; }

    void enumerate(const ColumnNodeIDs& seeds, std::vector<PathRow>& rows) {
        rows.clear();
        std::vector<uint64_t> path;
        for (size_t row = 0; row < seeds.size(); row++) {
            walk(row, seeds[row].getValue(), path, rows);
        }
    }

private:
    const Adjacency& _adjacency;
    PathExplorationDir _direction {PathExplorationDir::FORWARD};
    uint64_t _minHops {0};
    uint64_t _maxHops {0};
    std::optional<uint64_t> _edgeType;
    HopPredicate _predicate {nullptr};

    void walk(size_t seedRow, uint64_t node, std::vector<uint64_t>& path, std::vector<PathRow>& rows) {
        const uint64_t depth = path.size();
        if (depth >= _minHops) {
            rows.push_back({seedRow, node, path});
        }

        if (depth >= _maxHops) {
            return;
        }

        if (_direction != PathExplorationDir::BACKWARD) {
            descend(seedRow, node, _adjacency._outs[node], path, rows);
        }

        if (_direction != PathExplorationDir::FORWARD) {
            descend(seedRow, node, _adjacency._ins[node], path, rows);
        }
    }

    void descend(size_t seedRow,
                 uint64_t node,
                 const std::vector<ReferenceEdge>& candidates,
                 std::vector<uint64_t>& path,
                 std::vector<PathRow>& rows) {
        for (const ReferenceEdge& candidate : candidates) {
            const bool wrongType = _edgeType && candidate._type != *_edgeType;
            const bool onTrail = std::find(path.begin(), path.end(), candidate._edge) != path.end();
            const bool rejected = _predicate && !_predicate(node, candidate._edge, candidate._other);
            if (wrongType || onTrail || rejected) {
                continue;
            }

            path.push_back(candidate._edge);
            walk(seedRow, candidate._other, path, rows);
            path.pop_back();
        }
    }
};

// Applies a hop predicate to a frame the way the query engine will: compacting the spans
class PredicateHopFilter : public PathHopFilter {
public:
    explicit PredicateHopFilter(HopPredicate predicate)
        : _predicate(predicate)
    {
    }

    size_t filter(NodeID source, std::span<NodeID> nodes, std::span<EdgeID> edges) override {
        size_t kept = 0;
        for (size_t candidate = 0; candidate < edges.size(); candidate++) {
            if (_predicate(source.getValue(), edges[candidate].getValue(), nodes[candidate].getValue())) {
                nodes[kept] = nodes[candidate];
                edges[kept] = edges[candidate];
                kept++;
            }
        }

        return kept;
    }

private:
    HopPredicate _predicate {nullptr};
};

bool evenEdgesOnly(uint64_t, uint64_t edge, uint64_t) {
    return edge % 2 == 0;
}

bool nothingPasses(uint64_t, uint64_t, uint64_t) {
    return false;
}

struct ExplorationOptions {
    size_t _maxCount {ChunkConfig::CHUNK_SIZE};
    size_t _walkerCount {1};
    size_t _lookahead {1};
    std::optional<EdgeTypeID> _edgeType;
    PathHopFilter* _hopFilter {nullptr};
    bool _collectTargets {true};
    bool _collectPaths {true};
};

// Drives the explorator to exhaustion, expanding every emitted path through the trie. An
// edge appearing twice on one path fails here, whatever the configuration.
void collectPaths(const GraphView& view,
                  const ColumnNodeIDs& input,
                  PathExplorationDir direction,
                  uint64_t minHops,
                  uint64_t maxHops,
                  const ExplorationOptions& options,
                  std::vector<PathRow>& rows) {
    ColumnVector<size_t> indices;
    ColumnNodeIDs targets;
    ColumnVector<PathRef> paths;
    PathTrie trie;
    ListBuffer<> buffer;

    PathExplorator explorator(view, &input, direction, minHops, maxHops);
    explorator.setIndices(&indices);
    if (options._collectTargets) {
        explorator.setTargets(&targets);
    }
    if (options._collectPaths) {
        explorator.setPaths(&paths, &trie);
    }
    if (options._edgeType) {
        explorator.setEdgeTypeFilter(*options._edgeType);
    }
    explorator.setHopFilter(options._hopFilter);
    explorator.setWalkerCount(options._walkerCount);
    explorator.setCandidateLookahead(options._lookahead);

    rows.clear();
    while (explorator.isValid()) {
        explorator.fill(options._maxCount);

        for (size_t row = 0; row < indices.size(); row++) {
            PathRow& emitted = rows.emplace_back();
            emitted._index = indices[row];
            emitted._target = options._collectTargets ? targets[row].getValue() : 0;

            if (options._collectPaths) {
                const ListView edges = trie.expandEdges(paths[row], buffer);
                for (const ListElementView& element : edges) {
                    emitted._edges.push_back(element.getAs<EdgeID>().getValue());
                }

                std::vector<uint64_t> sorted = emitted._edges;
                std::sort(sorted.begin(), sorted.end());
                EXPECT_EQ(std::adjacent_find(sorted.begin(), sorted.end()), sorted.end())
                    << "An edge appears twice on one path";
            }
        }
    }
}

void expectSameRows(std::vector<PathRow> expected, std::vector<PathRow> actual) {
    std::sort(expected.begin(), expected.end());
    std::sort(actual.begin(), actual.end());
    EXPECT_EQ(expected, actual);
}

size_t countRowsThrough(const std::vector<PathRow>& rows, uint64_t edge) {
    size_t count = 0;
    for (const PathRow& row : rows) {
        if (std::find(row._edges.begin(), row._edges.end(), edge) != row._edges.end()) {
            count++;
        }
    }

    return count;
}

}

// A graph submitted in two commits, so the second commit's edges among first-commit nodes
// are patch edges: 0->1->2->0 is a 3-cycle, 1->0 closes a 2-cycle with 0->1, 2 carries a
// self-loop, 3->4->5 is a chain, and the second commit adds node 6 with 5->6, 6->0 and the
// patch edge 3->0 between two existing nodes. Every node carries the same label, so within
// each part the temporary IDs are the final ones.
class PathExploratorTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = 7;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _typeA = metadata.getOrCreateEdgeType("A");
            _typeB = metadata.getOrCreateEdgeType("B");

            for (size_t node = 0; node < 6; node++) {
                builder.addNode(labelset);
            }

            builder.addEdge(_typeA, 0, 1);
            builder.addEdge(_typeA, 1, 2);
            builder.addEdge(_typeA, 2, 0);
            builder.addEdge(_typeB, 1, 0);
            builder.addEdge(_typeA, 2, 2);
            builder.addEdge(_typeB, 3, 4);
            builder.addEdge(_typeA, 4, 5);

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            const NodeID six = builder.addNode(labelset);
            ASSERT_EQ(six.getValue(), 6u);

            builder.addEdge(_typeA, 5, six);
            builder.addEdge(_typeB, six, 0);
            builder.addEdge(_typeA, 3, 0);

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void allNodes(ColumnNodeIDs& input) const {
        input.clear();
        for (size_t node = 0; node < nodeCount; node++) {
            input.push_back(NodeID(node));
        }
    }

    // Deletes one edge in a further commit, so the view carries an edge tombstone
    void deleteEdge(uint64_t edge) {
        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        commitBuilder->writeBuffer().addDeletedEdge(EdgeID(edge));

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    EdgeTypeID _typeA;
    EdgeTypeID _typeB;
};

TEST_F(PathExploratorTest, matchesTheReferenceEnumerationInEveryConfiguration) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    const std::vector<std::pair<uint64_t, uint64_t>> bounds {
        {1, 1}, {0, 2}, {2, 3}, {1, unbounded}, {0, unbounded},
    };

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : bounds) {
            ReferenceEnumerator reference(_adjacency, direction, minHops, maxHops);
            std::vector<PathRow> expected;
            reference.enumerate(input, expected);
            ASSERT_FALSE(expected.empty());

            for (const size_t maxCount : {size_t {1}, size_t {2}, ChunkConfig::CHUNK_SIZE}) {
                for (const size_t walkerCount : {size_t {1}, size_t {8}}) {
                    for (const size_t lookahead : {size_t {0}, size_t {1}}) {
                        ExplorationOptions options;
                        options._maxCount = maxCount;
                        options._walkerCount = walkerCount;
                        options._lookahead = lookahead;

                        std::vector<PathRow> actual;
                        collectPaths(view, input, direction, minHops, maxHops, options, actual);
                        expectSameRows(expected, actual);
                    }
                }
            }
        }
    }
}

TEST_F(PathExploratorTest, typeFilterMatchesTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    for (const EdgeTypeID edgeType : {_typeA, _typeB}) {
        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BOTH}) {
            ReferenceEnumerator reference(_adjacency, direction, 0, unbounded);
            reference.setEdgeType(edgeType.getValue());
            std::vector<PathRow> expected;
            reference.enumerate(input, expected);

            ExplorationOptions options;
            options._edgeType = edgeType;
            options._maxCount = 2;

            std::vector<PathRow> actual;
            collectPaths(view, input, direction, 0, unbounded, options, actual);
            expectSameRows(expected, actual);

            // The zero-length rows do not depend on the type: one per seed
            size_t zeroLength = 0;
            for (const PathRow& row : actual) {
                zeroLength += row._edges.empty() ? 1 : 0;
            }
            EXPECT_EQ(zeroLength, input.size());
        }
    }
}

TEST_F(PathExploratorTest, maxHopsZeroNeverExpands) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input {0, 3, 0};

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::FORWARD, 0, 0, ExplorationOptions {}, rows);
    expectSameRows({{0, 0, {}}, {1, 3, {}}, {2, 0, {}}}, rows);

    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 0, ExplorationOptions {}, rows);
    EXPECT_TRUE(rows.empty());
}

TEST_F(PathExploratorTest, nullTargetsAndPathsAreSkipped) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    ReferenceEnumerator reference(_adjacency, PathExplorationDir::FORWARD, 1, 3);
    std::vector<PathRow> expected;
    reference.enumerate(input, expected);

    ExplorationOptions options;
    options._collectTargets = false;
    options._collectPaths = false;

    std::vector<PathRow> actual;
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, 3, options, actual);
    EXPECT_EQ(actual.size(), expected.size());

    std::vector<size_t> expectedIndices;
    for (const PathRow& row : expected) {
        expectedIndices.push_back(row._index);
    }
    std::vector<size_t> actualIndices;
    for (const PathRow& row : actual) {
        actualIndices.push_back(row._index);
    }
    std::sort(expectedIndices.begin(), expectedIndices.end());
    std::sort(actualIndices.begin(), actualIndices.end());
    EXPECT_EQ(expectedIndices, actualIndices);
}

TEST_F(PathExploratorTest, hopFilterMatchesTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    ReferenceEnumerator reference(_adjacency, PathExplorationDir::BOTH, 0, unbounded);
    reference.setHopPredicate(&evenEdgesOnly);
    std::vector<PathRow> expected;
    reference.enumerate(input, expected);

    PredicateHopFilter filter(&evenEdgesOnly);
    ExplorationOptions options;
    options._hopFilter = &filter;
    options._maxCount = 1;

    std::vector<PathRow> actual;
    collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, options, actual);
    expectSameRows(expected, actual);
}

TEST_F(PathExploratorTest, hopFilterRejectingEveryFrameLeavesTheZeroLengthRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    PredicateHopFilter filter(&nothingPasses);
    ExplorationOptions options;
    options._hopFilter = &filter;

    std::vector<PathRow> rows;
    collectPaths(view, input, PathExplorationDir::BOTH, 0, unbounded, options, rows);
    EXPECT_EQ(rows.size(), input.size());
    for (const PathRow& row : rows) {
        EXPECT_TRUE(row._edges.empty());
        EXPECT_EQ(row._target, input[row._index].getValue());
    }

    collectPaths(view, input, PathExplorationDir::BOTH, 1, unbounded, options, rows);
    EXPECT_TRUE(rows.empty());
}

TEST_F(PathExploratorTest, walksPatchEdgesOfEarlierNodes) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // 3->4 is a first-commit edge, 3->0 a second-commit patch edge of node 3
    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {3}, PathExplorationDir::FORWARD, 1, 1, ExplorationOptions {}, rows);
    expectSameRows({{0, 4, {edgeBetween(_adjacency, 3, 4)}}, {0, 0, {edgeBetween(_adjacency, 3, 0)}}}, rows);

    // 5->6 is a patch out-edge of node 5 towards the second commit's node
    collectPaths(view, ColumnNodeIDs {5}, PathExplorationDir::FORWARD, 1, 1, ExplorationOptions {}, rows);
    expectSameRows({{0, 6, {edgeBetween(_adjacency, 5, 6)}}}, rows);

    // Node 0's in-edges: two first-commit ones and two patched in by the second commit
    collectPaths(view, ColumnNodeIDs {0}, PathExplorationDir::BACKWARD, 1, 1, ExplorationOptions {}, rows);
    expectSameRows({{0, 2, {edgeBetween(_adjacency, 2, 0)}},
                    {0, 1, {edgeBetween(_adjacency, 1, 0)}},
                    {0, 6, {edgeBetween(_adjacency, 6, 0)}},
                    {0, 3, {edgeBetween(_adjacency, 3, 0)}}},
                   rows);
}

TEST_F(PathExploratorTest, twoCycleYieldsEachDirectionButNeverAnEdgeTwice) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const uint64_t zeroToOne = edgeBetween(_adjacency, 0, 1);
    const uint64_t oneToZero = edgeBetween(_adjacency, 1, 0);

    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {0}, PathExplorationDir::FORWARD, 1, unbounded, ExplorationOptions {}, rows);

    const PathRow there {0, 1, {zeroToOne}};
    const PathRow andBack {0, 0, {zeroToOne, oneToZero}};
    const PathRow again {0, 1, {zeroToOne, oneToZero, zeroToOne}};
    EXPECT_NE(std::find(rows.begin(), rows.end(), there), rows.end());
    EXPECT_NE(std::find(rows.begin(), rows.end(), andBack), rows.end());
    EXPECT_EQ(std::find(rows.begin(), rows.end(), again), rows.end());
}

TEST_F(PathExploratorTest, selfLoopIsEmittedTwiceInBothDirections) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const uint64_t loop = edgeBetween(_adjacency, 2, 2);

    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {2}, PathExplorationDir::BOTH, 1, 1, ExplorationOptions {}, rows);
    EXPECT_EQ(countRowsThrough(rows, loop), 2u);

    collectPaths(view, ColumnNodeIDs {2}, PathExplorationDir::FORWARD, 1, 1, ExplorationOptions {}, rows);
    EXPECT_EQ(countRowsThrough(rows, loop), 1u);
}

TEST_F(PathExploratorTest, tombstonedEdgesAreNeitherEmittedNorExpanded) {
    const uint64_t deleted = edgeBetween(_adjacency, 4, 5);
    deleteEdge(deleted);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    ASSERT_TRUE(view.tombstones().hasEdges());

    Adjacency deletedAdjacency;
    buildAdjacency(view, nodeCount, deletedAdjacency);

    ColumnNodeIDs input;
    allNodes(input);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        ReferenceEnumerator reference(deletedAdjacency, direction, 0, unbounded);
        std::vector<PathRow> expected;
        reference.enumerate(input, expected);

        std::vector<PathRow> actual;
        collectPaths(view, input, direction, 0, unbounded, ExplorationOptions {}, actual);
        expectSameRows(expected, actual);
        EXPECT_EQ(countRowsThrough(actual, deleted), 0u);
    }

    // Node 4's only out-edge is the deleted one, so nothing leaves it
    std::vector<PathRow> rows;
    collectPaths(view, ColumnNodeIDs {4}, PathExplorationDir::FORWARD, 1, unbounded, ExplorationOptions {}, rows);
    EXPECT_TRUE(rows.empty());

    // And 3->4->5->6 no longer reaches past 4
    collectPaths(view, ColumnNodeIDs {3}, PathExplorationDir::FORWARD, 2, unbounded, ExplorationOptions {}, rows);
    for (const PathRow& row : rows) {
        EXPECT_NE(row._target, 5u);
        EXPECT_NE(row._target, 6u);
    }
}

TEST_F(PathExploratorTest, emptyInputYieldsNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input;
    ColumnVector<size_t> indices;

    PathExplorator explorator(view, &input, PathExplorationDir::BOTH, 0, unbounded);
    explorator.setIndices(&indices);
    EXPECT_FALSE(explorator.isValid());

    explorator.fill(ChunkConfig::CHUNK_SIZE);
    EXPECT_TRUE(indices.empty());
    EXPECT_FALSE(explorator.isValid());
}

TEST_F(PathExploratorTest, resetRestartsFromTheFirstSeed) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input {0, 1};
    ColumnVector<size_t> indices;

    PathExplorator explorator(view, &input, PathExplorationDir::FORWARD, 1, 2);
    explorator.setIndices(&indices);

    size_t firstRun = 0;
    while (explorator.isValid()) {
        explorator.fill(1);
        firstRun += indices.size();
    }

    explorator.reset();
    EXPECT_TRUE(explorator.isValid());

    size_t secondRun = 0;
    while (explorator.isValid()) {
        explorator.fill(ChunkConfig::CHUNK_SIZE);
        secondRun += indices.size();
    }

    EXPECT_GT(firstRun, 0u);
    EXPECT_EQ(firstRun, secondRun);
}

// A chain longer than the 64 signature bits: every edge of the single trail from its head
// is on the path together, so some pairs share a bit and only the exact scan tells them apart.
TEST(PathExploratorChainTest, signatureCollisionsAreResolvedByTheExactScan) {
    constexpr size_t chainNodes = 70;

    JobSystem jobSystem;
    jobSystem.init();
    auto graph = Graph::create();

    {
        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const EdgeTypeID link = metadata.getOrCreateEdgeType("A");

        for (size_t node = 0; node < chainNodes; node++) {
            builder.addNode(labelset);
        }
        for (size_t node = 0; node + 1 < chainNodes; node++) {
            builder.addEdge(link, node, node + 1);
        }

        const auto submitted = change->access().submit(jobSystem);
        ASSERT_TRUE(submitted);
    }

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // The final node IDs are not the insertion order, so the head is the node nothing enters
    Adjacency adjacency;
    buildAdjacency(view, chainNodes, adjacency);

    ColumnNodeIDs head;
    for (size_t node = 0; node < chainNodes; node++) {
        if (adjacency._ins[node].empty()) {
            head.push_back(NodeID(node));
        }
    }
    ASSERT_EQ(head.size(), 1u);

    for (const size_t walkerCount : {size_t {1}, size_t {8}}) {
        ExplorationOptions options;
        options._walkerCount = walkerCount;

        std::vector<PathRow> rows;
        collectPaths(view, head, PathExplorationDir::FORWARD, 1, unbounded, options, rows);
        ASSERT_EQ(rows.size(), chainNodes - 1);

        std::sort(rows.begin(), rows.end(), [](const PathRow& lhs, const PathRow& rhs) {
            return lhs._edges.size() < rhs._edges.size();
        });
        for (size_t depth = 1; depth <= rows.size(); depth++) {
            EXPECT_EQ(rows[depth - 1]._edges.size(), depth);
        }
    }

    jobSystem.terminate();
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
