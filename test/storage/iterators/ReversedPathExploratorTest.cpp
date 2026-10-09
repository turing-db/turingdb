#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathTargetIndex.h"
#include "iterators/ReversedPathExplorator.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "PathExplorationReference.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using EndRow = std::pair<size_t, uint64_t>;

struct ReversedOptions {
    size_t _maxCount {ChunkConfig::CHUNK_SIZE};
    std::optional<EdgeTypeID> _edgeType;
    const LabelSet* _endLabels {nullptr};
    bool _distinctEnds {false};
    bool _indexesSeeds {false};
};

}

// The walk from an end set back to the seeds emits the rows the walk from the seeds emits:
// each (input row, end node) once per trail, or once in the distinct mode
class ReversedPathExploratorTest : public TuringTest {
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

    // Every node as a seed, the even ones on two rows
    static void seedsOfEveryNode(ColumnNodeIDs& input) {
        input.clear();
        for (size_t node = 0; node < nodeCount; node++) {
            input.push_back(NodeID(node));
            if (node % 2 == 0) {
                input.push_back(NodeID(node));
            }
        }
    }

    void expectedRows(const ColumnNodeIDs& input,
                      std::span<const NodeID> endNodeSet,
                      PathExplorationDir direction,
                      uint64_t minHops,
                      uint64_t maxHops,
                      const ReversedOptions& options,
                      const GraphView& view,
                      std::vector<EndRow>& rows) const {
        std::vector<bool> ends(nodeCount, false);
        const GraphReader reader = view.read();
        for (const NodeID end : endNodeSet) {
            const bool labelled = !options._endLabels || reader.getNodeLabelSet(end).hasAtLeastLabels(LabelSetHandle(*options._endLabels));
            ends[end.getValue()] = labelled;
        }

        ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);
        if (options._edgeType) {
            reference.setEdgeType(options._edgeType->getValue());
        }
        reference.setEnds(&ends);

        std::vector<PathRow> paths;
        reference.enumerate(input, paths);

        rows.clear();
        for (const PathRow& path : paths) {
            rows.emplace_back(path._index, path._target);
        }

        std::sort(rows.begin(), rows.end());
        if (options._distinctEnds) {
            rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
        }
    }

    static void reversedRows(const GraphView& view,
                             const ColumnNodeIDs& input,
                             std::span<const NodeID> endNodeSet,
                             PathExplorationDir direction,
                             uint64_t minHops,
                             uint64_t maxHops,
                             const ReversedOptions& options,
                             std::vector<EndRow>& rows) {
        ColumnVector<size_t> indices;
        ColumnNodeIDs targets;

        ReversedPathExplorator explorator(view, &input, endNodeSet, options._endLabels, direction, minHops, maxHops);
        explorator.setIndices(&indices);
        explorator.setTargets(&targets);
        explorator.setDistinctEnds(options._distinctEnds);

        std::span<const EdgeTypeID> edgeTypes;
        if (options._edgeType) {
            edgeTypes = std::span<const EdgeTypeID> {&*options._edgeType, 1};
            explorator.setEdgeTypeFilter(edgeTypes);
        }

        PathTargetIndex index;
        if (options._indexesSeeds) {
            index.buildSet(view, explorator.getSeedNodes(), reverseOf(direction), edgeTypes, maxHops);
            explorator.setTargetIndex(&index);
        }

        rows.clear();
        while (explorator.isValid()) {
            explorator.fill(options._maxCount);
            ASSERT_LE(indices.size(), options._maxCount);
            ASSERT_EQ(targets.size(), indices.size());

            for (size_t row = 0; row < indices.size(); row++) {
                rows.emplace_back(indices[row], targets[row].getValue());
            }
        }

        std::sort(rows.begin(), rows.end());
    }

    void expectSameRowsAsTheReference(const GraphView& view, std::span<const NodeID> endNodeSet, const ReversedOptions& options) {
        ColumnNodeIDs input;
        seedsOfEveryNode(input);

        const std::vector<std::pair<uint64_t, uint64_t>> bounds {
            {1, 1}, {1, 3}, {0, 2}, {2, unbounded}, {0, unbounded},
        };

        size_t comparedRows = 0;
        for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
            for (const auto& [minHops, maxHops] : bounds) {
                std::vector<EndRow> expected;
                expectedRows(input, endNodeSet, direction, minHops, maxHops, options, view, expected);

                std::vector<EndRow> actual;
                reversedRows(view, input, endNodeSet, direction, minHops, maxHops, options, actual);

                EXPECT_EQ(actual, expected) << "direction " << static_cast<int>(direction) << " hops " << minHops << " to " << maxHops;
                comparedRows += expected.size();
            }
        }

        EXPECT_GT(comparedRows, 0u);
    }

    // A node of each kind the hub's dead branch is made of: the branch, a cluster member, a leaf
    void deadBranch(uint64_t& dead, uint64_t& member, uint64_t& leaf) const {
        const Adjacency& adjacency = _hubGraph._adjacency;
        for (const ReferenceEdge& edge : adjacency._outs[_hubGraph._hub]) {
            if (edge._other != _hubGraph._chainOne) {
                dead = edge._other;
            }
        }

        member = adjacency._outs[dead].front()._other;
        leaf = adjacency._outs[member].front()._other;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
};

TEST_F(ReversedPathExploratorTest, emitsTheRowsOfTheWalkFromTheSeeds) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    std::vector<NodeID> spread;
    for (size_t node = 0; node < nodeCount; node += 5) {
        spread.push_back(NodeID(node));
    }

    std::vector<NodeID> targets {NodeID(_hubGraph._target), NodeID(_hubGraph._secondTarget)};
    std::sort(targets.begin(), targets.end());

    for (const std::vector<NodeID>* endNodeSet : {&spread, &targets}) {
        for (const size_t maxCount : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            ReversedOptions options;
            options._maxCount = maxCount;

            expectSameRowsAsTheReference(view, *endNodeSet, options);
        }
    }
}

TEST_F(ReversedPathExploratorTest, appliesTheEdgeTypeTheEndLabelsAndTheDistinctMode) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    std::vector<NodeID> everyNode;
    for (size_t node = 0; node < nodeCount; node++) {
        everyNode.push_back(NodeID(node));
    }

    const LabelSet endLabels = LabelSet::fromList({_hubGraph._labelT});

    ReversedOptions typed;
    typed._edgeType = _hubGraph._typeA;
    expectSameRowsAsTheReference(view, everyNode, typed);

    ReversedOptions labelled;
    labelled._endLabels = &endLabels;
    expectSameRowsAsTheReference(view, everyNode, labelled);

    ReversedOptions distinct;
    distinct._distinctEnds = true;
    expectSameRowsAsTheReference(view, everyNode, distinct);
}

TEST_F(ReversedPathExploratorTest, anIndexOverTheSeedsKeepsEveryRow) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    std::vector<NodeID> targets {NodeID(_hubGraph._target), NodeID(_hubGraph._secondTarget)};
    std::sort(targets.begin(), targets.end());

    ReversedOptions indexed;
    indexed._indexesSeeds = true;
    expectSameRowsAsTheReference(view, targets, indexed);
}

TEST_F(ReversedPathExploratorTest, walksFromWhicheverSideFansOutLess) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    uint64_t dead = 0;
    uint64_t member = 0;
    uint64_t leaf = 0;
    deadBranch(dead, member, leaf);

    // Out of the hub the walk fans out over the dead branch; into a leaf it is one edge a level
    const std::vector<NodeID> hub {NodeID(_hubGraph._hub)};
    const std::vector<NodeID> leaves {NodeID(leaf)};

    PathDistanceIndex::SeedExpansion fromEnds;
    EXPECT_TRUE(ReversedPathExplorator::isCheaper(parts, PathExplorationDir::FORWARD, {}, hub, leaves, 3, fromEnds));
    EXPECT_FALSE(ReversedPathExplorator::isCheaper(parts, PathExplorationDir::BACKWARD, {}, leaves, hub, 3, fromEnds));
}
