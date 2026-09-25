#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathDistanceIndex.h"
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

void distinctPairs(const std::vector<PathRow>& rows, std::vector<PathRow>& pairs) {
    pairs.clear();
    for (const PathRow& row : rows) {
        pairs.push_back({row._index, row._target, {}});
    }

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
}

void keepRowsEndingOnTheirTarget(std::vector<PathRow>& rows, const ColumnNodeIDs& endNodes) {
    std::erase_if(rows, [&endNodes](const PathRow& row) {
        return row._target != endNodes[row._index].getValue();
    });
}

const std::vector<std::pair<uint64_t, uint64_t>> walkedBounds {
    {2, 2}, {2, 3}, {3, 3}, {4, 4}, {2, 6}, {2, unbounded},
};

}

// A distinct exploration past a minimum of one hop walks trails rather than searching, so
// it takes the pruning indexes the enumeration takes
class PathExploratorDistinctIndexTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = HubGraph::nodeCount;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        buildHubGraph(*_graph, *_jobSystem, _hubGraph);
        _endLabels = LabelSet::fromList({_hubGraph._labelT});
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

    void seedsWithTargets(ColumnNodeIDs& input, ColumnNodeIDs& endNodes, std::vector<NodeID>& targets) const {
        input.clear();
        endNodes.clear();
        for (size_t node = 0; node < nodeCount; node++) {
            input.push_back(NodeID(node));
            endNodes.push_back(NodeID(node));

            input.push_back(NodeID(node));
            endNodes.push_back(NodeID(_hubGraph._target));

            input.push_back(NodeID(node));
            endNodes.push_back(NodeID((node * 7 + 3) % nodeCount));
        }

        targets.assign(endNodes.begin(), endNodes.end());
        std::sort(targets.begin(), targets.end());
        targets.erase(std::unique(targets.begin(), targets.end()), targets.end());
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    HubGraph _hubGraph;
    LabelSet _endLabels;
};

TEST_F(PathExploratorDistinctIndexTest, distinctWalkWithTheDistanceIndexMatchesTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : walkedBounds) {
            ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);
            reference.setEnds(&_hubGraph._ends);

            std::vector<PathRow> rows;
            reference.enumerate(input, rows);

            std::vector<PathRow> expected;
            distinctPairs(rows, expected);

            PathDistanceIndex index;
            index.build(view, _endLabels, direction, {}, maxHops);

            for (const size_t maxCount : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
                ExplorationOptions options;
                options._maxCount = maxCount;
                options._distinctEnds = true;
                options._collectPaths = false;
                options._endLabels = &_endLabels;
                options._distanceIndex = &index;

                std::vector<PathRow> actual;
                collectPaths(view, input, direction, minHops, maxHops, options, actual);
                expectSameRows(expected, actual);
            }
        }
    }
}

TEST_F(PathExploratorDistinctIndexTest, distinctWalkWithTheDistanceIndexPrunesTheDeadEndRegion) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    ExplorationOptions options;
    options._distinctEnds = true;
    options._collectPaths = false;
    options._endLabels = &_endLabels;

    std::vector<PathRow> unpruned;
    const size_t unprunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 2, 3, options, unpruned);

    PathDistanceIndex index;
    index.build(view, _endLabels, PathExplorationDir::FORWARD, {}, 3);
    options._distanceIndex = &index;

    std::vector<PathRow> pruned;
    const size_t prunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, 2, 3, options, pruned);

    expectSameRows(unpruned, pruned);
    EXPECT_FALSE(pruned.empty());
    EXPECT_LT(prunedChecks * 2, unprunedChecks) << prunedChecks << " of " << unprunedChecks;
}

TEST_F(PathExploratorDistinctIndexTest, distinctWalkWithTheTargetIndexMatchesTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    ColumnNodeIDs endNodes;
    std::vector<NodeID> targets;
    seedsWithTargets(input, endNodes, targets);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const auto& [minHops, maxHops] : walkedBounds) {
            ReferenceEnumerator reference(_hubGraph._adjacency, direction, minHops, maxHops);

            std::vector<PathRow> rows;
            reference.enumerate(input, rows);
            keepRowsEndingOnTheirTarget(rows, endNodes);

            std::vector<PathRow> expected;
            distinctPairs(rows, expected);

            PathTargetIndex index;
            index.build(view, targets, direction, {}, maxHops);

            ExplorationOptions options;
            options._distinctEnds = true;
            options._collectPaths = false;
            options._endNodes = &endNodes;
            options._targetIndex = &index;

            std::vector<PathRow> actual;
            collectPaths(view, input, direction, minHops, maxHops, options, actual);
            expectSameRows(expected, actual);
        }
    }
}

// A complete graph whose one way out is a chain to the only T node: whether a cluster node
// can still reach T depends on the hops left, so the index rules nodes out deep in the walk,
// behind edges the trail already holds
class PathExploratorDistinctIndexClusterTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void build(size_t clusterSize, size_t chainLength) {
        _graph = Graph::create();
        _cluster.clear();

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            _labelT = metadata.getOrCreateLabel("T");
            _type = metadata.getOrCreateEdgeType("A");

            for (size_t node = 0; node < clusterSize + chainLength; node++) {
                builder.addNode(plain);
            }
            builder.addNode(LabelSet::fromList({_labelT}));

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        const size_t nodeCount = clusterSize + chainLength + 1;
        std::vector<NodeID> plainNodes;
        NodeID target;
        {
            const FrozenCommitTx transaction = _graph->openTransaction();
            const GraphReader reader = transaction.readGraph();
            for (size_t node = 0; node < nodeCount; node++) {
                if (reader.getNodeLabelSet(NodeID(node)).hasLabel(_labelT)) {
                    target = NodeID(node);
                } else {
                    plainNodes.push_back(NodeID(node));
                }
            }
        }

        _cluster.assign(plainNodes.begin(), plainNodes.begin() + clusterSize);
        const std::vector<NodeID> chain(plainNodes.begin() + clusterSize, plainNodes.end());

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();

            for (const NodeID source : _cluster) {
                for (const NodeID other : _cluster) {
                    if (source != other) {
                        builder.addEdge(_type, source, other);
                    }
                }
            }

            NodeID previous = _cluster.front();
            for (const NodeID link : chain) {
                builder.addEdge(_type, previous, link);
                previous = link;
            }
            builder.addEdge(_type, previous, target);
            _target = target;

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    LabelID _labelT;
    EdgeTypeID _type;
    std::vector<NodeID> _cluster;
    NodeID _target;
};

TEST_F(PathExploratorDistinctIndexClusterTest, distanceIndexNeverMakesTheDistinctWalkLonger) {
    for (const auto& [clusterSize, chainLength] : std::vector<std::pair<size_t, size_t>> {{5, 2}, {6, 3}, {7, 3}}) {
        build(clusterSize, chainLength);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        for (const NodeID node : _cluster) {
            input.push_back(node);
        }

        const LabelSet endLabels = LabelSet::fromList({_labelT});

        for (uint64_t hops = chainLength + 2; hops <= chainLength + 8; hops++) {
            SCOPED_TRACE("cluster " + std::to_string(clusterSize) + " chain " + std::to_string(chainLength)
                         + " hops " + std::to_string(hops));

            ExplorationOptions options;
            options._distinctEnds = true;
            options._collectPaths = false;
            options._endLabels = &endLabels;

            std::vector<PathRow> unpruned;
            const size_t unprunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, hops, hops, options, unpruned);

            PathDistanceIndex index;
            index.build(view, endLabels, PathExplorationDir::FORWARD, {}, hops);
            options._distanceIndex = &index;

            std::vector<PathRow> pruned;
            const size_t prunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, hops, hops, options, pruned);

            expectSameRows(unpruned, pruned);
            EXPECT_FALSE(pruned.empty());
            EXPECT_LE(prunedChecks, unprunedChecks);
        }
    }
}

TEST_F(PathExploratorDistinctIndexClusterTest, targetIndexNeverMakesTheDistinctWalkLonger) {
    for (const auto& [clusterSize, chainLength] : std::vector<std::pair<size_t, size_t>> {{5, 2}, {6, 3}, {7, 3}}) {
        build(clusterSize, chainLength);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs input;
        ColumnNodeIDs endNodes;
        for (const NodeID node : _cluster) {
            input.push_back(node);
            endNodes.push_back(_target);
        }

        const std::vector<NodeID> targets {_target};

        for (uint64_t hops = chainLength + 2; hops <= chainLength + 8; hops++) {
            SCOPED_TRACE("cluster " + std::to_string(clusterSize) + " chain " + std::to_string(chainLength)
                         + " hops " + std::to_string(hops));

            ExplorationOptions options;
            options._distinctEnds = true;
            options._collectPaths = false;
            options._endNodes = &endNodes;

            std::vector<PathRow> unpruned;
            const size_t unprunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, hops, hops, options, unpruned);

            PathTargetIndex index;
            index.build(view, targets, PathExplorationDir::FORWARD, {}, hops);
            options._targetIndex = &index;

            std::vector<PathRow> pruned;
            const size_t prunedChecks = collectPaths(view, input, PathExplorationDir::FORWARD, hops, hops, options, pruned);

            expectSameRows(unpruned, pruned);
            EXPECT_FALSE(pruned.empty());
            EXPECT_LE(prunedChecks, unprunedChecks);
        }
    }
}
