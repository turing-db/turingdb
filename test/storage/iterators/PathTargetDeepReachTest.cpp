#include <memory>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
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

// One chain longer than the hop count a distance byte holds, so the walk from its head to its
// tail is the case an index that stopped at that byte could not answer.
class PathTargetDeepReachTest : public TuringTest {
protected:
    static constexpr size_t chainLength = 300;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        _type = metadata.getOrCreateEdgeType("NEXT");

        for (size_t node = 0; node < chainLength; node++) {
            builder.addNode(plain);
        }

        for (size_t node = 0; node + 1 < chainLength; node++) {
            builder.addEdge(_type, node, node + 1);
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), chainLength, _adjacency);

        readChain();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // The commit may renumber the nodes, so the chain is followed off the adjacency: its head
    // is the one node no edge arrives at
    void readChain() {
        uint64_t node = chainLength;
        for (uint64_t candidate = 0; candidate < chainLength; candidate++) {
            if (_adjacency._ins[candidate].empty()) {
                node = candidate;
                break;
            }
        }
        ASSERT_LT(node, chainLength);

        _chain.clear();
        _chain.push_back(node);
        while (!_adjacency._outs[node].empty()) {
            node = _adjacency._outs[node].front()._other;
            _chain.push_back(node);
        }

        ASSERT_EQ(_chain.size(), chainLength);
    }

    NodeID head() const { return NodeID(_chain.front()); }
    NodeID tail() const { return NodeID(_chain.back()); }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    EdgeTypeID _type {0};
    Adjacency _adjacency;
    std::vector<uint64_t> _chain;
};

TEST_F(PathTargetDeepReachTest, reachesATargetFartherThanADistanceByteHolds) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<NodeID> targets {tail()};

    PathTargetIndex index;
    index.build(view, targets, PathExplorationDir::FORWARD, {&_type, 1}, unbounded);

    const PathTargetHandle handle = index.find(tail());
    ASSERT_TRUE(handle.isValid());

    // Every node of the chain reaches the tail, the head across the 299 hops of the whole of it
    EXPECT_EQ(index.getReachedCount(), chainLength);
    EXPECT_TRUE(handle.canReachWithin(head(), unbounded));
    EXPECT_TRUE(handle.canReachWithin(NodeID(_chain[chainLength - 2]), unbounded));
}

TEST_F(PathTargetDeepReachTest, prunesByTheBoundItWasBuiltFor) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const std::vector<NodeID> targets {tail()};

    PathTargetIndex index;
    index.build(view, targets, PathExplorationDir::FORWARD, {&_type, 1}, 4);

    const PathTargetHandle handle = index.find(tail());
    ASSERT_TRUE(handle.isValid());

    EXPECT_TRUE(handle.canReachWithin(NodeID(_chain[chainLength - 5]), 4));
    EXPECT_FALSE(handle.canReachWithin(NodeID(_chain[chainLength - 6]), 4));
    EXPECT_FALSE(handle.canReachWithin(head(), 4));
}

TEST_F(PathTargetDeepReachTest, walksTheWholeChainWithAndWithoutTheIndex) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const ColumnNodeIDs input {head()};
    const ColumnNodeIDs endNodes {tail()};

    std::vector<bool> ends(chainLength, false);
    ends[tail().getValue()] = true;

    ReferenceEnumerator reference(_adjacency, PathExplorationDir::FORWARD, 1, unbounded);
    reference.setEdgeType(_type.getValue());
    reference.setEnds(&ends);

    std::vector<PathRow> expected;
    reference.enumerate(input, expected);
    ASSERT_EQ(expected.size(), 1);
    ASSERT_EQ(expected.front()._edges.size(), chainLength - 1);

    ExplorationOptions options;
    options._edgeType = _type;
    options._endNodes = &endNodes;

    std::vector<PathRow> actual;
    collectPaths(view, input, PathExplorationDir::FORWARD, 1, unbounded, options, actual);
    expectSameRows(expected, actual);

    const std::vector<NodeID> targets {tail()};
    PathTargetIndex index;
    index.build(view, targets, PathExplorationDir::FORWARD, {&_type, 1}, unbounded);
    options._targetIndex = &index;

    collectPaths(view, input, PathExplorationDir::FORWARD, 1, unbounded, options, actual);
    expectSameRows(expected, actual);
}
