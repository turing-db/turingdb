#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "TuringException.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathReachTable.h"
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

// The (seed row, end) pairs a breadth-first search reports for hops in [min, max], the oracle
// where the trails are too many to enumerate. A seed is its own end at level zero when the
// minimum is zero, and otherwise only once a cycle brings the search back to it.
void reachablePairs(const Adjacency& adjacency,
                    const ColumnNodeIDs& seeds,
                    PathExplorationDir direction,
                    uint64_t minHops,
                    uint64_t maxHops,
                    std::vector<PathRow>& pairs) {
    pairs.clear();

    const size_t nodeCount = adjacency._outs.size();
    std::vector<bool> seen(nodeCount);
    std::vector<uint64_t> frontier;
    std::vector<uint64_t> next;

    const auto relax = [&](size_t row, uint64_t level, const std::vector<ReferenceEdge>& edges) {
        for (const ReferenceEdge& edge : edges) {
            if (seen[edge._other]) {
                continue;
            }

            seen[edge._other] = true;
            next.push_back(edge._other);
            if (level >= minHops) {
                pairs.push_back({row, edge._other, {}});
            }
        }
    };

    for (size_t row = 0; row < seeds.size(); row++) {
        const uint64_t seed = seeds[row].getValue();
        std::fill(seen.begin(), seen.end(), false);
        frontier.assign(1, seed);

        if (minHops == 0) {
            seen[seed] = true;
            pairs.push_back({row, seed, {}});
        }

        for (uint64_t level = 1; level <= maxHops && !frontier.empty(); level++) {
            next.clear();
            for (const uint64_t node : frontier) {
                if (direction != PathExplorationDir::BACKWARD) {
                    relax(row, level, adjacency._outs[node]);
                }
                if (direction != PathExplorationDir::FORWARD) {
                    relax(row, level, adjacency._ins[node]);
                }
            }

            std::swap(frontier, next);
        }
    }

    std::sort(pairs.begin(), pairs.end());
}

}

TEST(PathReachTableTest, growsPastItsFirstCapacityAndFindsEveryNodeAgain) {
    PathReachTable table;

    constexpr size_t reachedCount = 1000;
    for (size_t node = 0; node < reachedCount; node++) {
        PathReachTable::Slot& slot = table.reach(NodeID(node));
        EXPECT_EQ(slot._node, node);
        EXPECT_EQ(slot._seen, 0u);
        slot._seen = node + 1;
    }
    EXPECT_EQ(table.size(), reachedCount);

    for (size_t node = 0; node < reachedCount; node++) {
        EXPECT_EQ(table.get(NodeID(node))._seen, node + 1);
        EXPECT_EQ(table.reach(NodeID(node))._seen, node + 1);
    }
    EXPECT_EQ(table.size(), reachedCount);

    // Far apart keys, so the hashing rather than the key order spreads them
    for (uint64_t node = 1ull << 40; node < (1ull << 40) + 300; node += 7) {
        table.reach(NodeID(node))._gained = node;
    }
    for (uint64_t node = 1ull << 40; node < (1ull << 40) + 300; node += 7) {
        EXPECT_EQ(table.get(NodeID(node))._gained, node);
    }
}

TEST(PathReachTableTest, clearsWhatItReachedAndNothingElse) {
    PathReachTable table;

    for (size_t node = 0; node < 500; node++) {
        table.reach(NodeID(node))._frontier = 1;
    }
    table.clear();
    EXPECT_EQ(table.size(), 0u);
    EXPECT_THROW(table.get(NodeID(3)), TuringException);

    // A cleared node comes back empty, and a table reused across batches keeps no bit
    for (size_t node = 0; node < 500; node += 2) {
        const PathReachTable::Slot& slot = table.reach(NodeID(node));
        EXPECT_EQ(slot._seen, 0u);
        EXPECT_EQ(slot._frontier, 0u);
        EXPECT_EQ(slot._gained, 0u);
    }
    EXPECT_EQ(table.size(), 250u);
    EXPECT_THROW(table.get(NodeID(1)), TuringException);
}

// A random graph whose every batch of seeds reaches far more nodes than the table starts
// with, from more seeds than one batch holds: the table grows within a batch and is reused
// across them
class PathReachTableExplorationTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = 500;
    static constexpr size_t outDegree = 3;
    static constexpr size_t seedCount = 200;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet labels = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const EdgeTypeID type = metadata.getOrCreateEdgeType("A");

        std::vector<NodeID> nodes;
        for (size_t node = 0; node < nodeCount; node++) {
            nodes.push_back(builder.addNode(labels));
        }

        std::mt19937_64 generator(7);
        std::uniform_int_distribution<size_t> target(0, nodeCount - 1);
        for (const NodeID source : nodes) {
            for (size_t edge = 0; edge < outDegree; edge++) {
                builder.addEdge(type, source, nodes[target(generator)]);
            }
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), nodeCount, _adjacency);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void seeds(ColumnNodeIDs& input) const {
        input.clear();
        for (size_t row = 0; row < seedCount; row++) {
            input.push_back(NodeID((row * 7) % nodeCount));
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
};

TEST_F(PathReachTableExplorationTest, matchesTheDeduplicatedEnumerationOverBatchesThatOutgrowTheTable) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    seeds(input);
    ASSERT_GT(input.size(), 3 * PathTargetIndex::targetsPerBatch);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t minHops : {uint64_t {0}, uint64_t {1}}) {
            if (minHops == 1 && direction == PathExplorationDir::BOTH) {
                continue;
            }

            for (const uint64_t maxHops : {uint64_t {2}, uint64_t {3}}) {
                SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " hops " + std::to_string(minHops) + " to " + std::to_string(maxHops));

                ReferenceEnumerator reference(_adjacency, direction, minHops, maxHops);
                std::vector<PathRow> rows;
                reference.enumerate(input, rows);

                std::vector<PathRow> expected;
                distinctPairs(rows, expected);

                ExplorationOptions options;
                options._distinctEnds = true;

                std::vector<PathRow> actual;
                collectPaths(view, input, direction, minHops, maxHops, options, actual);
                expectSameRows(expected, actual);
            }
        }
    }
}

TEST_F(PathReachTableExplorationTest, matchesTheReachableEndsOfAnUnboundedSearch) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    seeds(input);

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t minHops : {uint64_t {0}, uint64_t {1}}) {
            if (minHops == 1 && direction == PathExplorationDir::BOTH) {
                continue;
            }

            for (const size_t maxCount : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
                SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " min " + std::to_string(minHops) + " chunk " + std::to_string(maxCount));

                std::vector<PathRow> expected;
                reachablePairs(_adjacency, input, direction, minHops, unbounded, expected);
                ASSERT_GT(expected.size(), input.size() * (nodeCount / 2));

                ExplorationOptions options;
                options._distinctEnds = true;
                options._maxCount = maxCount;

                std::vector<PathRow> actual;
                collectPaths(view, input, direction, minHops, unbounded, options, actual);
                expectSameRows(expected, actual);
            }
        }
    }
}
