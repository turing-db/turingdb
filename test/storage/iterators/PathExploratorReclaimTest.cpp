#include <memory>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathExplorator.h"
#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
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

// A complete digraph on six nodes: every ordered pair is an edge, so a four-hop walk has
// hundreds of trails per seed and the search backtracks over far more prefixes than it emits
class PathExploratorReclaimTest : public TuringTest {
protected:
    static constexpr size_t nodeCount = 6;

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
            const EdgeTypeID type = metadata.getOrCreateEdgeType("A");

            for (size_t node = 0; node < nodeCount; node++) {
                builder.addNode(labelset);
            }

            for (size_t source = 0; source < nodeCount; source++) {
                for (size_t target = 0; target < nodeCount; target++) {
                    if (source != target) {
                        builder.addEdge(type, source, target);
                    }
                }
            }

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

    void readRows(const ColumnVector<size_t>& indices,
                  const ColumnNodeIDs& targets,
                  const ColumnVector<PathRef>& paths,
                  const PathTrie& trie,
                  std::vector<PathRow>& rows) {
        for (size_t row = 0; row < indices.size(); row++) {
            PathRow& emitted = rows.emplace_back();
            emitted._index = indices[row];
            emitted._target = targets[row].getValue();

            const ListView edges = trie.expandEdges(paths[row], _buffer);
            for (const ListElementView& element : edges) {
                emitted._edges.push_back(element.getAs<EdgeID>().getValue());
            }
        }
    }

    // Drives the explorator to exhaustion maxCount rows at a time. After every fill the trie
    // may hold the chunk's paths and each walker's current prefix, and nothing else.
    void exploreInChunks(const GraphView& view,
                         const ColumnNodeIDs& input,
                         uint64_t maxHops,
                         size_t walkerCount,
                         size_t maxCount,
                         std::vector<PathRow>& rows) {
        ColumnVector<size_t> indices;
        ColumnNodeIDs targets;
        ColumnVector<PathRef> paths;
        PathTrie trie;

        PathExplorator explorator(view, &input, PathExplorationDir::FORWARD, 1, maxHops);
        explorator.setIndices(&indices);
        explorator.setTargets(&targets);
        explorator.setPaths(&paths, &trie);
        explorator.setWalkerCount(walkerCount);

        rows.clear();
        size_t fills = 0;
        while (explorator.isValid()) {
            explorator.fill(maxCount);
            readRows(indices, targets, paths, trie, rows);
            fills++;

            const size_t heldEntries = 1 + (indices.size() + walkerCount) * maxHops;
            ASSERT_LE(trie.size(), heldEntries)
                << "fill " << fills << " left " << trie.size() << " entries in the trie for "
                << indices.size() << " rows and " << walkerCount << " walkers";
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
    ListBuffer<> _buffer;
};

}

TEST_F(PathExploratorReclaimTest, holdsTheChunkAndTheWalkedPrefixesAlone) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    const uint64_t maxHops = 4;
    ReferenceEnumerator reference(_adjacency, PathExplorationDir::FORWARD, 1, maxHops);
    std::vector<PathRow> expected;
    reference.enumerate(input, expected);
    ASSERT_GT(expected.size(), 1000u);

    for (const size_t walkerCount : {1, 16}) {
        for (const size_t maxCount : {1, 64, 1000}) {
            std::vector<PathRow> rows;
            exploreInChunks(view, input, maxHops, walkerCount, maxCount, rows);
            expectSameRows(expected, rows);
        }
    }
}

TEST_F(PathExploratorReclaimTest, exploratorsSharingATrieKeepTheirOwnEntries) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    allNodes(input);

    PathTrie trie;

    ColumnVector<size_t> outerIndices;
    ColumnNodeIDs outerTargets;
    ColumnVector<PathRef> outerPaths;
    PathExplorator outer(view, &input, PathExplorationDir::FORWARD, 1, 3);
    outer.setIndices(&outerIndices);
    outer.setTargets(&outerTargets);
    outer.setPaths(&outerPaths, &trie);

    outer.fill(8);
    ASSERT_EQ(outerIndices.size(), 8u);

    {
        ColumnVector<size_t> innerIndices;
        ColumnNodeIDs innerTargets;
        ColumnVector<PathRef> innerPaths;
        PathExplorator inner(view, &input, PathExplorationDir::FORWARD, 1, 2);
        inner.setIndices(&innerIndices);
        inner.setTargets(&innerTargets);
        inner.setPaths(&innerPaths, &trie);

        std::vector<PathRow> innerRows;
        while (inner.isValid()) {
            inner.fill(4);
            readRows(innerIndices, innerTargets, innerPaths, trie, innerRows);
        }

        ReferenceEnumerator innerReference(_adjacency, PathExplorationDir::FORWARD, 1, 2);
        std::vector<PathRow> innerExpected;
        innerReference.enumerate(input, innerExpected);
        expectSameRows(innerExpected, innerRows);
    }

    std::vector<PathRow> outerRows;
    readRows(outerIndices, outerTargets, outerPaths, trie, outerRows);
    while (outer.isValid()) {
        outer.fill(8);
        readRows(outerIndices, outerTargets, outerPaths, trie, outerRows);
    }

    ReferenceEnumerator outerReference(_adjacency, PathExplorationDir::FORWARD, 1, 3);
    std::vector<PathRow> outerExpected;
    outerReference.enumerate(input, outerExpected);
    expectSameRows(outerExpected, outerRows);
}
