#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/ExcludedEdges.h"
#include "iterators/GetInEdgesByTypeAndLabelIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesByTypeAndLabelIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "metadata/GraphMetadata.h"
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

// One emitted (input index, edge, neighbour) row as raw values
struct CollectedEdge {
    size_t _index {0};
    uint64_t _edgeID {0};
    uint64_t _neighbour {0};
};

void collectOutEdges(const GraphReader& reader,
                     const ColumnNodeIDs* input,
                     const ExcludedEdges& excluded,
                     size_t maxCount,
                     std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;

    GetOutEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);
    writer.setExcludedEdges(excluded);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        for (size_t row = 0; row < indices.size(); row++) {
            out.push_back({indices[row], edgeIDs[row].getValue(), targets[row].getValue()});
        }
    }
}

void collectInEdges(const GraphReader& reader,
                    const ColumnNodeIDs* input,
                    const ExcludedEdges& excluded,
                    size_t maxCount,
                    std::vector<CollectedEdge>& out) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs sources;

    GetInEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    writer.setSrcIDs(&sources);
    writer.setExcludedEdges(excluded);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        for (size_t row = 0; row < indices.size(); row++) {
            out.push_back({indices[row], edgeIDs[row].getValue(), sources[row].getValue()});
        }
    }
}

// The same writers' typed and labelled siblings, asked for the LINK edges reaching a Node
template <typename Writer>
void collectTypedLabelledEdges(const GraphReader& reader,
                               const ColumnNodeIDs* input,
                               const ExcludedEdges& excluded,
                               std::vector<CollectedEdge>& out) {
    const GraphView view = reader.getView();
    const GraphMetadata& metadata = view.metadata();
    const std::vector<EdgeTypeID> types {metadata.edgeTypes().get("LINK").value()};
    const LabelSet labels = LabelSet::fromList({metadata.labels().get("Node").value()});
    const LabelSetHandle handle {labels};

    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs neighbours;

    Writer writer(view, input, types, handle);
    writer.setIndices(&indices);
    writer.setEdgeIDs(&edgeIDs);
    if constexpr (std::is_same_v<Writer, GetOutEdgesByTypeAndLabelChunkWriter>) {
        writer.setTgtIDs(&neighbours);
    } else {
        writer.setSrcIDs(&neighbours);
    }
    writer.setExcludedEdges(excluded);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);
        for (size_t row = 0; row < indices.size(); row++) {
            out.push_back({indices[row], edgeIDs[row].getValue(), neighbours[row].getValue()});
        }
    }
}

uint64_t edgeTo(const std::vector<CollectedEdge>& edges, size_t index, uint64_t neighbour) {
    for (const CollectedEdge& edge : edges) {
        if (edge._index == index && edge._neighbour == neighbour) {
            return edge._edgeID;
        }
    }

    throw TuringException("No such edge in the collected rows");
}

void expectSameContent(std::vector<std::pair<size_t, uint64_t>> expected, const std::vector<CollectedEdge>& actual) {
    std::vector<std::pair<size_t, uint64_t>> actualPairs;
    for (const CollectedEdge& edge : actual) {
        actualPairs.emplace_back(edge._index, edge._neighbour);
    }

    std::sort(expected.begin(), expected.end());
    std::sort(actualPairs.begin(), actualPairs.end());
    EXPECT_EQ(expected, actualPairs);
}

}

// The writers leave out of each input row the edges its span holds: an out-run by the
// arithmetic over its consecutive IDs, an in-run by a scan. Node 1 has two out-edges (to 0
// and 2) and two in-edges (from 0 and 2).
class ExcludedEdgesTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID label = metadata.getOrCreateLabel("Node");
        const LabelSet labelset = LabelSet::fromList({label});
        const EdgeTypeID link = metadata.getOrCreateEdgeType("LINK");

        for (size_t node = 0; node < 3; node++) {
            builder.addNode(labelset);
        }

        builder.addEdge(link, 0, 1);
        builder.addEdge(link, 0, 2);
        builder.addEdge(link, 1, 0);
        builder.addEdge(link, 1, 2);
        builder.addEdge(link, 2, 1);

        const auto res = change->access().submit(*_jobSystem);
        ASSERT_TRUE(res);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(ExcludedEdgesTest, leavesTheExcludedEdgeOutOfAnOutRun) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1};

    std::vector<CollectedEdge> all;
    collectOutEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);
    expectSameContent({{0, 0}, {0, 2}, {1, 0}, {1, 2}}, all);

    // Row 0 leaves 1 -> 0 out, row 1 an edge node 1 does not have
    std::vector<CollectedEdge> allOfZero;
    const ColumnNodeIDs zero = {0};
    collectOutEdges(reader, &zero, {}, ChunkConfig::CHUNK_SIZE, allOfZero);

    const std::vector<size_t> offsets {0, 1, 2};
    const std::vector<EdgeID> edges {EdgeID(edgeTo(all, 0, 0)), EdgeID(edgeTo(allOfZero, 0, 1))};

    std::vector<CollectedEdge> pruned;
    collectOutEdges(reader, &input, ExcludedEdges {offsets, edges}, ChunkConfig::CHUNK_SIZE, pruned);
    expectSameContent({{0, 2}, {1, 0}, {1, 2}}, pruned);
}

TEST_F(ExcludedEdgesTest, leavesTheExcludedEdgeOutOfAnInRun) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1};

    std::vector<CollectedEdge> all;
    collectInEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);
    expectSameContent({{0, 0}, {0, 2}, {1, 0}, {1, 2}}, all);

    const std::vector<size_t> offsets {0, 1, 2};
    const std::vector<EdgeID> edges {EdgeID(edgeTo(all, 0, 0)), EdgeID(edgeTo(all, 1, 2))};

    std::vector<CollectedEdge> pruned;
    collectInEdges(reader, &input, ExcludedEdges {offsets, edges}, ChunkConfig::CHUNK_SIZE, pruned);
    expectSameContent({{0, 2}, {1, 0}}, pruned);
}

TEST_F(ExcludedEdgesTest, leavesTheExcludedEdgeOutOfATypedLabelledRun) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1};

    std::vector<CollectedEdge> outs;
    collectTypedLabelledEdges<GetOutEdgesByTypeAndLabelChunkWriter>(reader, &input, {}, outs);
    expectSameContent({{0, 0}, {0, 2}, {1, 0}, {1, 2}}, outs);

    std::vector<CollectedEdge> ins;
    collectTypedLabelledEdges<GetInEdgesByTypeAndLabelChunkWriter>(reader, &input, {}, ins);
    expectSameContent({{0, 0}, {0, 2}, {1, 0}, {1, 2}}, ins);

    const std::vector<size_t> offsets {0, 1, 2};
    const std::vector<EdgeID> outEdges {EdgeID(edgeTo(outs, 0, 0)), EdgeID(edgeTo(outs, 1, 2))};
    const std::vector<EdgeID> inEdges {EdgeID(edgeTo(ins, 0, 0)), EdgeID(edgeTo(ins, 1, 2))};

    std::vector<CollectedEdge> prunedOuts;
    collectTypedLabelledEdges<GetOutEdgesByTypeAndLabelChunkWriter>(reader, &input, ExcludedEdges {offsets, outEdges}, prunedOuts);
    expectSameContent({{0, 2}, {1, 0}}, prunedOuts);

    std::vector<CollectedEdge> prunedIns;
    collectTypedLabelledEdges<GetInEdgesByTypeAndLabelChunkWriter>(reader, &input, ExcludedEdges {offsets, inEdges}, prunedIns);
    expectSameContent({{0, 2}, {1, 0}}, prunedIns);
}

TEST_F(ExcludedEdgesTest, prunesARunSplitAcrossFills) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1};

    std::vector<CollectedEdge> all;
    collectOutEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);

    const std::vector<size_t> offsets {0, 1, 2};
    const std::vector<EdgeID> edges {EdgeID(edgeTo(all, 0, 2)), EdgeID(edgeTo(all, 1, 0))};

    std::vector<CollectedEdge> pruned;
    collectOutEdges(reader, &input, ExcludedEdges {offsets, edges}, 1, pruned);
    expectSameContent({{0, 0}, {1, 2}}, pruned);
}

TEST_F(ExcludedEdgesTest, aRowMayLeaveSeveralEdgesOut) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1};

    std::vector<CollectedEdge> all;
    collectOutEdges(reader, &input, {}, ChunkConfig::CHUNK_SIZE, all);

    const std::vector<size_t> offsets {0, 2};
    const std::vector<EdgeID> edges {EdgeID(edgeTo(all, 0, 0)), EdgeID(edgeTo(all, 0, 2))};

    std::vector<CollectedEdge> pruned;
    collectOutEdges(reader, &input, ExcludedEdges {offsets, edges}, ChunkConfig::CHUNK_SIZE, pruned);
    EXPECT_TRUE(pruned.empty());
}
