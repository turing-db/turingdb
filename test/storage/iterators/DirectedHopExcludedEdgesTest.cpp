#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/ExcludedEdges.h"
#include "iterators/GetInEdgesByLabelIterator.h"
#include "iterators/GetInEdgesByTypeIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesByLabelIterator.h"
#include "iterators/GetOutEdgesByTypeIterator.h"
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

struct EdgeRow {
    size_t _index {0};
    EdgeID _edgeID;
    NodeID _otherID;
    EdgeTypeID _edgeTypeID;
};

struct FilledColumns {
    bool _edgeIDs {false};
    bool _others {false};
    bool _types {false};
};

template <typename Writer>
void collectEdges(Writer& writer,
                  void (Writer::*setOthers)(ColumnNodeIDs*),
                  const ExcludedEdges& excluded,
                  FilledColumns filled,
                  size_t maxCount,
                  std::vector<EdgeRow>& rows) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs others;
    ColumnEdgeTypes types;

    writer.setIndices(&indices);
    writer.setEdgeIDs(filled._edgeIDs ? &edgeIDs : nullptr);
    (writer.*setOthers)(filled._others ? &others : nullptr);
    writer.setEdgeTypes(filled._types ? &types : nullptr);
    writer.setExcludedEdges(excluded);

    while (writer.isValid()) {
        writer.fill(maxCount);
        EXPECT_LE(indices.size(), maxCount);

        for (size_t row = 0; row < indices.size(); row++) {
            EdgeRow edge;
            edge._index = indices[row];

            if (filled._edgeIDs) {
                edge._edgeID = edgeIDs[row];
            }
            if (filled._others) {
                edge._otherID = others[row];
            }
            if (filled._types) {
                edge._edgeTypeID = types[row];
            }

            rows.push_back(edge);
        }
    }
}

bool holds(std::span<const EdgeID> edges, EdgeID edge) {
    return std::find(edges.begin(), edges.end(), edge) != edges.end();
}

// Every other edge of a row, all of a row with a repeat, or none, plus an edge the row's
// node does not have
void chooseExcluded(const std::vector<EdgeRow>& reference,
                    size_t rowCount,
                    std::vector<std::vector<EdgeID>>& excluded) {
    std::vector<std::vector<EdgeID>> rowEdges(rowCount);
    for (const EdgeRow& row : reference) {
        rowEdges[row._index].push_back(row._edgeID);
    }

    excluded.assign(rowCount, {});
    for (size_t row = 0; row < rowCount; row++) {
        const std::vector<EdgeID>& edges = rowEdges[row];

        if (row % 3 == 1) {
            for (size_t position = 0; position < edges.size(); position += 2) {
                excluded[row].push_back(edges[position]);
            }
        } else if (row % 3 == 2 && !edges.empty()) {
            excluded[row] = edges;
            excluded[row].push_back(edges.front());
        }

        for (const EdgeRow& other : reference) {
            if (!holds(edges, other._edgeID)) {
                excluded[row].push_back(other._edgeID);
                break;
            }
        }
    }
}

}

// Node 1 has out-edges and in-edges in both parts, a self-loop in each part, and two
// parallel in-edges from node 3; the second part holds node 1's edges as a patch node.
// Nodes 2 and 4 carry a second label, so the by-label writers keep only their edges.
class DirectedHopExcludedEdgesTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto& builder = change->access().getTip()->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelID node = metadata.getOrCreateLabel("Node");
            const LabelID other = metadata.getOrCreateLabel("Other");
            const LabelSet plain = LabelSet::fromList({node});
            const LabelSet marked = LabelSet::fromList({node, other});
            const EdgeTypeID link = metadata.getOrCreateEdgeType("LINK");
            const EdgeTypeID loop = metadata.getOrCreateEdgeType("LOOP");

            builder.addNode(plain);
            builder.addNode(plain);
            builder.addNode(marked);
            builder.addNode(plain);

            builder.addEdge(link, 0, 1);
            builder.addEdge(link, 1, 0);
            builder.addEdge(link, 1, 2);
            builder.addEdge(link, 2, 1);
            builder.addEdge(loop, 1, 1);
            builder.addEdge(link, 3, 1);
            builder.addEdge(link, 3, 1);
            builder.addEdge(link, 1, 3);

            ASSERT_TRUE(change->access().submit(*_jobSystem));
        }

        {
            auto change = _graph->newChange();
            auto& builder = change->access().getTip()->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet marked = LabelSet::fromList({metadata.getOrCreateLabel("Node"),
                                                        metadata.getOrCreateLabel("Other")});
            const EdgeTypeID link = metadata.getOrCreateEdgeType("LINK");
            const EdgeTypeID loop = metadata.getOrCreateEdgeType("LOOP");

            builder.addNode(marked);

            builder.addEdge(link, 1, 4);
            builder.addEdge(link, 4, 1);
            builder.addEdge(link, 2, 1);
            builder.addEdge(loop, 1, 1);

            ASSERT_TRUE(change->access().submit(*_jobSystem));
        }
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Runs the writer @p makeWriter builds over every column set and chunk size, and
    // compares it with its own unexcluded rows less the rows @p excluded holds
    template <typename Writer, typename MakeWriter>
    void expectLeavesOutExcluded(MakeWriter makeWriter,
                                 void (Writer::*setOthers)(ColumnNodeIDs*),
                                 const std::vector<std::vector<EdgeID>>& excluded) {
        std::vector<EdgeRow> reference;
        {
            std::unique_ptr<Writer> writer = makeWriter();
            collectEdges(*writer, setOthers, {}, {true, true, true}, ChunkConfig::CHUNK_SIZE, reference);
        }

        std::vector<size_t> offsets;
        std::vector<EdgeID> edges;
        for (const std::vector<EdgeID>& rowExcluded : excluded) {
            offsets.push_back(edges.size());
            edges.insert(edges.end(), rowExcluded.begin(), rowExcluded.end());
        }
        offsets.push_back(edges.size());

        const ExcludedEdges excludedEdges {offsets, edges};

        std::vector<EdgeRow> expected;
        for (const EdgeRow& row : reference) {
            if (!holds(excluded[row._index], row._edgeID)) {
                expected.push_back(row);
            }
        }
        ASSERT_LT(expected.size(), reference.size());

        for (size_t combination = 0; combination < 8; combination++) {
            const FilledColumns filled {(combination & 1) != 0, (combination & 2) != 0, (combination & 4) != 0};

            for (const size_t maxCount : {size_t {1}, size_t {2}, size_t {3}, size_t {5}, ChunkConfig::CHUNK_SIZE}) {
                SCOPED_TRACE(testing::Message() << "combination " << combination << ", maxCount " << maxCount);

                std::unique_ptr<Writer> writer = makeWriter();
                std::vector<EdgeRow> rows;
                collectEdges(*writer, setOthers, excludedEdges, filled, maxCount, rows);

                ASSERT_EQ(rows.size(), expected.size());
                for (size_t row = 0; row < expected.size(); row++) {
                    EXPECT_EQ(rows[row]._index, expected[row]._index);

                    if (filled._edgeIDs) {
                        EXPECT_EQ(rows[row]._edgeID, expected[row]._edgeID);
                    }
                    if (filled._others) {
                        EXPECT_EQ(rows[row]._otherID, expected[row]._otherID);
                    }
                    if (filled._types) {
                        EXPECT_EQ(rows[row]._edgeTypeID, expected[row]._edgeTypeID);
                    }
                }
            }
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(DirectedHopExcludedEdgesTest, leavesOutEveryExcludedEdgeInEveryDirectedWriter) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const ColumnNodeIDs input = {1, 1, 2, 1, 0, 1, 3, 4};

    const GraphMetadata& metadata = view.metadata();
    const std::vector<EdgeTypeID> linkType {metadata.edgeTypes().get("LINK").value()};
    const LabelSet otherLabelSet = LabelSet::fromList({metadata.labels().get("Other").value()});
    const LabelSetHandle otherLabel {otherLabelSet};

    std::vector<EdgeRow> outReference;
    {
        GetOutEdgesChunkWriter writer(view, &input);
        collectEdges(writer, &GetOutEdgesChunkWriter::setTgtIDs, {}, {true, true, true}, ChunkConfig::CHUNK_SIZE, outReference);
    }

    std::vector<EdgeRow> inReference;
    {
        GetInEdgesChunkWriter writer(view, &input);
        collectEdges(writer, &GetInEdgesChunkWriter::setSrcIDs, {}, {true, true, true}, ChunkConfig::CHUNK_SIZE, inReference);
    }

    std::vector<std::vector<EdgeID>> outExcluded;
    chooseExcluded(outReference, input.size(), outExcluded);

    std::vector<std::vector<EdgeID>> inExcluded;
    chooseExcluded(inReference, input.size(), inExcluded);

    {
        SCOPED_TRACE("GetOutEdgesChunkWriter");
        expectLeavesOutExcluded([&]() { return std::make_unique<GetOutEdgesChunkWriter>(view, &input); },
                                &GetOutEdgesChunkWriter::setTgtIDs,
                                outExcluded);
    }
    {
        SCOPED_TRACE("GetInEdgesChunkWriter");
        expectLeavesOutExcluded([&]() { return std::make_unique<GetInEdgesChunkWriter>(view, &input); },
                                &GetInEdgesChunkWriter::setSrcIDs,
                                inExcluded);
    }
    {
        SCOPED_TRACE("GetOutEdgesByTypeChunkWriter");
        expectLeavesOutExcluded([&]() { return std::make_unique<GetOutEdgesByTypeChunkWriter>(view, &input, linkType); },
                                &GetOutEdgesByTypeChunkWriter::setTgtIDs,
                                outExcluded);
    }
    {
        SCOPED_TRACE("GetInEdgesByTypeChunkWriter");
        expectLeavesOutExcluded([&]() { return std::make_unique<GetInEdgesByTypeChunkWriter>(view, &input, linkType); },
                                &GetInEdgesByTypeChunkWriter::setSrcIDs,
                                inExcluded);
    }
    {
        SCOPED_TRACE("GetOutEdgesByLabelChunkWriter");
        expectLeavesOutExcluded([&]() { return std::make_unique<GetOutEdgesByLabelChunkWriter>(view, &input, otherLabel); },
                                &GetOutEdgesByLabelChunkWriter::setTgtIDs,
                                outExcluded);
    }
    {
        SCOPED_TRACE("GetInEdgesByLabelChunkWriter");
        expectLeavesOutExcluded([&]() { return std::make_unique<GetInEdgesByLabelChunkWriter>(view, &input, otherLabel); },
                                &GetInEdgesByLabelChunkWriter::setSrcIDs,
                                inExcluded);
    }
}
