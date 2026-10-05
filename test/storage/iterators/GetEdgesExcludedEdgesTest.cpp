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
#include "iterators/GetEdgesIterator.h"
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

void collectEdges(const GraphReader& reader,
                  const ColumnNodeIDs* input,
                  const ExcludedEdges& excluded,
                  FilledColumns filled,
                  size_t maxCount,
                  ColumnVector<size_t>& indices,
                  ColumnEdgeIDs& edgeIDs,
                  ColumnNodeIDs& others,
                  ColumnEdgeTypes& types) {
    ColumnVector<size_t> chunkIndices;
    ColumnEdgeIDs chunkEdgeIDs;
    ColumnNodeIDs chunkOthers;
    ColumnEdgeTypes chunkTypes;

    GetEdgesChunkWriter writer(reader.getView(), input);
    writer.setIndices(&chunkIndices);
    writer.setEdgeIDs(filled._edgeIDs ? &chunkEdgeIDs : nullptr);
    writer.setOtherIDs(filled._others ? &chunkOthers : nullptr);
    writer.setEdgeTypes(filled._types ? &chunkTypes : nullptr);
    writer.setExcludedEdges(excluded);

    while (writer.isValid()) {
        writer.fill(maxCount);
        EXPECT_LE(chunkIndices.size(), maxCount);

        for (size_t row = 0; row < chunkIndices.size(); row++) {
            indices.push_back(chunkIndices[row]);

            if (filled._edgeIDs) {
                edgeIDs.push_back(chunkEdgeIDs[row]);
            }
            if (filled._others) {
                others.push_back(chunkOthers[row]);
            }
            if (filled._types) {
                types.push_back(chunkTypes[row]);
            }
        }
    }
}

void collectReference(const GraphReader& reader, const ColumnNodeIDs* input, std::vector<EdgeRow>& rows) {
    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs others;
    ColumnEdgeTypes types;

    collectEdges(reader, input, {}, {true, true, true}, ChunkConfig::CHUNK_SIZE, indices, edgeIDs, others, types);

    for (size_t row = 0; row < indices.size(); row++) {
        rows.push_back({indices[row], edgeIDs[row], others[row], types[row]});
    }
}

bool holds(std::span<const EdgeID> edges, EdgeID edge) {
    return std::find(edges.begin(), edges.end(), edge) != edges.end();
}

}

// Node 1 has out-edges and in-edges in both parts, a self-loop in each part, and two
// parallel in-edges from node 3; the second part holds node 1's edges as a patch node.
class GetEdgesExcludedEdgesTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto& builder = change->access().getTip()->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("Node")});
            const EdgeTypeID link = metadata.getOrCreateEdgeType("LINK");
            const EdgeTypeID loop = metadata.getOrCreateEdgeType("LOOP");

            for (size_t node = 0; node < 4; node++) {
                builder.addNode(labelset);
            }

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

            const LabelSet labelset = LabelSet::fromList({metadata.getOrCreateLabel("Node")});
            const EdgeTypeID link = metadata.getOrCreateEdgeType("LINK");
            const EdgeTypeID loop = metadata.getOrCreateEdgeType("LOOP");

            builder.addNode(labelset);

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

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(GetEdgesExcludedEdgesTest, leavesOutEveryExcludedEdgeForEveryColumnSet) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const ColumnNodeIDs input = {1, 1, 2, 1, 0, 1, 3};

    std::vector<EdgeRow> reference;
    collectReference(reader, &input, reference);

    std::vector<std::vector<EdgeID>> rowEdges(input.size());
    for (const EdgeRow& row : reference) {
        rowEdges[row._index].push_back(row._edgeID);
    }

    const auto edgeTo = [&](size_t index, NodeID other) {
        for (const EdgeRow& row : reference) {
            if (row._index == index && row._otherID == other) {
                return row._edgeID;
            }
        }

        throw TuringException("No such edge in the reference rows");
    };

    std::vector<std::vector<EdgeID>> excluded(input.size());

    for (size_t position = 0; position < rowEdges[1].size(); position += 2) {
        excluded[1].push_back(rowEdges[1][position]);
    }

    excluded[2] = {rowEdges[2].front(), edgeTo(4, 1)};

    for (const EdgeRow& row : reference) {
        if (row._index == 3 && row._otherID == 1) {
            excluded[3].push_back(row._edgeID);
        }
    }
    ASSERT_EQ(excluded[3].size(), 4);
    excluded[3].push_back(excluded[3].front());

    excluded[4] = {edgeTo(1, 4), edgeTo(4, 1)};
    excluded[5] = rowEdges[5];

    std::vector<size_t> offsets;
    std::vector<EdgeID> edges;
    for (const std::vector<EdgeID>& rowExcluded : excluded) {
        offsets.push_back(edges.size());
        edges.insert(edges.end(), rowExcluded.begin(), rowExcluded.end());
    }
    offsets.push_back(edges.size());

    std::vector<EdgeRow> expected;
    for (const EdgeRow& row : reference) {
        if (!holds(excluded[row._index], row._edgeID)) {
            expected.push_back(row);
        }
    }
    ASSERT_LT(expected.size(), reference.size());

    const ExcludedEdges excludedEdges {offsets, edges};

    for (size_t combination = 0; combination < 8; combination++) {
        const FilledColumns filled {(combination & 1) != 0, (combination & 2) != 0, (combination & 4) != 0};

        for (const size_t maxCount : {size_t {1}, size_t {2}, size_t {3}, size_t {5}, ChunkConfig::CHUNK_SIZE}) {
            SCOPED_TRACE(testing::Message() << "combination " << combination << ", maxCount " << maxCount);

            ColumnVector<size_t> indices;
            ColumnEdgeIDs edgeIDs;
            ColumnNodeIDs others;
            ColumnEdgeTypes types;
            collectEdges(reader, &input, excludedEdges, filled, maxCount, indices, edgeIDs, others, types);

            ASSERT_EQ(indices.size(), expected.size());
            for (size_t row = 0; row < expected.size(); row++) {
                EXPECT_EQ(indices[row], expected[row]._index);

                if (filled._edgeIDs) {
                    EXPECT_EQ(edgeIDs[row], expected[row]._edgeID);
                }
                if (filled._others) {
                    EXPECT_EQ(others[row], expected[row]._otherID);
                }
                if (filled._types) {
                    EXPECT_EQ(types[row], expected[row]._edgeTypeID);
                }
            }
        }
    }
}
