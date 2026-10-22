#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <tuple>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/ScanEdgesByEndpointLabelsChunkWriter.h"
#include "iterators/ScanInEdgesByTargetLabelIterator.h"
#include "iterators/ScanOutEdgesBySourceLabelIterator.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"
#include "SimpleGraph.h"

using namespace db;
using namespace turing::test;

namespace {

// One row as the writer lays it out: the indexed end, the edge, its type and the other end.
using ScannedRow = std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>;

struct ScannedChunk {
    ColumnNodeIDs _indexedEnds;
    ColumnEdgeIDs _edgeIDs;
    ColumnEdgeTypes _edgeTypes;
    ColumnNodeIDs _otherEnds;
};

template <typename ChunkWriterType>
void bindChunk(ChunkWriterType& chunkWriter, ScannedChunk& chunk) {
    chunkWriter.setSrcIDs(&chunk._indexedEnds);
    chunkWriter.setEdgeIDs(&chunk._edgeIDs);
    chunkWriter.setEdgeTypes(&chunk._edgeTypes);
    chunkWriter.setTgtIDs(&chunk._otherEnds);
}

void appendRows(const ScannedChunk& chunk, std::vector<ScannedRow>& rows) {
    for (size_t row = 0; row < chunk._edgeIDs.size(); row++) {
        rows.emplace_back(chunk._indexedEnds[row].getValue(),
                          chunk._edgeIDs[row].getValue(),
                          chunk._edgeTypes[row].getValue(),
                          chunk._otherEnds[row].getValue());
    }
}

// Drives the writer as the executor does, one fill of chunkSize rows at a time, and checks
// that no fill writes more than chunkSize rows and every fill leaves the four columns aligned.
template <typename ChunkWriterType>
void scanInChunks(ChunkWriterType& chunkWriter, size_t chunkSize, std::vector<ScannedRow>& rows) {
    ScannedChunk chunk;
    bindChunk(chunkWriter, chunk);

    while (chunkWriter.isValid()) {
        chunkWriter.fill(chunkSize);

        const size_t rowCount = chunk._edgeIDs.size();
        ASSERT_LE(rowCount, chunkSize);
        ASSERT_EQ(chunk._indexedEnds.size(), rowCount);
        ASSERT_EQ(chunk._edgeTypes.size(), rowCount);
        ASSERT_EQ(chunk._otherEnds.size(), rowCount);

        appendRows(chunk, rows);
    }

    std::sort(rows.begin(), rows.end());
}

}

// The both-labels writer keeps the edges of one end's index whose other end carries a second
// label set. Its rows are checked against the single-end writer over the same index, with the
// other end filtered here, so a range cut short by the chunk size or a row dropped by a
// tombstone shows up as a missing, extra or misaligned row.
class ScanEdgesByEndpointLabelsChunkWriterTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void labelSet(std::string_view name, LabelSet& labelset) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        const std::optional<LabelID> label = reader.getMetadata().labels().get(name);
        ASSERT_TRUE(label.has_value());

        labelset.set(*label);
    }

    // The rows of the single-end writer over the same index whose other end carries otherLabels.
    template <typename SingleEndChunkWriterType>
    void expectedRows(const LabelSet& indexedLabels, const LabelSet& otherLabels, std::vector<ScannedRow>& rows) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        SingleEndChunkWriterType chunkWriter(reader.getView(), LabelSetHandle(indexedLabels));
        std::vector<ScannedRow> indexedRows;
        scanInChunks(chunkWriter, ChunkConfig::CHUNK_SIZE, indexedRows);

        const LabelSetHandle otherHandle(otherLabels);
        for (const ScannedRow& row : indexedRows) {
            const LabelSetHandle otherEndLabels = reader.getNodeLabelSet(NodeID {std::get<3>(row)});
            if (otherEndLabels.isValid() && otherEndLabels.hasAtLeastLabels(otherHandle)) {
                rows.push_back(row);
            }
        }
    }

    template <typename ChunkWriterType>
    void scannedRows(const LabelSet& indexedLabels, const LabelSet& otherLabels, size_t chunkSize, std::vector<ScannedRow>& rows) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        ChunkWriterType chunkWriter(reader.getView(), LabelSetHandle(indexedLabels), LabelSetHandle(otherLabels));
        scanInChunks(chunkWriter, chunkSize, rows);
    }

    void deleteEdge(EdgeID edgeID) {
        GraphWriter writer(_graph.get(), _jobSystem.get());
        writer.deleteEdge(edgeID);
        writer.submit();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

// The 15 Person -> Interest edges sit among the Person out-edges with the KNOWS_WELL edges
// between Person nodes, so a fill of 1 to 4 rows stops mid-range, and some ranges end on
// edges the other end's labels drop.
TEST_F(ScanEdgesByEndpointLabelsChunkWriterTest, outScanEmitsEveryRowWhateverTheChunkSize) {
    LabelSet person;
    LabelSet interest;
    labelSet("Person", person);
    labelSet("Interest", interest);

    std::vector<ScannedRow> expected;
    expectedRows<ScanOutEdgesBySourceLabelChunkWriter>(person, interest, expected);
    ASSERT_EQ(expected.size(), 15u);

    for (size_t chunkSize = 1; chunkSize <= 4; chunkSize++) {
        std::vector<ScannedRow> rows;
        scannedRows<ScanOutEdgesByBothLabelsChunkWriter>(person, interest, chunkSize, rows);

        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ScanEdgesByEndpointLabelsChunkWriterTest, inScanEmitsEveryRowWhateverTheChunkSize) {
    LabelSet person;
    LabelSet interest;
    labelSet("Person", person);
    labelSet("Interest", interest);

    std::vector<ScannedRow> expected;
    expectedRows<ScanInEdgesByTargetLabelChunkWriter>(interest, person, expected);
    ASSERT_EQ(expected.size(), 15u);

    for (size_t chunkSize = 1; chunkSize <= 4; chunkSize++) {
        std::vector<ScannedRow> rows;
        scannedRows<ScanInEdgesByBothLabelsChunkWriter>(interest, person, chunkSize, rows);

        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ScanEdgesByEndpointLabelsChunkWriterTest, outScanDropsADeletedEdgeFromEveryColumn) {
    LabelSet person;
    LabelSet interest;
    labelSet("Person", person);
    labelSet("Interest", interest);

    std::vector<ScannedRow> beforeDelete;
    expectedRows<ScanOutEdgesBySourceLabelChunkWriter>(person, interest, beforeDelete);
    ASSERT_FALSE(beforeDelete.empty());

    const ScannedRow deletedRow = beforeDelete.front();
    deleteEdge(EdgeID {std::get<1>(deletedRow)});

    std::vector<ScannedRow> expected = beforeDelete;
    expected.erase(expected.begin());

    for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
        std::vector<ScannedRow> rows;
        scannedRows<ScanOutEdgesByBothLabelsChunkWriter>(person, interest, chunkSize, rows);

        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ScanEdgesByEndpointLabelsChunkWriterTest, inScanDropsADeletedEdgeFromEveryColumn) {
    LabelSet person;
    LabelSet interest;
    labelSet("Person", person);
    labelSet("Interest", interest);

    std::vector<ScannedRow> beforeDelete;
    expectedRows<ScanInEdgesByTargetLabelChunkWriter>(interest, person, beforeDelete);
    ASSERT_FALSE(beforeDelete.empty());

    const ScannedRow deletedRow = beforeDelete.front();
    deleteEdge(EdgeID {std::get<1>(deletedRow)});

    std::vector<ScannedRow> expected = beforeDelete;
    expected.erase(expected.begin());

    for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
        std::vector<ScannedRow> rows;
        scannedRows<ScanInEdgesByBothLabelsChunkWriter>(interest, person, chunkSize, rows);

        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
    }
}
