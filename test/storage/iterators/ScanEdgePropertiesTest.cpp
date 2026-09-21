#include <gtest/gtest.h>

#include <stdint.h>
#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "iterators/ScanEdgePropertiesIterator.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t nodeCount = 6;
constexpr size_t edgeCount = 9;

using EdgeValue = std::pair<uint64_t, int64_t>;

int64_t valueOf(size_t edge) {
    return static_cast<int64_t>(edge) * 10;
}

constexpr uint64_t twiceRewrittenEdge = 2;
constexpr uint64_t rewrittenEdge = 5;
constexpr uint64_t nulledEdge = 4;

constexpr int64_t secondPartValue = 222;
constexpr int64_t thirdPartValue = 999;
constexpr int64_t rewrittenValue = 555;

}

class ScanEdgePropertiesTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        writeBasePart();
        writePatchPart({{twiceRewrittenEdge, secondPartValue}, {rewrittenEdge, rewrittenValue}}, {});
        writePatchPart({{twiceRewrittenEdge, thirdPartValue}}, {nulledEdge});
    }

    void declare(MetadataBuilder& metadata) {
        _rowLabel = metadata.getOrCreateLabel("Row");
        _edgeType = metadata.getOrCreateEdgeType("KNOWS");
        _weightID = metadata.getOrCreatePropertyType("weight", ValueType::Int64)._id;
    }

    void writeBasePart() {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        declare(builder.getMetadata());

        for (size_t node = 0; node < nodeCount; node++) {
            LabelSet labelset;
            labelset.set(_rowLabel);

            builder.addNode(labelset);
        }

        for (size_t edge = 0; edge < edgeCount; edge++) {
            const NodeID source {edge % nodeCount};
            const NodeID target {(edge + 1) % nodeCount};
            const EdgeRecord& record = builder.addEdge(_edgeType, source, target);

            builder.addEdgeProperty<types::Int64>(record, _weightID, valueOf(edge));
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    // Patching a property onto an edge of an older part needs that edge's record, which is
    // how the write buffer applies a SET to an edge the change only matched.
    void writePatchPart(const std::vector<EdgeValue>& overrides, const std::vector<uint64_t>& nulls) {
        std::vector<EdgeRecord> overrideRecords;
        std::vector<EdgeRecord> nullRecords;

        {
            const FrozenCommitTx transaction = _graph->openTransaction();
            const GraphReader reader = transaction.readGraph();

            for (const EdgeValue& rewrite : overrides) {
                const EdgeRecord* record = reader.getEdge(EdgeID {rewrite.first});
                ASSERT_NE(record, nullptr);
                overrideRecords.push_back(*record);
            }

            for (const uint64_t edge : nulls) {
                const EdgeRecord* record = reader.getEdge(EdgeID {edge});
                ASSERT_NE(record, nullptr);
                nullRecords.push_back(*record);
            }
        }

        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        declare(builder.getMetadata());

        for (size_t rewrite = 0; rewrite < overrides.size(); rewrite++) {
            builder.addEdgeProperty<types::Int64>(overrideRecords[rewrite],
                                                  _weightID,
                                                  overrides[rewrite].second);
        }

        for (const EdgeRecord& record : nullRecords) {
            builder.addEdgeProperty<types::Int64>(record, _weightID, std::nullopt);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    // What the per-edge lookup resolves for every edge: the newest part carrying the
    // property decides, and an edge nulled there holds no value at all.
    void expectedRows(const GraphView& view, std::vector<EdgeValue>& rows) {
        const GraphReader reader(view);

        rows.clear();
        for (uint64_t edge = 0; edge < edgeCount; edge++) {
            const std::optional<const int64_t*> value =
                reader.tryGetEdgeProperty<types::Int64>(_weightID, EdgeID {edge});
            const bool explicitNull = !value.has_value();

            if (explicitNull || !value.value()) {
                continue;
            }

            rows.emplace_back(edge, *value.value());
        }
    }

    void scanRange(const GraphView& view, std::vector<EdgeValue>& rows) {
        const GraphReader reader(view);
        const auto range = reader.scanEdgeProperties<types::Int64>(_weightID);

        rows.clear();
        for (auto it = range.begin(); it.isValid(); it.next()) {
            rows.emplace_back(it.getCurrentEdgeID().getValue(), it.get());
        }

        std::sort(rows.begin(), rows.end());
    }

    void scanChunked(const GraphView& view, size_t chunkSize, std::vector<EdgeValue>& rows) {
        ColumnVector<types::Int64::Primitive> values;
        ColumnEdgeIDs edgeIDs;

        ScanEdgePropertiesChunkWriter<types::Int64> writer(view, _weightID);
        writer.setProperties(&values);
        writer.setEdgeIDs(&edgeIDs);

        rows.clear();
        while (writer.isValid()) {
            writer.fill(chunkSize);

            ASSERT_EQ(values.size(), edgeIDs.size());
            ASSERT_LE(values.size(), chunkSize);

            for (size_t row = 0; row < values.size(); row++) {
                rows.emplace_back(edgeIDs[row].getValue(), values[row]);
            }
        }

        std::sort(rows.begin(), rows.end());
    }

    static bool holds(const std::vector<EdgeValue>& rows, uint64_t edge, int64_t value) {
        return std::find(rows.begin(), rows.end(), EdgeValue {edge, value}) != rows.end();
    }

    static bool mentions(const std::vector<EdgeValue>& rows, uint64_t edge) {
        const auto isEdge = [edge](const EdgeValue& row) { return row.first == edge; };
        return std::any_of(rows.begin(), rows.end(), isEdge);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    PropertyTypeID _weightID;
    EdgeTypeID _edgeType;
    LabelID _rowLabel;
};

TEST_F(ScanEdgePropertiesTest, expectationsPinTheRewritesAndTheNull) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeValue> expected;
    expectedRows(view, expected);

    EXPECT_EQ(expected.size(), edgeCount - 1);
    EXPECT_TRUE(holds(expected, twiceRewrittenEdge, thirdPartValue));
    EXPECT_TRUE(holds(expected, rewrittenEdge, rewrittenValue));
    EXPECT_FALSE(mentions(expected, nulledEdge));
}

TEST_F(ScanEdgePropertiesTest, rangeYieldsOneCurrentValuePerEdge) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeValue> expected;
    expectedRows(view, expected);

    std::vector<EdgeValue> found;
    scanRange(view, found);

    EXPECT_EQ(found, expected);
}

TEST_F(ScanEdgePropertiesTest, chunkWriterYieldsOneCurrentValuePerEdge) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeValue> expected;
    expectedRows(view, expected);

    for (const size_t chunkSize : {size_t {1}, size_t {2}, size_t {5}, size_t {64}}) {
        std::vector<EdgeValue> found;
        scanChunked(view, chunkSize, found);

        EXPECT_EQ(found, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ScanEdgePropertiesTest, nulledEdgeIsNotEmittedWithItsOlderValue) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeValue> found;
    scanRange(view, found);

    EXPECT_FALSE(mentions(found, nulledEdge));
}

TEST_F(ScanEdgePropertiesTest, rewrittenEdgeIsNotEmittedTwice) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeValue> found;
    scanRange(view, found);

    const auto isRewritten = [](const EdgeValue& row) { return row.first == twiceRewrittenEdge; };
    EXPECT_EQ(std::count_if(found.begin(), found.end(), isRewritten), 1);
    EXPECT_TRUE(holds(found, twiceRewrittenEdge, thirdPartValue));
}
