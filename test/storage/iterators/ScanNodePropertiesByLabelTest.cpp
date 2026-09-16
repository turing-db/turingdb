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
#include "iterators/ScanNodePropertiesByLabelIterator.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
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

constexpr size_t nodeCount = 12;

using NodeValue = std::pair<uint64_t, int64_t>;

int64_t valueOf(size_t node) {
    return static_cast<int64_t>(node) * 10;
}

// Node 2 is rewritten in the second part and again in the third, so two older parts still
// carry a value for it. Node 5 is rewritten once. Node 4 is nulled in the third part.
const std::vector<NodeValue> secondPartOverrides {{2, 222}, {5, 555}};
const std::vector<NodeValue> thirdPartOverrides {{2, 999}};
const std::vector<uint64_t> thirdPartNulls {4};

}

class ScanNodePropertiesByLabelTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        writePart(0, nodeCount, {}, {});
        writePart(nodeCount, 0, secondPartOverrides, {});
        writePart(nodeCount, 0, thirdPartOverrides, thirdPartNulls);
    }

    void writePart(size_t firstNode,
                   size_t count,
                   const std::vector<NodeValue>& overrides,
                   const std::vector<uint64_t>& nulls) {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();
        MetadataBuilder& metadata = builder.getMetadata();

        _rowLabel = metadata.getOrCreateLabel("Row");
        _evenLabel = metadata.getOrCreateLabel("Even");
        _valueID = metadata.getOrCreatePropertyType("value", ValueType::Int64)._id;

        for (size_t node = firstNode; node < firstNode + count; node++) {
            LabelSet labelset;
            labelset.set(_rowLabel);

            if (node % 2 == 0) {
                labelset.set(_evenLabel);
            }

            const NodeID nodeID = builder.addNode(labelset);

            builder.addNodeProperty<types::Int64>(nodeID, _valueID, valueOf(node));
        }

        for (const NodeValue& rewrite : overrides) {
            builder.addNodeProperty<types::Int64>(NodeID {rewrite.first}, _valueID, rewrite.second);
        }

        for (const uint64_t node : nulls) {
            builder.addNodeProperty<types::Int64>(NodeID {node}, _valueID, std::nullopt);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    void scanRange(const GraphView& view, const LabelSetHandle& labelset, std::vector<NodeValue>& rows) {
        const GraphReader reader(view);
        const auto range = reader.scanNodePropertiesByLabel<types::Int64>(_valueID, labelset);

        rows.clear();
        for (auto it = range.begin(); it.isValid(); it.next()) {
            rows.emplace_back(it.getCurrentNodeID().getValue(), it.get());
        }

        std::sort(rows.begin(), rows.end());
    }

    void scanChunked(const GraphView& view,
                     const LabelSetHandle& labelset,
                     size_t chunkSize,
                     std::vector<NodeValue>& rows) {
        ColumnVector<types::Int64::Primitive> values;
        ColumnNodeIDs nodeIDs;

        ScanNodePropertiesByLabelChunkWriter<types::Int64> writer(view, _valueID, labelset);
        writer.setProperties(&values);
        writer.setNodeIDs(&nodeIDs);

        rows.clear();
        while (writer.isValid()) {
            writer.fill(chunkSize);

            ASSERT_EQ(values.size(), nodeIDs.size());
            ASSERT_LE(values.size(), chunkSize);

            for (size_t row = 0; row < values.size(); row++) {
                rows.emplace_back(nodeIDs[row].getValue(), values[row]);
            }
        }

        std::sort(rows.begin(), rows.end());
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    PropertyTypeID _valueID;
    LabelID _rowLabel;
    LabelID _evenLabel;
};

TEST_F(ScanNodePropertiesByLabelTest, rangeYieldsOneCurrentValuePerNode) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    const LabelSet row = LabelSet::fromList({_rowLabel});

    std::vector<NodeValue> found;
    scanRange(view, LabelSetHandle(row), found);

    const std::vector<NodeValue> expected {
        {0, 0},
        {1, 10},
        {2, 999},
        {3, 30},
        {5, 555},
        {6, 60},
        {7, 70},
        {8, 80},
        {9, 90},
        {10, 100},
        {11, 110},
    };

    EXPECT_EQ(found, expected);
}

TEST_F(ScanNodePropertiesByLabelTest, chunkWriterYieldsOneCurrentValuePerNode) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    const LabelSet row = LabelSet::fromList({_rowLabel});

    const std::vector<NodeValue> expected {
        {0, 0},
        {1, 10},
        {2, 999},
        {3, 30},
        {5, 555},
        {6, 60},
        {7, 70},
        {8, 80},
        {9, 90},
        {10, 100},
        {11, 110},
    };

    for (const size_t chunkSize : {size_t {1}, size_t {2}, size_t {5}, size_t {64}}) {
        std::vector<NodeValue> found;
        scanChunked(view, LabelSetHandle(row), chunkSize, found);

        EXPECT_EQ(found, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ScanNodePropertiesByLabelTest, nulledNodeIsNotEmittedWithItsOlderValue) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    const LabelSet row = LabelSet::fromList({_rowLabel});

    std::vector<NodeValue> found;
    scanRange(view, LabelSetHandle(row), found);

    const auto isNulledNode = [](const NodeValue& value) { return value.first == thirdPartNulls.front(); };
    EXPECT_TRUE(std::none_of(found.begin(), found.end(), isNulledNode));
}

TEST_F(ScanNodePropertiesByLabelTest, labelRestrictedScanResolvesAcrossParts) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    const LabelSet even = LabelSet::fromList({_evenLabel});

    const std::vector<NodeValue> expected {
        {0, 0},
        {2, 999},
        {6, 60},
        {8, 80},
        {10, 100},
    };

    std::vector<NodeValue> found;
    scanRange(view, LabelSetHandle(even), found);
    EXPECT_EQ(found, expected);

    scanChunked(view, LabelSetHandle(even), 3, found);
    EXPECT_EQ(found, expected);
}
