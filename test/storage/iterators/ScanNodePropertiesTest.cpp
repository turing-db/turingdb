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
#include "iterators/ScanNodePropertiesIterator.h"
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

// The first part writes nodes in creation order, but a part lays its nodes out grouped by
// label set, so a node's final ID is not the order it was added in. The rewrites below
// name final IDs, and every expectation is read back from the graph rather than derived
// from creation order.
constexpr uint64_t twiceRewrittenNode = 2;
constexpr uint64_t rewrittenNode = 5;
constexpr uint64_t nulledNode = 4;

constexpr int64_t secondPartValue = 222;
constexpr int64_t thirdPartValue = 999;
constexpr int64_t rewrittenValue = 555;

}

class ScanNodePropertiesTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        writePart(0, nodeCount, {}, {});
        writePart(nodeCount, 0, {{twiceRewrittenNode, secondPartValue}, {rewrittenNode, rewrittenValue}}, {});
        writePart(nodeCount, 0, {{twiceRewrittenNode, thirdPartValue}}, {nulledNode});
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

    // What the per-node lookup resolves for every node: the newest part carrying the
    // property decides, and a node nulled there holds no value at all.
    void expectedRows(const GraphView& view, std::vector<NodeValue>& rows) {
        const GraphReader reader(view);

        rows.clear();
        for (uint64_t node = 0; node < nodeCount; node++) {
            const std::optional<const int64_t*> value =
                reader.tryGetNodeProperty<types::Int64>(_valueID, NodeID {node});
            const bool explicitNull = !value.has_value();

            if (explicitNull || !value.value()) {
                continue;
            }

            rows.emplace_back(node, *value.value());
        }
    }

    void scanRange(const GraphView& view, std::vector<NodeValue>& rows) {
        const GraphReader reader(view);
        const auto range = reader.scanNodeProperties<types::Int64>(_valueID);

        rows.clear();
        for (auto it = range.begin(); it.isValid(); it.next()) {
            rows.emplace_back(it.getCurrentNodeID().getValue(), it.get());
        }

        std::sort(rows.begin(), rows.end());
    }

    void scanByLabel(const GraphView& view, const LabelSetHandle& labelset, std::vector<NodeValue>& rows) {
        const GraphReader reader(view);
        const auto range = reader.scanNodePropertiesByLabel<types::Int64>(_valueID, labelset);

        rows.clear();
        for (auto it = range.begin(); it.isValid(); it.next()) {
            rows.emplace_back(it.getCurrentNodeID().getValue(), it.get());
        }

        std::sort(rows.begin(), rows.end());
    }

    void scanChunked(const GraphView& view, size_t chunkSize, std::vector<NodeValue>& rows) {
        ColumnVector<types::Int64::Primitive> values;
        ColumnNodeIDs nodeIDs;

        ScanNodePropertiesChunkWriter<types::Int64> writer(view, _valueID);
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

    static bool holds(const std::vector<NodeValue>& rows, uint64_t node, int64_t value) {
        return std::find(rows.begin(), rows.end(), NodeValue {node, value}) != rows.end();
    }

    static bool mentions(const std::vector<NodeValue>& rows, uint64_t node) {
        const auto isNode = [node](const NodeValue& row) { return row.first == node; };
        return std::any_of(rows.begin(), rows.end(), isNode);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    PropertyTypeID _valueID;
    LabelID _rowLabel;
    LabelID _evenLabel;
};

TEST_F(ScanNodePropertiesTest, expectationsPinTheRewritesAndTheNull) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<NodeValue> expected;
    expectedRows(view, expected);

    EXPECT_EQ(expected.size(), nodeCount - 1);
    EXPECT_TRUE(holds(expected, twiceRewrittenNode, thirdPartValue));
    EXPECT_TRUE(holds(expected, rewrittenNode, rewrittenValue));
    EXPECT_FALSE(mentions(expected, nulledNode));
}

TEST_F(ScanNodePropertiesTest, rangeYieldsOneCurrentValuePerNode) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<NodeValue> expected;
    expectedRows(view, expected);

    std::vector<NodeValue> found;
    scanRange(view, found);

    EXPECT_EQ(found, expected);
}

TEST_F(ScanNodePropertiesTest, chunkWriterYieldsOneCurrentValuePerNode) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<NodeValue> expected;
    expectedRows(view, expected);

    for (const size_t chunkSize : {size_t {1}, size_t {2}, size_t {5}, size_t {64}}) {
        std::vector<NodeValue> found;
        scanChunked(view, chunkSize, found);

        EXPECT_EQ(found, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ScanNodePropertiesTest, nulledNodeIsNotEmittedWithItsOlderValue) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<NodeValue> found;
    scanRange(view, found);

    EXPECT_FALSE(mentions(found, nulledNode));
}

TEST_F(ScanNodePropertiesTest, rewrittenNodeIsNotEmittedTwice) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<NodeValue> found;
    scanRange(view, found);

    const auto isRewritten = [](const NodeValue& row) { return row.first == twiceRewrittenNode; };
    EXPECT_EQ(std::count_if(found.begin(), found.end(), isRewritten), 1);
    EXPECT_TRUE(holds(found, twiceRewrittenNode, thirdPartValue));
}

TEST_F(ScanNodePropertiesTest, everyNodeIsLabelledAndTheTwoScansMustAgree) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    const LabelSet row = LabelSet::fromList({_rowLabel});
    const LabelSetHandle rowHandle(row);

    for (uint64_t node = 0; node < nodeCount; node++) {
        EXPECT_TRUE(reader.getNodeLabelSet(NodeID {node}).hasAtLeastLabels(rowHandle)) << "node " << node;
    }

    std::vector<NodeValue> byLabel;
    scanByLabel(view, rowHandle, byLabel);

    std::vector<NodeValue> unfiltered;
    scanRange(view, unfiltered);

    EXPECT_EQ(unfiltered, byLabel);
}
