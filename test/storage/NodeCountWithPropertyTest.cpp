#include <gtest/gtest.h>

#include <stdint.h>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
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

// A part lays its nodes out grouped by label set, so a node's final ID is not the order it
// was added in. The rewrites below name final IDs, and every expectation is read back from
// the graph rather than derived from creation order.
constexpr uint64_t twiceRewrittenNode = 2;
constexpr uint64_t rewrittenNode = 5;
constexpr uint64_t nulledNode = 4;

constexpr int64_t secondPartValue = 222;
constexpr int64_t thirdPartValue = 999;
constexpr int64_t rewrittenValue = 555;

}

class NodeCountWithPropertyTest : public TuringTest {
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

    // How many nodes of the label set the per-node lookup resolves a value for: the newest
    // part carrying the property decides, and a node nulled there holds no value at all.
    size_t expectedCount(const GraphView& view, const LabelSetHandle& labelset) const {
        const GraphReader reader(view);

        size_t count = 0;
        for (uint64_t node = 0; node < nodeCount; node++) {
            if (!reader.getNodeLabelSet(NodeID {node}).hasAtLeastLabels(labelset)) {
                continue;
            }

            const std::optional<const int64_t*> value =
                reader.tryGetNodeProperty<types::Int64>(_valueID, NodeID {node});
            const bool explicitNull = !value.has_value();

            if (explicitNull || !value.value()) {
                continue;
            }

            count++;
        }

        return count;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    PropertyTypeID _valueID;
    LabelID _rowLabel;
    LabelID _evenLabel;
};

TEST_F(NodeCountWithPropertyTest, countsEveryNodeThatHoldsTheProperty) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    const LabelSet row = LabelSet::fromList({_rowLabel});
    const LabelSetHandle rowHandle(row);

    EXPECT_EQ(expectedCount(view, rowHandle), nodeCount - 1);
    EXPECT_EQ(reader.getNodeCountWithProperty(rowHandle, _valueID), nodeCount - 1);
}

TEST_F(NodeCountWithPropertyTest, nulledNodeIsNotCounted) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    const LabelSet row = LabelSet::fromList({_rowLabel});
    const LabelSetHandle rowHandle(row);

    const std::optional<const int64_t*> nulled =
        reader.tryGetNodeProperty<types::Int64>(_valueID, NodeID {nulledNode});
    ASSERT_FALSE(nulled.has_value());

    EXPECT_EQ(reader.getNodeCountWithProperty(rowHandle, _valueID), nodeCount - 1);
}

TEST_F(NodeCountWithPropertyTest, rewrittenNodeIsCountedOnce) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    const LabelSet row = LabelSet::fromList({_rowLabel});
    const LabelSetHandle rowHandle(row);

    const std::optional<const int64_t*> rewritten =
        reader.tryGetNodeProperty<types::Int64>(_valueID, NodeID {twiceRewrittenNode});
    ASSERT_TRUE(rewritten.has_value());
    ASSERT_NE(rewritten.value(), nullptr);
    EXPECT_EQ(*rewritten.value(), thirdPartValue);

    EXPECT_EQ(reader.getNodeCountWithProperty(rowHandle, _valueID), nodeCount - 1);
}

TEST_F(NodeCountWithPropertyTest, countIsRestrictedToTheLabelSet) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    const LabelSet even = LabelSet::fromList({_evenLabel});
    const LabelSetHandle evenHandle(even);

    const size_t expected = expectedCount(view, evenHandle);

    EXPECT_LT(expected, nodeCount - 1);
    EXPECT_EQ(reader.getNodeCountWithProperty(evenHandle, _valueID), expected);
}
