#include <gtest/gtest.h>

#include <stdint.h>
#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "datapart/EdgeRecord.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/EdgeView.h"
#include "views/EntityPropertyView.h"
#include "views/GraphView.h"
#include "views/NodeView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr uint64_t nodeCount = 4;
constexpr uint64_t edgeCount = 4;

// One entity keeps its first value, one is rewritten in the second part, one is nulled in
// the second part, and one is rewritten in the second part then nulled in the third.
constexpr uint64_t keptEntity = 0;
constexpr uint64_t rewrittenEntity = 1;
constexpr uint64_t nulledEntity = 2;
constexpr uint64_t rewrittenThenNulledEntity = 3;

constexpr int64_t firstValue = 10;
constexpr int64_t secondValue = 222;

}

class EntityPropertyViewTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        writeBasePart();
        writePatchPart({rewrittenEntity, rewrittenThenNulledEntity}, {nulledEntity});
        writePatchPart({}, {rewrittenThenNulledEntity});
    }

    void declare(MetadataBuilder& metadata) {
        _rowLabel = metadata.getOrCreateLabel("Row");
        _edgeType = metadata.getOrCreateEdgeType("KNOWS");
        _valueID = metadata.getOrCreatePropertyType("value", ValueType::Int64)._id;
    }

    void writeBasePart() {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        declare(builder.getMetadata());

        for (uint64_t node = 0; node < nodeCount; node++) {
            LabelSet labelset;
            labelset.set(_rowLabel);

            const NodeID nodeID = builder.addNode(labelset);
            builder.addNodeProperty<types::Int64>(nodeID, _valueID, firstValue);
        }

        for (uint64_t edge = 0; edge < edgeCount; edge++) {
            const NodeID source {edge % nodeCount};
            const NodeID target {(edge + 1) % nodeCount};
            const EdgeRecord& record = builder.addEdge(_edgeType, source, target);

            builder.addEdgeProperty<types::Int64>(record, _valueID, firstValue);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    // Patching a property onto an entity of an older part is what the write buffer does
    // for a SET on a node or edge the change only matched. An edge needs its record.
    void writePatchPart(const std::vector<uint64_t>& rewrites, const std::vector<uint64_t>& nulls) {
        std::vector<EdgeRecord> rewrittenEdges;
        std::vector<EdgeRecord> nulledEdges;

        {
            const FrozenCommitTx transaction = _graph->openTransaction();
            const GraphReader reader = transaction.readGraph();

            for (const uint64_t edge : rewrites) {
                const EdgeRecord* record = reader.getEdge(EdgeID {edge});
                ASSERT_NE(record, nullptr);
                rewrittenEdges.push_back(*record);
            }

            for (const uint64_t edge : nulls) {
                const EdgeRecord* record = reader.getEdge(EdgeID {edge});
                ASSERT_NE(record, nullptr);
                nulledEdges.push_back(*record);
            }
        }

        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        declare(builder.getMetadata());

        for (const uint64_t node : rewrites) {
            builder.addNodeProperty<types::Int64>(NodeID {node}, _valueID, secondValue);
        }

        for (const uint64_t node : nulls) {
            builder.addNodeProperty<types::Int64>(NodeID {node}, _valueID, std::nullopt);
        }

        for (const EdgeRecord& record : rewrittenEdges) {
            builder.addEdgeProperty<types::Int64>(record, _valueID, secondValue);
        }

        for (const EdgeRecord& record : nulledEdges) {
            builder.addEdgeProperty<types::Int64>(record, _valueID, std::nullopt);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    PropertyTypeID _valueID;
    EdgeTypeID _edgeType;
    LabelID _rowLabel;
};

TEST_F(EntityPropertyViewTest, nodeViewHoldsWhatTheLookupResolves) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    for (uint64_t node = 0; node < nodeCount; node++) {
        const std::optional<const int64_t*> resolved =
            reader.tryGetNodeProperty<types::Int64>(_valueID, NodeID {node});
        const bool holdsValue = resolved.has_value() && resolved.value();

        const NodeView nodeView = reader.getNodeView(NodeID {node});
        const EntityPropertyView& properties = nodeView.properties();

        ASSERT_TRUE(nodeView.isValid()) << "node " << node;
        EXPECT_EQ(properties.hasProperty(_valueID), holdsValue) << "node " << node;

        const int64_t* inView = properties.tryGetProperty<types::Int64>(_valueID);
        if (!holdsValue) {
            EXPECT_EQ(inView, nullptr) << "node " << node;
            continue;
        }

        ASSERT_NE(inView, nullptr) << "node " << node;
        EXPECT_EQ(*inView, *resolved.value()) << "node " << node;
    }
}

TEST_F(EntityPropertyViewTest, edgeViewHoldsWhatTheLookupResolves) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    for (uint64_t edge = 0; edge < edgeCount; edge++) {
        const std::optional<const int64_t*> resolved =
            reader.tryGetEdgeProperty<types::Int64>(_valueID, EdgeID {edge});
        const bool holdsValue = resolved.has_value() && resolved.value();

        const EdgeView edgeView = reader.getEdgeView(EdgeID {edge});
        const EntityPropertyView& properties = edgeView.properties();

        ASSERT_TRUE(edgeView.isValid()) << "edge " << edge;
        EXPECT_EQ(properties.hasProperty(_valueID), holdsValue) << "edge " << edge;

        const int64_t* inView = properties.tryGetProperty<types::Int64>(_valueID);
        if (!holdsValue) {
            EXPECT_EQ(inView, nullptr) << "edge " << edge;
            continue;
        }

        ASSERT_NE(inView, nullptr) << "edge " << edge;
        EXPECT_EQ(*inView, *resolved.value()) << "edge " << edge;
    }
}

TEST_F(EntityPropertyViewTest, nulledPropertyIsNotInTheView) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader(transaction.viewGraph());

    for (const uint64_t entity : {nulledEntity, rewrittenThenNulledEntity}) {
        const NodeView nodeView = reader.getNodeView(NodeID {entity});
        EXPECT_FALSE(nodeView.properties().hasProperty(_valueID)) << "node " << entity;
        EXPECT_EQ(nodeView.properties().getCount(), 0) << "node " << entity;

        const EdgeView edgeView = reader.getEdgeView(EdgeID {entity});
        EXPECT_FALSE(edgeView.properties().hasProperty(_valueID)) << "edge " << entity;
        EXPECT_EQ(edgeView.properties().getCount(), 0) << "edge " << entity;
    }
}

TEST_F(EntityPropertyViewTest, rewrittenPropertyIsHeldOnceAtItsNewestValue) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader(transaction.viewGraph());

    const NodeView nodeView = reader.getNodeView(NodeID {rewrittenEntity});
    const EntityPropertyView& nodeProperties = nodeView.properties();

    EXPECT_EQ(nodeProperties.getCount(), 1);
    EXPECT_EQ(nodeProperties.getProperty<types::Int64>(_valueID), secondValue);

    const EdgeView edgeView = reader.getEdgeView(EdgeID {rewrittenEntity});
    const EntityPropertyView& edgeProperties = edgeView.properties();

    EXPECT_EQ(edgeProperties.getCount(), 1);
    EXPECT_EQ(edgeProperties.getProperty<types::Int64>(_valueID), secondValue);
}

TEST_F(EntityPropertyViewTest, untouchedPropertyStandsAtItsFirstValue) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader(transaction.viewGraph());

    const NodeView nodeView = reader.getNodeView(NodeID {keptEntity});
    EXPECT_EQ(nodeView.properties().getCount(), 1);
    EXPECT_EQ(nodeView.properties().getProperty<types::Int64>(_valueID), firstValue);

    const EdgeView edgeView = reader.getEdgeView(EdgeID {keptEntity});
    EXPECT_EQ(edgeView.properties().getCount(), 1);
    EXPECT_EQ(edgeView.properties().getProperty<types::Int64>(_valueID), firstValue);
}
