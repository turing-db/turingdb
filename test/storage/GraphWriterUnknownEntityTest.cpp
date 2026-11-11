#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "Graph.h"
#include "ID.h"
#include "JobSystem.h"
#include "datapart/EdgeRecord.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"
#include "writers/GraphWriter.h"
#include "writers/MetadataBuilder.h"

#include "FatalException.h"
#include "TuringException.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr uint64_t UNCREATED_OFFSET = 1000;
const std::vector<float> EMBEDDING {1.0f, 0.0f};

template <typename Write>
void expectUnknownEntity(Write write, std::string_view message) {
    try {
        write();
        FAIL() << "accepted, expected: " << message;
    } catch (const FatalException& e) {
        FAIL() << "internal error: " << e.what();
    } catch (const TuringException& e) {
        EXPECT_EQ(std::string_view {e.what()}, message);
    }
}

std::string unknownNode(NodeID node) {
    return fmt::format("Node {} does not exist", node.getValue());
}

std::string unknownEdge(EdgeID edge) {
    return fmt::format("Edge {} does not exist", edge.getValue());
}

}

// The graph holds two Person nodes, 0 and 1, and one KNOWS edge, 0, between them.
class GraphWriterUnknownEntityTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        GraphWriter writer(_graph.get(), _jobSystem.get());
        const NodeID first = writer.addNode({"Person"});
        const NodeID second = writer.addNode({"Person"});
        writer.addEdge("KNOWS", first, second);
        ASSERT_TRUE(writer.submit());
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    EdgeRecord getCommittedEdge(EdgeID edgeID) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const EdgeRecord* edge = reader.getEdge(edgeID);

        return edge ? *edge : EdgeRecord {};
    }

    size_t getNodeCount() const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        return transaction.readGraph().getNodeCount();
    }

    size_t getEdgeCount() const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        return transaction.readGraph().getEdgeCount();
    }

    bool hasPropertyType(std::string_view name) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphMetadata& metadata = reader.getMetadata();

        return metadata.propTypes().get(name).has_value();
    }

    bool hasEdgeType(std::string_view name) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphMetadata& metadata = reader.getMetadata();

        return metadata.edgeTypes().get(name).has_value();
    }

    std::optional<int64_t> getAge(NodeID node) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphMetadata& metadata = reader.getMetadata();

        const std::optional<PropertyType> age = metadata.propTypes().get("age");
        if (!age) {
            return std::nullopt;
        }

        const std::optional<const int64_t*> value = reader.tryGetNodeProperty<types::Int64>(age->_id, node);
        if (!value || !*value) {
            return std::nullopt;
        }

        return **value;
    }

    std::optional<int64_t> getSince(EdgeID edge) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphMetadata& metadata = reader.getMetadata();

        const std::optional<PropertyType> since = metadata.propTypes().get("since");
        if (!since) {
            return std::nullopt;
        }

        const std::optional<const int64_t*> value = reader.tryGetEdgeProperty<types::Int64>(since->_id, edge);
        if (!value || !*value) {
            return std::nullopt;
        }

        return **value;
    }

    size_t getAgeCount() const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphMetadata& metadata = reader.getMetadata();

        const std::optional<PropertyType> age = metadata.propTypes().get("age");

        return age ? reader.getNodePropertyCount(age->_id) : 0;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(GraphWriterUnknownEntityTest, rejectsAPropertyOfANodeTheWriterNeverCreated) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    const NodeID node = writer.addNode({"Person"});
    const NodeID uncreated {node.getValue() + UNCREATED_OFFSET};
    writer.addNodeProperty<types::Int64>(node, "age", 30);

    expectUnknownEntity([&] { writer.addNodeProperty<types::Int64>(uncreated, "age", 31); },
                        unknownNode(uncreated));

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getNodeCount(), 3);
    EXPECT_EQ(getAge(node), 30);
    EXPECT_EQ(getAgeCount(), 1);
}

TEST_F(GraphWriterUnknownEntityTest, rejectsAPropertyOfAnUncreatedNodeByTypeAndAsAnEmbedding) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    const NodeID node = writer.addNode({"Person"});
    const NodeID uncreated {node.getValue() + UNCREATED_OFFSET};
    const PropertyType age = writer.addPropertyType("age", ValueType::Int64);

    expectUnknownEntity([&] { writer.addNodeProperty<types::Int64>(uncreated, age, 31); },
                        unknownNode(uncreated));
    expectUnknownEntity([&] { writer.addNodeProperty<types::Embedding>(uncreated, "vec", std::span<const float> {EMBEDDING}); },
                        unknownNode(uncreated));

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getNodeCount(), 3);
    EXPECT_EQ(getAgeCount(), 0);
    EXPECT_FALSE(hasPropertyType("vec"));
}

TEST_F(GraphWriterUnknownEntityTest, rejectsAnEncodedListOrMapOfAnUncreatedEntity) {
    std::unique_ptr<Change> change = _graph->newChange();
    CommitBuilder* tip = change->access().getTip();
    DataPartBuilder& builder = tip->newBuilder();
    MetadataBuilder& metadata = builder.getMetadata();

    const NodeID uncreatedNode {UNCREATED_OFFSET};
    const EdgeRecord uncreatedEdge {._edgeID = EdgeID {UNCREATED_OFFSET},
                                    ._nodeID = NodeID {0},
                                    ._otherID = NodeID {1},
                                    ._edgeTypeID = EdgeTypeID {0}};
    const PropertyTypeID tags = metadata.getOrCreatePropertyType("tags", ValueType::List)._id;
    const PropertyTypeID card = metadata.getOrCreatePropertyType("card", ValueType::Map)._id;

    expectUnknownEntity([&] { builder.addNodeProperty<types::List>(uncreatedNode, tags, std::optional<types::List::OwningPrimitive> {}); },
                        unknownNode(uncreatedNode));
    expectUnknownEntity([&] { builder.addNodeProperty<types::Map>(uncreatedNode, card, std::optional<types::Map::OwningPrimitive> {}); },
                        unknownNode(uncreatedNode));
    expectUnknownEntity([&] { builder.addEdgeProperty<types::List>(uncreatedEdge, tags, std::optional<types::List::OwningPrimitive> {}); },
                        unknownEdge(uncreatedEdge._edgeID));
    expectUnknownEntity([&] { builder.addEdgeProperty<types::Map>(uncreatedEdge, card, std::optional<types::Map::OwningPrimitive> {}); },
                        unknownEdge(uncreatedEdge._edgeID));
}

TEST_F(GraphWriterUnknownEntityTest, rejectsAnEdgeFromOrToAnUncreatedNode) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    const NodeID node = writer.addNode({"Person"});
    const NodeID uncreated {node.getValue() + UNCREATED_OFFSET};

    expectUnknownEntity([&] { writer.addEdge("LIKES", uncreated, node); }, unknownNode(uncreated));
    expectUnknownEntity([&] { writer.addEdge("LIKES", node, uncreated); }, unknownNode(uncreated));
    expectUnknownEntity([&] { writer.addEdge("LIKES", NodeID {0}, uncreated); }, unknownNode(uncreated));

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getNodeCount(), 3);
    EXPECT_EQ(getEdgeCount(), 1);
    EXPECT_FALSE(hasEdgeType("LIKES"));
}

TEST_F(GraphWriterUnknownEntityTest, rejectsAPropertyOfAnEdgeTheWriterNeverCreated) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    const NodeID source = writer.addNode({"Person"});
    const NodeID target = writer.addNode({"Person"});
    const EdgeRecord edge = writer.addEdge("KNOWS", source, target);

    EdgeRecord uncreated = edge;
    uncreated._edgeID = EdgeID {edge._edgeID.getValue() + UNCREATED_OFFSET};

    expectUnknownEntity([&] { writer.addEdgeProperty<types::Int64>(uncreated, "since", 2020); },
                        unknownEdge(uncreated._edgeID));
    expectUnknownEntity([&] { writer.addEdgeProperty<types::Embedding>(uncreated, "vec", std::span<const float> {EMBEDDING}); },
                        unknownEdge(uncreated._edgeID));

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getEdgeCount(), 2);
    EXPECT_FALSE(hasPropertyType("since"));
    EXPECT_FALSE(hasPropertyType("vec"));
}

TEST_F(GraphWriterUnknownEntityTest, rejectsAPropertyOfAnEdgeWhoseSourceWasNeverCreated) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    EdgeRecord edge = getCommittedEdge(EdgeID {0});
    const NodeID uncreated {UNCREATED_OFFSET};
    edge._nodeID = uncreated;

    expectUnknownEntity([&] { writer.addEdgeProperty<types::Int64>(edge, "since", 2020); },
                        unknownNode(uncreated));

    ASSERT_TRUE(writer.submit());
    EXPECT_FALSE(hasPropertyType("since"));
}

TEST_F(GraphWriterUnknownEntityTest, rejectsDeletingAnEntityTheGraphNeverHeld) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    const NodeID uncreatedNode {UNCREATED_OFFSET};
    const EdgeID uncreatedEdge {UNCREATED_OFFSET};

    expectUnknownEntity([&] { writer.deleteNode(uncreatedNode); }, unknownNode(uncreatedNode));
    expectUnknownEntity([&] { writer.deleteNodes({NodeID {0}, uncreatedNode}); }, unknownNode(uncreatedNode));
    expectUnknownEntity([&] { writer.deleteEdge(uncreatedEdge); }, unknownEdge(uncreatedEdge));
    expectUnknownEntity([&] { writer.deleteEdges({EdgeID {0}, uncreatedEdge}); }, unknownEdge(uncreatedEdge));

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getNodeCount(), 2);
    EXPECT_EQ(getEdgeCount(), 1);
}

TEST_F(GraphWriterUnknownEntityTest, writesToTheEntitiesOfAnEarlierDataPart) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    const NodeID node = writer.addNode({"Person"});
    writer.addNodeProperty<types::Int64>(NodeID {0}, "age", 40);
    writer.addEdge("KNOWS", NodeID {1}, node);
    writer.addEdgeProperty<types::Int64>(getCommittedEdge(EdgeID {0}), "since", 2020);

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getNodeCount(), 3);
    EXPECT_EQ(getEdgeCount(), 2);
    EXPECT_EQ(getAge(NodeID {0}), 40);
    EXPECT_EQ(getSince(EdgeID {0}), 2020);
}

TEST_F(GraphWriterUnknownEntityTest, writesToTheEntitiesOfAnEarlierCommitOfTheSameChange) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    const NodeID node = writer.addNode({"Person"});
    const EdgeRecord edge = writer.addEdge("KNOWS", NodeID {0}, node);
    ASSERT_TRUE(writer.commit());

    writer.addNodeProperty<types::Int64>(node, "age", 50);
    writer.addEdgeProperty<types::Int64>(edge, "since", 2021);

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getAge(node), 50);
    EXPECT_EQ(getSince(edge._edgeID), 2021);
}

TEST_F(GraphWriterUnknownEntityTest, deletesTheEntitiesOfAnEarlierDataPart) {
    GraphWriter writer(_graph.get(), _jobSystem.get());

    writer.deleteEdge(EdgeID {0});
    writer.deleteNode(NodeID {1});

    ASSERT_TRUE(writer.submit());
    EXPECT_EQ(getNodeCount(), 1);
    EXPECT_EQ(getEdgeCount(), 0);
}

// The commit builds the nodes it creates in one data part and its updates in a later one,
// so an update of a node the commit creates names a node of the part before its own.
TEST_F(GraphWriterUnknownEntityTest, updatesANodeTheSameCommitCreates) {
    std::unique_ptr<Change> change = _graph->newChange();
    CommitBuilder* tip = change->access().getTip();
    MetadataBuilder& metadata = tip->metadata();
    CommitWriteBuffer& buffer = tip->writeBuffer();

    const LabelID person = metadata.getOrCreateLabel("Person");
    CommitWriteBuffer::PendingNode& pending = buffer.newPendingNode();
    pending.labelsetHandle = metadata.getOrCreateLabelSet(LabelSet::fromList({person}));

    const NodeID created {2};
    CommitWriteBuffer::UntypedProperty age {
        .propertyID = metadata.getOrCreatePropertyType("age", ValueType::Int64)._id,
        .value = std::optional<int64_t> {60},
    };
    buffer.addNodeUpdate(created, age);

    ASSERT_TRUE(change->access().submit(*_jobSystem));
    EXPECT_EQ(getNodeCount(), 3);
    EXPECT_EQ(getAge(created), 60);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
