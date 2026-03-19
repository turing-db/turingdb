#include <gtest/gtest.h>

#include "TuringTest.h"

#include "Graph.h"
#include "JobSystem.h"
#include "properties/PropertyContainer.h"
#include "EmbeddingBucket.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "iterators/GetPropertiesIterator.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

using namespace db;
using namespace turing::test;

TEST(EmbeddingPropertyContainerTest, EmptyContainer) {
    TypedPropertyContainer<types::Embedding> container(3);

    ASSERT_EQ(container.size(), 0);
    ASSERT_EQ(container.getValueType(), ValueType::Embedding);
    ASSERT_EQ(container.all().size(), 0);
    ASSERT_FALSE(container.has(EntityID(0)));
    ASSERT_EQ(container.tryGet(EntityID(0)), nullptr);
}

TEST(EmbeddingPropertyContainerTest, AddAndGet) {
    TypedPropertyContainer<types::Embedding> container(3);

    const float data1[] = {1.0f, 2.0f, 3.0f};
    const float data2[] = {4.0f, 5.0f, 6.0f};

    container.add(EntityID(10), data1);
    container.add(EntityID(20), data2);

    ASSERT_EQ(container.size(), 2);
    ASSERT_TRUE(container.has(EntityID(10)));
    ASSERT_TRUE(container.has(EntityID(20)));
    ASSERT_FALSE(container.has(EntityID(30)));

    const auto v1 = container.get(EntityID(10));
    ASSERT_EQ(v1.size(), 3);
    ASSERT_EQ(v1[0], 1.0f);
    ASSERT_EQ(v1[1], 2.0f);
    ASSERT_EQ(v1[2], 3.0f);

    const auto v2 = container.get(EntityID(20));
    ASSERT_EQ(v2[0], 4.0f);
    ASSERT_EQ(v2[1], 5.0f);
    ASSERT_EQ(v2[2], 6.0f);
}

TEST(EmbeddingPropertyContainerTest, TryGet) {
    TypedPropertyContainer<types::Embedding> container(2);

    const float data[] = {1.0f, 2.0f};
    container.add(EntityID(5), data);

    const auto* found = container.tryGet(EntityID(5));
    ASSERT_NE(found, nullptr);
    ASSERT_EQ((*found)[0], 1.0f);
    ASSERT_EQ((*found)[1], 2.0f);

    const auto* notFound = container.tryGet(EntityID(99));
    ASSERT_EQ(notFound, nullptr);
}

TEST(EmbeddingPropertyContainerTest, AllAndGetSpan) {
    TypedPropertyContainer<types::Embedding> container(2);

    const float data1[] = {1.0f, 2.0f};
    const float data2[] = {3.0f, 4.0f};
    const float data3[] = {5.0f, 6.0f};

    container.add(EntityID(0), data1);
    container.add(EntityID(1), data2);
    container.add(EntityID(2), data3);

    const auto allViews = container.all();
    ASSERT_EQ(allViews.size(), 3);
    ASSERT_EQ(allViews[0][0], 1.0f);
    ASSERT_EQ(allViews[1][0], 3.0f);
    ASSERT_EQ(allViews[2][0], 5.0f);

    const auto span = container.getSpan(1, 2);
    ASSERT_EQ(span.size(), 2);
    ASSERT_EQ(span[0][0], 3.0f);
    ASSERT_EQ(span[1][0], 5.0f);
}

TEST(EmbeddingPropertyContainerTest, MultipleBuckets) {
    const size_t dimension = 4;
    TypedPropertyContainer<types::Embedding> container(dimension);

    // Ensure we exceed a single bucket capacity
    EmbeddingBucket probe(dimension);
    const size_t bucketCapacity = probe.getAvailCount();
    const size_t count = bucketCapacity * 2 + 100;

    std::vector<float> embedding(dimension);
    for (size_t i = 0; i < count; i++) {
        for (size_t d = 0; d < dimension; d++) {
            embedding[d] = static_cast<float>(i * dimension + d);
        }
        container.add(EntityID(i), embedding);
    }

    ASSERT_EQ(container.size(), count);

    // Verify every embedding via get by EntityID
    for (size_t i = 0; i < count; i++) {
        ASSERT_TRUE(container.has(EntityID(i)));

        const auto view = container.get(EntityID(i));
        ASSERT_EQ(view.size(), dimension);
        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
        }
    }

    // Verify via get by offset
    for (size_t i = 0; i < count; i++) {
        const auto view = container.get(i);
        ASSERT_EQ(view.size(), dimension);
        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
        }
    }

    // Verify via all()
    const auto allViews = container.all();
    ASSERT_EQ(allViews.size(), count);
    for (size_t i = 0; i < count; i++) {
        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(allViews[i][d], static_cast<float>(i * dimension + d));
        }
    }

    // Verify via getSpan() across bucket boundary
    const size_t spanStart = bucketCapacity - 50;
    const size_t spanCount = 100;
    const auto span = container.getSpan(spanStart, spanCount);
    ASSERT_EQ(span.size(), spanCount);
    for (size_t i = 0; i < spanCount; i++) {
        const size_t globalIdx = spanStart + i;
        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(span[i][d], static_cast<float>(globalIdx * dimension + d));
        }
    }
}

TEST(EmbeddingPropertyContainerTest, SortMultipleBuckets) {
    const size_t dimension = 4;
    EmbeddingBucket probe(dimension);
    const size_t count = probe.getAvailCount() + 1;

    TypedPropertyContainer<types::Embedding> container(dimension);

    std::vector<float> embedding(dimension);
    for (size_t i = 0; i < count; i++) {
        for (size_t d = 0; d < dimension; d++) {
            embedding[d] = static_cast<float>(i * dimension + d);
        }
        container.add(EntityID(i), embedding);
    }

    container.sort();

    for (size_t i = 0; i < count; i++) {
        const auto view = container.get(EntityID(i));
        ASSERT_EQ(view.size(), dimension);
        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
        }
    }
}

TEST(EmbeddingPropertyContainerTest, Sort) {
    TypedPropertyContainer<types::Embedding> container(2);

    const float data1[] = {5.0f, 6.0f};
    const float data2[] = {1.0f, 2.0f};
    const float data3[] = {3.0f, 4.0f};

    container.add(EntityID(30), data1);
    container.add(EntityID(10), data2);
    container.add(EntityID(20), data3);

    container.sort();

    // After sort, IDs should be in ascending order
    const auto& ids = container.ids();
    ASSERT_EQ(ids[0], EntityID(10));
    ASSERT_EQ(ids[1], EntityID(20));
    ASSERT_EQ(ids[2], EntityID(30));

    // Values should follow the sorted ID order
    const auto v0 = container.get(size_t(0));
    ASSERT_EQ(v0[0], 1.0f);
    ASSERT_EQ(v0[1], 2.0f);

    const auto v1 = container.get(size_t(1));
    ASSERT_EQ(v1[0], 3.0f);
    ASSERT_EQ(v1[1], 4.0f);

    const auto v2 = container.get(size_t(2));
    ASSERT_EQ(v2[0], 5.0f);
    ASSERT_EQ(v2[1], 6.0f);

    // Lookup by EntityID should still work
    const auto byId = container.get(EntityID(20));
    ASSERT_EQ(byId[0], 3.0f);
    ASSERT_EQ(byId[1], 4.0f);
}

class EmbeddingGraphTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = JobSystem::create();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(EmbeddingGraphTest, GetPropertiesIterator) {
    const size_t dimension = 4;

    EmbeddingBucket probe(dimension);
    const size_t bucketCapacity = probe.getAvailCount();
    const size_t count = bucketCapacity * 2 + 100;

    auto graph = Graph::create();
    GraphWriter writer(graph.get());

    // Create nodes with embedding and a unique index property to correlate after remapping
    std::vector<float> embedding(dimension);
    for (size_t i = 0; i < count; i++) {
        const NodeID node = writer.addNode({"Node"});

        for (size_t d = 0; d < dimension; d++) {
            embedding[d] = static_cast<float>(i * dimension + d);
        }
        writer.addNodeProperty<types::Embedding>(node, "vec", std::span<const float>(embedding));
        writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));
    }

    writer.submit();

    const FrozenCommitTx tx = graph->openTransaction();
    const GraphReader reader = tx.readGraph();

    const auto& propTypes = tx.viewGraph().metadata().propTypes();
    const auto vecType = propTypes.get("vec");
    const auto idxType = propTypes.get("idx");
    ASSERT_TRUE(vecType.has_value());
    ASSERT_TRUE(idxType.has_value());
    ASSERT_EQ(vecType->_valueType, ValueType::Embedding);

    ColumnNodeIDs nodeIDs;
    for (size_t i = 0; i < count; i++) {
        nodeIDs.push_back(NodeID(i));
    }

    // Iterate embeddings and verify against the idx property
    const auto vecRange = reader.getNodeProperties<types::Embedding>(vecType->_id, &nodeIDs);

    size_t total = 0;
    for (auto it = vecRange.begin(); it.isValid(); it.next()) {
        const NodeID nodeID = it.getCurrentEntityID();
        const auto view = it.get();
        ASSERT_EQ(view.size(), dimension);

        // Look up the original loop index via the idx property
        const auto* origIdx = reader.tryGetNodeProperty<types::Int64>(idxType->_id, nodeID);
        ASSERT_NE(origIdx, nullptr);
        const size_t i = static_cast<size_t>(*origIdx);

        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
        }
        total++;
    }

    ASSERT_EQ(total, count);
}

TEST_F(EmbeddingGraphTest, GetPropertiesWithNullIterator) {
    const size_t dimension = 4;
    const size_t count = 1000;

    auto graph = Graph::create();
    GraphWriter writer(graph.get());

    // Create nodes, only add embeddings to even-numbered loop iterations
    // Use idx property to correlate after remapping
    std::vector<float> embedding(dimension);
    for (size_t i = 0; i < count; i++) {
        const NodeID node = writer.addNode({"Node"});
        writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));

        if (i % 2 == 0) {
            for (size_t d = 0; d < dimension; d++) {
                embedding[d] = static_cast<float>(i * dimension + d);
            }
            writer.addNodeProperty<types::Embedding>(node, "vec", std::span<const float>(embedding));
        }
    }

    writer.submit();

    const FrozenCommitTx tx = graph->openTransaction();
    const GraphReader reader = tx.readGraph();

    const auto& propTypes = tx.viewGraph().metadata().propTypes();
    const auto vecType = propTypes.get("vec");
    const auto idxType = propTypes.get("idx");
    ASSERT_TRUE(vecType.has_value());
    ASSERT_TRUE(idxType.has_value());

    ColumnNodeIDs nodeIDs;
    for (size_t i = 0; i < count; i++) {
        nodeIDs.push_back(NodeID(i));
    }

    const auto range = reader.getNodePropertiesWithNull<types::Embedding>(vecType->_id, &nodeIDs);

    size_t total = 0;
    for (auto it = range.begin(); it.isValid(); it.next()) {
        const NodeID nodeID = it.getCurrentID();
        const auto value = it.get();

        const auto* origIdx = reader.tryGetNodeProperty<types::Int64>(idxType->_id, nodeID);
        ASSERT_NE(origIdx, nullptr);
        const size_t i = static_cast<size_t>(*origIdx);

        if (i % 2 == 0) {
            ASSERT_TRUE(value.has_value());
            const auto view = value.value();
            ASSERT_EQ(view.size(), dimension);
            for (size_t d = 0; d < dimension; d++) {
                ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
            }
        } else {
            ASSERT_FALSE(value.has_value());
        }

        total++;
    }

    ASSERT_EQ(total, count);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
