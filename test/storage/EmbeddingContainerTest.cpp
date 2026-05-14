#include <gtest/gtest.h>

#include "embedding/EmbeddingContainer.h"

using namespace db;

TEST(EmbeddingContainerTest, AllocAndRetrieve) {
    EmbeddingContainer container(3);

    const float data1[] = {1.0f, 2.0f, 3.0f};
    const float data2[] = {4.0f, 5.0f, 6.0f};
    const float data3[] = {7.0f, 8.0f, 9.0f};

    container.alloc(data1);
    container.alloc(data2);
    container.alloc(data3);

    ASSERT_EQ(container.size(), 3);
    ASSERT_EQ(container.getDimension(), 3);

    const auto v1 = container.getView(0);
    const auto v2 = container.getView(1);
    const auto v3 = container.getView(2);

    ASSERT_EQ(v1.size(), 3);
    ASSERT_EQ(v1[0], 1.0f);
    ASSERT_EQ(v1[1], 2.0f);
    ASSERT_EQ(v1[2], 3.0f);

    ASSERT_EQ(v2[0], 4.0f);
    ASSERT_EQ(v2[1], 5.0f);
    ASSERT_EQ(v2[2], 6.0f);

    ASSERT_EQ(v3[0], 7.0f);
    ASSERT_EQ(v3[1], 8.0f);
    ASSERT_EQ(v3[2], 9.0f);
}

TEST(EmbeddingContainerTest, GetReturnsAllViews) {
    EmbeddingContainer container(2);

    const float data1[] = {1.0f, 2.0f};
    const float data2[] = {3.0f, 4.0f};

    container.alloc(data1);
    container.alloc(data2);

    const auto& views = container.get();
    ASSERT_EQ(views.size(), 2);
    ASSERT_EQ(views[0][0], 1.0f);
    ASSERT_EQ(views[0][1], 2.0f);
    ASSERT_EQ(views[1][0], 3.0f);
    ASSERT_EQ(views[1][1], 4.0f);
}

TEST(EmbeddingContainerTest, OverflowToBucket) {
    EmbeddingContainer container(2);

    const size_t count = EmbeddingBucket::MIN_EMBEDDINGS + 100;
    std::vector<float> data = {1.0f, 2.0f};

    for (size_t i = 0; i < count; i++) {
        data[0] = static_cast<float>(i);
        container.alloc(data);
    }

    ASSERT_EQ(container.size(), count);

    for (size_t i = 0; i < count; i++) {
        const auto view = container.getView(i);
        ASSERT_EQ(view.size(), 2);
        ASSERT_EQ(view[0], static_cast<float>(i));
        ASSERT_EQ(view[1], 2.0f);
    }
}

TEST(EmbeddingContainerTest, Clear) {
    EmbeddingContainer container(2);

    const float data[] = {1.0f, 2.0f};
    container.alloc(data);
    container.alloc(data);

    ASSERT_EQ(container.size(), 2);

    container.clear();

    ASSERT_EQ(container.size(), 0);
    ASSERT_EQ(container.get().size(), 0);
}

TEST(EmbeddingContainerTest, MoveConstructor) {
    EmbeddingContainer container(3);

    const float data1[] = {1.0f, 2.0f, 3.0f};
    const float data2[] = {4.0f, 5.0f, 6.0f};
    container.alloc(data1);
    container.alloc(data2);

    EmbeddingContainer moved(std::move(container));

    ASSERT_EQ(moved.size(), 2);
    ASSERT_EQ(moved.getDimension(), 3);

    const auto v1 = moved.getView(0);
    const auto v2 = moved.getView(1);

    ASSERT_EQ(v1[0], 1.0f);
    ASSERT_EQ(v1[1], 2.0f);
    ASSERT_EQ(v1[2], 3.0f);
    ASSERT_EQ(v2[0], 4.0f);
    ASSERT_EQ(v2[1], 5.0f);
    ASSERT_EQ(v2[2], 6.0f);
}

TEST(EmbeddingContainerTest, MoveAssignment) {
    EmbeddingContainer container(2);
    const float data1[] = {1.0f, 2.0f};
    container.alloc(data1);

    EmbeddingContainer other(2);
    const float data2[] = {3.0f, 4.0f};
    const float data3[] = {5.0f, 6.0f};
    other.alloc(data2);
    other.alloc(data3);

    container = std::move(other);

    ASSERT_EQ(container.size(), 2);
    ASSERT_EQ(container.getView(0)[0], 3.0f);
    ASSERT_EQ(container.getView(0)[1], 4.0f);
    ASSERT_EQ(container.getView(1)[0], 5.0f);
    ASSERT_EQ(container.getView(1)[1], 6.0f);
}
