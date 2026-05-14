#include <gtest/gtest.h>

#include "embedding/EmbeddingBucket.h"

using namespace db;

TEST(EmbeddingBucketTest, AllocAndRetrieve) {
    EmbeddingBucket bucket(3);

    const float data1[] = {1.0f, 2.0f, 3.0f};
    const float data2[] = {4.0f, 5.0f, 6.0f};

    const auto v1 = bucket.alloc(data1);
    const auto v2 = bucket.alloc(data2);

    ASSERT_EQ(v1.size(), 3);
    ASSERT_EQ(v1[0], 1.0f);
    ASSERT_EQ(v1[1], 2.0f);
    ASSERT_EQ(v1[2], 3.0f);

    ASSERT_EQ(v2.size(), 3);
    ASSERT_EQ(v2[0], 4.0f);
    ASSERT_EQ(v2[1], 5.0f);
    ASSERT_EQ(v2[2], 6.0f);

    ASSERT_EQ(bucket.getEmbeddingCount(), 2);

    const auto s = bucket.span();
    ASSERT_EQ(s.size(), 6);
    ASSERT_EQ(s[0], 1.0f);
    ASSERT_EQ(s[3], 4.0f);
    ASSERT_EQ(s[5], 6.0f);
}

TEST(EmbeddingBucketTest, Capacity) {
    EmbeddingBucket bucket(128);

    const size_t capacity = bucket.getAvailCount();
    ASSERT_GE(capacity, EmbeddingBucket::MIN_EMBEDDINGS);
    ASSERT_GE(capacity * 128 * sizeof(float), EmbeddingBucket::MIN_BUCKET_BYTES);
}

TEST(EmbeddingBucketTest, FillToCapacity) {
    EmbeddingBucket bucket(2);

    const size_t capacity = bucket.getAvailCount();
    std::vector<float> data = {1.0f, 2.0f};

    for (size_t i = 0; i < capacity; i++) {
        bucket.alloc(data);
    }

    ASSERT_EQ(bucket.getAvailCount(), 0);
    ASSERT_EQ(bucket.getEmbeddingCount(), capacity);

    const auto s = bucket.span();
    ASSERT_EQ(s.size(), capacity * 2);
    ASSERT_EQ(s[0], 1.0f);
    ASSERT_EQ(s[1], 2.0f);
    ASSERT_EQ(s[s.size() - 2], 1.0f);
    ASSERT_EQ(s[s.size() - 1], 2.0f);
}

TEST(EmbeddingBucketTest, MoveConstructor) {
    EmbeddingBucket bucket(2);

    const float data1[] = {1.0f, 2.0f};
    const float data2[] = {3.0f, 4.0f};
    bucket.alloc(data1);
    bucket.alloc(data2);

    EmbeddingBucket moved(std::move(bucket));

    ASSERT_EQ(moved.getEmbeddingCount(), 2);
    ASSERT_EQ(moved.getDimension(), 2);

    const auto s = moved.span();
    ASSERT_EQ(s.size(), 4);
    ASSERT_EQ(s[0], 1.0f);
    ASSERT_EQ(s[1], 2.0f);
    ASSERT_EQ(s[2], 3.0f);
    ASSERT_EQ(s[3], 4.0f);
}
