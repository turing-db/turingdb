#include <gtest/gtest.h>

#include "SmallVector.h"

class SmallVectorTest : public ::testing::Test {
};

TEST_F(SmallVectorTest, PushBackClear) {
    SmallVector<int, 3> vec;
    vec.push_back(1);
    vec.push_back(2);
    vec.push_back(3);
    vec.push_back(4);
    vec.push_back(5);

    ASSERT_EQ(vec.size(), 5);
    ASSERT_EQ(vec[0], 1);
    ASSERT_EQ(vec[1], 2);
    ASSERT_EQ(vec[2], 3);
    ASSERT_EQ(vec[3], 4);
    ASSERT_EQ(vec[4], 5);

    vec.clear();
    ASSERT_EQ(vec.size(), 0);
}

TEST_F(SmallVectorTest, SmallCapacity) {
    SmallVector<int, 4> vec;
    vec.push_back(1);
    vec.push_back(2);

    ASSERT_EQ(vec.size(), 2);
    ASSERT_EQ(vec[0], 1);
    ASSERT_EQ(vec[1], 2);
}

TEST_F(SmallVectorTest, Empty) {
    SmallVector<int, 4> vec;
    ASSERT_TRUE(vec.empty());
    vec.push_back(1);
    ASSERT_FALSE(vec.empty());
    vec.clear();
    ASSERT_TRUE(vec.empty());
}

TEST_F(SmallVectorTest, Grow) {
    SmallVector<int, 4> vec;
    vec.push_back(1);
    vec.push_back(2);

    ASSERT_EQ(vec.size(), 2);
    ASSERT_EQ(vec[0], 1);
    ASSERT_EQ(vec[1], 2);

    vec.push_back(3);
    vec.push_back(4);
    vec.push_back(5);

    ASSERT_EQ(vec.size(), 5);
    ASSERT_EQ(vec[0], 1);
    ASSERT_EQ(vec[1], 2);
    ASSERT_EQ(vec[2], 3);
    ASSERT_EQ(vec[3], 4);
    ASSERT_EQ(vec[4], 5);

    vec.clear();
    ASSERT_EQ(vec.size(), 0);

    vec.push_back(6);
    vec.push_back(7);
    vec.push_back(8);

    ASSERT_EQ(vec.size(), 3);
    ASSERT_EQ(vec[0], 6);
    ASSERT_EQ(vec[1], 7);
    ASSERT_EQ(vec[2], 8);
    vec.push_back(9);
    vec.push_back(10);
    vec.push_back(11);

    ASSERT_EQ(vec.size(), 6);
    ASSERT_EQ(vec[0], 6);
    ASSERT_EQ(vec[1], 7);
    ASSERT_EQ(vec[2], 8);
    ASSERT_EQ(vec[3], 9);
    ASSERT_EQ(vec[4], 10);
    ASSERT_EQ(vec[5], 11);

    vec.clear();
    ASSERT_EQ(vec.size(), 0);
}

TEST_F(SmallVectorTest, Iterator) {
    constexpr size_t SMALL_CAPACITY = 4;
    std::vector<int> vec;
    SmallVector<int, SMALL_CAPACITY> smallVec;

    // Small
    for (size_t i = 0; i < SMALL_CAPACITY; ++i) {
        smallVec.push_back(i);
        vec.push_back(i);
    }

    ASSERT_EQ(smallVec.size(), vec.size());
    ASSERT_EQ(smallVec, vec);

    size_t i = 0;
    for (auto it = smallVec.begin(); it != smallVec.end(); ++it, ++i) {
        ASSERT_EQ(*it, vec[i]);
    }

    i = 0;
    for (int x : smallVec) {
        ASSERT_EQ(x, vec[i]);
        ++i;
    }
}

TEST_F(SmallVectorTest, Map) {
    constexpr size_t size = 100000;
    SmallVector<int, 100> smallVec;

    for (size_t i = 0; i < size; ++i) {
        smallVec.push_back(i);
    }

    std::vector<int> vec;
    smallVec.map([&vec](int x) {
        vec.push_back(x);
    });

    ASSERT_EQ(smallVec, vec);
}
