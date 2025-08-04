#include "gtest/gtest.h"

#include "metadata/LabelSetMap.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"

using namespace db;

class LabelSetMapTest : public ::testing::Test {
};

TEST_F(LabelSetMapTest, createOne) {
    LabelSetMap labelMap;

    LabelSet labelset = LabelSet::fromList({0});
    const auto handle = labelMap.getOrCreate(labelset);
    ASSERT_TRUE(handle.isValid());
    ASSERT_TRUE(handle.isStored());
    ASSERT_EQ(handle.getID(), 0);
    ASSERT_EQ(labelset, handle);
}

TEST_F(LabelSetMapTest, get) {
    LabelSetMap labelMap;

    const LabelSet labelset = LabelSet::fromList({0});
    labelMap.getOrCreate(labelset);
    const auto handle = labelMap.get(labelset);
    ASSERT_TRUE(handle);
    ASSERT_TRUE(handle);
}

TEST_F(LabelSetMapTest, different) {
    LabelSetMap labelMap;

    const auto labelset1 = LabelSet::fromList({1});
    const auto labelset2 = LabelSet::fromList({2});
    const auto handle1 = labelMap.getOrCreate(labelset1);
    const auto handle2 = labelMap.getOrCreate(labelset2);

    ASSERT_TRUE(handle1.isValid());
    ASSERT_TRUE(handle1.isStored());
    ASSERT_EQ(handle1.getID(), 0);
    ASSERT_EQ(labelset1, handle1);
    ASSERT_TRUE(handle2.isValid());
    ASSERT_TRUE(handle2.isStored());
    ASSERT_EQ(handle2.getID(), 1);
    ASSERT_EQ(labelset2, handle2);
}
