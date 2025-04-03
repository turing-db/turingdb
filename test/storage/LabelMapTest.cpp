#include "gtest/gtest.h"

#include "labels/LabelMap.h"

using namespace db;

class LabelMapTest : public ::testing::Test {
};

TEST_F(LabelMapTest, createOne) {
    LabelMap labelMap;

    const LabelID protein = labelMap.getOrCreate("protein");
    ASSERT_TRUE(protein.isValid());
}

TEST_F(LabelMapTest, get) {
    LabelMap labelMap;

    labelMap.getOrCreate("protein");
    const auto protein = labelMap.get("protein");
    ASSERT_TRUE(protein);
}

TEST_F(LabelMapTest, different) {
    LabelMap labelMap;

    const LabelID protein1 = labelMap.getOrCreate("protein1");
    const LabelID protein2 = labelMap.getOrCreate("protein2");

    ASSERT_TRUE(protein1.isValid());
    ASSERT_TRUE(protein2.isValid());

    ASSERT_NE(protein1, protein2);
}

TEST_F(LabelMapTest, emptyString) {
    LabelMap map;

    const LabelID id = map.getOrCreate("");
    ASSERT_TRUE(id.isValid());
}
