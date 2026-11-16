#include <gtest/gtest.h>

#include <stdint.h>
#include <string>

#include "columns/ColumnOptVector.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;

TEST(ColumnOptVectorResizeValidTest, growFromEmptyKeepsNoMask) {
    ColumnOptVector<int64_t> column;
    column.resizeValid(130);

    ASSERT_EQ(column.size(), 130u);
    EXPECT_FALSE(column.hasMask());
    EXPECT_EQ(column.getNullCount(), 0u);
    for (size_t row = 0; row < column.size(); ++row) {
        ASSERT_TRUE(column.isValid(row));
        EXPECT_EQ(column.getValue(row), 0);
    }
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorResizeValidTest, everyRowSetKeepsNoMask) {
    ColumnOptVector<std::string> column;
    column.resizeValid(70);
    for (size_t row = 0; row < column.size(); ++row) {
        column.set(row, std::to_string(row));
    }

    EXPECT_FALSE(column.hasMask());
    EXPECT_EQ(column.getNullCount(), 0u);
    EXPECT_EQ(column.getValue(69), "69");
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorResizeValidTest, setNullAfterResizeValidCreatesMaskWithEarlierRowsValid) {
    ColumnOptVector<int64_t> column;
    column.resizeValid(100);
    for (size_t row = 0; row < column.size(); ++row) {
        column.set(row, static_cast<int64_t>(row) + 1);
    }

    column.setNull(70);

    ASSERT_TRUE(column.hasMask());
    EXPECT_EQ(column.getNullCount(), 1u);
    EXPECT_FALSE(column.isValid(70));
    for (size_t row = 0; row < column.size(); ++row) {
        if (row != 70) {
            ASSERT_TRUE(column.isValid(row)) << "row " << row;
            EXPECT_EQ(column.getValue(row), static_cast<int64_t>(row) + 1);
        }
    }
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorResizeValidTest, growWithExistingMaskMarksNewRowsValid) {
    ColumnOptVector<int64_t> column;
    column.push_back(1);
    column.pushNull();
    column.push_back(3);
    ASSERT_TRUE(column.hasMask());

    column.resizeValid(200);

    ASSERT_EQ(column.size(), 200u);
    ASSERT_TRUE(column.hasMask());
    EXPECT_EQ(column.getNullCount(), 1u);
    EXPECT_FALSE(column.isValid(1));
    EXPECT_EQ(column.getValue(0), 1);
    EXPECT_EQ(column.getValue(2), 3);
    for (size_t row = 3; row < column.size(); ++row) {
        ASSERT_TRUE(column.isValid(row)) << "row " << row;
        EXPECT_EQ(column.getValue(row), 0);
    }
    expectColumnOptVectorInvariants(column);

    column.setNull(150);
    EXPECT_EQ(column.getNullCount(), 2u);
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorResizeValidTest, shrinkDropsNullsInTheCutPart) {
    ColumnOptVector<int64_t> column;
    column.resizeValid(200);
    column.setNull(10);
    column.setNull(130);
    column.setNull(199);
    ASSERT_EQ(column.getNullCount(), 3u);

    column.resizeValid(100);

    ASSERT_EQ(column.size(), 100u);
    EXPECT_EQ(column.getNullCount(), 1u);
    EXPECT_FALSE(column.isValid(10));
    expectColumnOptVectorInvariants(column);

    column.resizeValid(150);

    ASSERT_EQ(column.size(), 150u);
    EXPECT_EQ(column.getNullCount(), 1u);
    EXPECT_TRUE(column.isValid(130));
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorResizeValidTest, shrinkWithoutMaskKeepsNoMask) {
    ColumnOptVector<int64_t> column;
    column.resizeValid(100);
    column.resizeValid(10);

    ASSERT_EQ(column.size(), 10u);
    EXPECT_FALSE(column.hasMask());
    expectColumnOptVectorInvariants(column);

    column.resizeValid(0);

    EXPECT_TRUE(column.empty());
    EXPECT_FALSE(column.hasMask());
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorResizeValidTest, reuseAfterClearStartsWithoutMask) {
    ColumnOptVector<int64_t> column;
    column.resizeValid(10);
    column.setNull(3);
    ASSERT_TRUE(column.hasMask());

    column.clear();
    column.resizeValid(10);

    EXPECT_FALSE(column.hasMask());
    EXPECT_EQ(column.getNullCount(), 0u);
    expectColumnOptVectorInvariants(column);
}
