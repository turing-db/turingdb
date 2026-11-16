#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <utility>

#include "columns/ColumnOptVector.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;

namespace {

const std::string FIRST_VALUE = "a value long enough to live on the heap, the first row";

void fillToCapacity(ColumnOptVector<std::string>& column, bool withNull) {
    column.push_back(FIRST_VALUE);
    if (withNull) {
        column.pushNull();
    }

    while (column.size() < column.getValues().capacity()) {
        column.push_back("a filler value long enough to live on the heap");
    }
}

}

TEST(ColumnOptVectorPushAliasTest, copiesARowOfAFullColumnWithoutAMask) {
    ColumnOptVector<std::string> column;
    fillToCapacity(column, false);
    ASSERT_FALSE(column.hasMask());

    column.push_back(column.getValue(0));

    EXPECT_EQ(column.getValue(column.size() - 1), FIRST_VALUE);
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorPushAliasTest, copiesARowOfAFullColumnWithAMask) {
    ColumnOptVector<std::string> column;
    fillToCapacity(column, true);
    ASSERT_TRUE(column.hasMask());

    column.push_back(column.getValue(0));

    EXPECT_EQ(column.getValue(column.size() - 1), FIRST_VALUE);
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorPushAliasTest, movesARowOfAFullColumn) {
    ColumnOptVector<std::string> column;
    fillToCapacity(column, true);

    column.push_back(std::move(column.getValues()[0]));

    EXPECT_EQ(column.getValue(column.size() - 1), FIRST_VALUE);
}
