#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class ListOrderingTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ListOrderingTest, ordersTwoListsByTheirFirstDifferentElement) {
    expectRows("RETURN [1, 2] < [1, 3], [1, 2] > [1, 3], [2] > [1, 3]", {{"true", "false", "true"}});
}

TEST_F(ListOrderingTest, ordersAPrefixBeforeTheLongerList) {
    expectRows("RETURN [1, 2] < [1, 2, 3], [] < [1], [1, 2, 3] <= [1, 2]", {{"true", "true", "false"}});
}

TEST_F(ListOrderingTest, ordersEqualLists) {
    expectRows("RETURN [1, 2] <= [1, 2], [1, 2] >= [1, 2], [1, 2] < [1, 2]", {{"true", "true", "false"}});
}

TEST_F(ListOrderingTest, ordersListsOfStrings) {
    expectRows("RETURN ['a', 'b'] < ['a', 'c']", {{"true"}});
}

TEST_F(ListOrderingTest, ordersNestedLists) {
    expectRows("RETURN [[1, 2], 3] < [[1, 3], 0]", {{"true"}});
}

TEST_F(ListOrderingTest, answersNullForElementsOfDifferentTypes) {
    expectRows("RETURN [1, 'a'] < [1, 2]", {{"null"}});
}

TEST_F(ListOrderingTest, answersNullForANullElementItReaches) {
    expectRows("RETURN [1, null] < [1, 2], [null, 1] < [2, 3]", {{"null", "null"}});
}

TEST_F(ListOrderingTest, decidesBeforeANullElementItDoesNotReach) {
    expectRows("RETURN [1, null] < [2, 3]", {{"true"}});
}

TEST_F(ListOrderingTest, ordersAListCellAgainstAList) {
    expectRows("UNWIND [[1, 2], 1] AS x RETURN x, x > [1]",
               {{"1", "null"},
                {"[1, 2]", "true"}});
}

TEST_F(ListOrderingTest, ordersAListAgainstAListCell) {
    expectRows("UNWIND [[1, 2], 1] AS x RETURN x, [1] < x",
               {{"1", "null"},
                {"[1, 2]", "true"}});
}

TEST_F(ListOrderingTest, ordersListsBoundPerRow) {
    expectRows("UNWIND [[1], [1, 2], [2]] AS x WITH x WHERE x > [1, 1] RETURN x",
               {{"[1, 2]"},
                {"[2]"}});
}

TEST_F(ListOrderingTest, ordersAStoredListAndAnswersNullWhereItIsAbsent) {
    runWrite("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
    runWrite("MATCH (n {name: 'Adam'}) SET n.tags = [1, 4]");

    expectRows("MATCH (n) WHERE n.name IN ['Remy', 'Adam', 'Luc'] RETURN n.name, n.tags < [1, 3]",
               {{"Adam", "false"},
                {"Luc", "null"},
                {"Remy", "true"}});
}
