#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A type-erased cell carries its null in its tag, and a comparison of two values of
// different types is null, so a comparison over a null or a mismatched cell answers null
class TaggedCellComparisonTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(TaggedCellComparisonTest, answersNullForANullCell) {
    expectRows("UNWIND [null] AS x RETURN x > 1", {{"null"}});
}

TEST_F(TaggedCellComparisonTest, answersNullForACellOfAnotherType) {
    expectRows("UNWIND [1, 'a', null] AS x RETURN x, x > 0",
               {{"1", "true"},
                {"a", "null"},
                {"null", "null"}});
}

TEST_F(TaggedCellComparisonTest, answersNullWithTheCellOnTheRight) {
    expectRows("UNWIND [1, 'a', null] AS x RETURN x, 0 < x",
               {{"1", "true"},
                {"a", "null"},
                {"null", "null"}});
}

TEST_F(TaggedCellComparisonTest, answersNullForEveryOrdering) {
    expectRows("UNWIND [1, 'a', null] AS x RETURN x, x < 1, x <= 1, x >= 1",
               {{"1", "false", "true", "true"},
                {"a", "null", "null", "null"},
                {"null", "null", "null", "null"}});
}

TEST_F(TaggedCellComparisonTest, comparesAStringCellAgainstAString) {
    expectRows("UNWIND [1, 'a', null] AS x RETURN x, x > 'b'",
               {{"1", "null"},
                {"a", "false"},
                {"null", "null"}});
}

TEST_F(TaggedCellComparisonTest, comparesNumberCellsOfEitherType) {
    expectRows("UNWIND [1, 2.5, 'a'] AS x RETURN x > 2", {{"false"}, {"null"}, {"true"}});
}

TEST_F(TaggedCellComparisonTest, comparesABooleanCellAgainstABoolean) {
    expectRows("UNWIND [true, 1, null] AS x RETURN x, x > false",
               {{"1", "null"},
                {"null", "null"},
                {"true", "true"}});
}

TEST_F(TaggedCellComparisonTest, answersNullForTheEqualityOfANullCell) {
    expectRows("UNWIND [1, 'a', null] AS x RETURN x, x = 1, x <> 1",
               {{"1", "true", "false"},
                {"a", "false", "true"},
                {"null", "null", "null"}});
}

TEST_F(TaggedCellComparisonTest, answersNullForTheEqualityOfACellAgainstAList) {
    expectRows("UNWIND [[1, 2], 1, null] AS x RETURN x = [1, 2]", {{"false"}, {"null"}, {"true"}});
}

TEST_F(TaggedCellComparisonTest, answersNullForAListCellHoldingANull) {
    expectRows("UNWIND [[1, null], 1] AS x RETURN x = [1, null]", {{"false"}, {"null"}});
}

TEST_F(TaggedCellComparisonTest, matchesANodeAgainstACellHoldingItsID) {
    expectRows("UNWIND [[0, [0.5]]] AS r MATCH (n) WHERE n = r[0] RETURN n", {{"0"}});
}

TEST_F(TaggedCellComparisonTest, matchesANodeAgainstACellOfAListMixingAString) {
    expectRows("UNWIND [[0, 'a']] AS r MATCH (n) WHERE n = r[0] RETURN n", {{"0"}});
}

TEST_F(TaggedCellComparisonTest, matchesANodeAgainstACellOfAListBoundByWith) {
    expectRows("WITH [0, [0.5]] AS r MATCH (n) WHERE n = r[0] RETURN n", {{"0"}});
}

TEST_F(TaggedCellComparisonTest, matchesNoNodeAgainstACellHoldingAString) {
    expectRows("UNWIND [[0, 'a']] AS r MATCH (n) WHERE n = r[1] RETURN n", {});
}

TEST_F(TaggedCellComparisonTest, matchesNoNodeAgainstANullCell) {
    expectRows("UNWIND [[0, 'a', null]] AS r MATCH (n) WHERE n = r[2] RETURN n", {});
}

TEST_F(TaggedCellComparisonTest, keepsTheNodeACellDiffersFrom) {
    expectRows("UNWIND [[0, 'a']] AS r MATCH (n) WHERE n.name = 'Adam' AND n <> r[0] RETURN n", {{"1"}});
}

TEST_F(TaggedCellComparisonTest, dropsTheNodeACellIsEqualTo) {
    expectRows("UNWIND [[0, 'a']] AS r MATCH (n) WHERE n.name = 'Remy' AND n <> r[0] RETURN n", {});
}

TEST_F(TaggedCellComparisonTest, matchesANodeAgainstACellHoldingTheNodeItself) {
    expectRows("MATCH (n) WHERE n = 0 UNWIND [n, 'a'] AS x MATCH (m) WHERE m = x RETURN m", {{"0"}});
}

TEST_F(TaggedCellComparisonTest, matchesAnEdgeAgainstACellHoldingTheEdgeItself) {
    expectRows("MATCH ()-[e]->() WHERE e.name = 'Remy -> Adam' UNWIND [e, 'a'] AS x "
               "MATCH ()-[f]->() WHERE f = x RETURN f.name",
               {{"Remy -> Adam"}});
}

TEST_F(TaggedCellComparisonTest, comparesTwoCells) {
    expectRows("UNWIND [1, 'a', null] AS x UNWIND [1, 'b', null] AS y RETURN x, y, x < y, x = y",
               {{"1", "1", "false", "true"},
                {"1", "b", "null", "false"},
                {"1", "null", "null", "null"},
                {"a", "1", "null", "false"},
                {"a", "b", "true", "false"},
                {"a", "null", "null", "null"},
                {"null", "1", "null", "null"},
                {"null", "b", "null", "null"},
                {"null", "null", "null", "null"}});
}

TEST_F(TaggedCellComparisonTest, comparesACellAgainstAMask) {
    expectRows("UNWIND [1, 2] AS y UNWIND [true, 'a', null] AS x RETURN y, x, x = (y > 1)",
               {{"1", "a", "false"},
                {"1", "null", "null"},
                {"1", "true", "false"},
                {"2", "a", "false"},
                {"2", "null", "null"},
                {"2", "true", "true"}});
}

TEST_F(TaggedCellComparisonTest, stillTestsACellForNull) {
    expectRows("UNWIND [1, 'a', null] AS x RETURN x, x IS NULL, x IS NOT NULL",
               {{"1", "false", "true"},
                {"a", "false", "true"},
                {"null", "true", "false"}});
}

TEST_F(TaggedCellComparisonTest, filtersOutTheCellsThatDoNotCompare) {
    expectRows("UNWIND [1, 'a', null] AS x WITH x WHERE x < 5 RETURN x", {{"1"}});
}

TEST_F(TaggedCellComparisonTest, filtersOutTheNegationOfACellThatDoesNotCompare) {
    expectRows("UNWIND [1, 'a', null] AS x WITH x WHERE NOT x > 5 RETURN x", {{"1"}});
}

TEST_F(TaggedCellComparisonTest, decidesAnyNullOverANullElement) {
    expectRows("RETURN any(x IN [null] WHERE x > 1)", {{"null"}});
}

TEST_F(TaggedCellComparisonTest, decidesAllNullOverANullElement) {
    expectRows("RETURN all(x IN [null] WHERE x > 1)", {{"null"}});
}

TEST_F(TaggedCellComparisonTest, decidesNoneNullOverANullElement) {
    expectRows("RETURN none(x IN [null] WHERE x > 1)", {{"null"}});
}

TEST_F(TaggedCellComparisonTest, decidesSingleNullOverANullElement) {
    expectRows("RETURN single(x IN [null] WHERE x > 1)", {{"null"}});
}

TEST_F(TaggedCellComparisonTest, decidesAnyNullOverAMixedList) {
    expectRows("RETURN any(x IN [1, 'a', null] WHERE x > 5)", {{"null"}});
}

TEST_F(TaggedCellComparisonTest, decidesAnyTrueOverAMixedListWithAMatch) {
    expectRows("RETURN any(x IN [1, 'a', null] WHERE x > 0)", {{"true"}});
}

TEST_F(TaggedCellComparisonTest, decidesAllNullOverAMixedList) {
    expectRows("RETURN all(x IN [1, 'a'] WHERE x > 0)", {{"null"}});
}
