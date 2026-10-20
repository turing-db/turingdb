#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class UnwindRangeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectSortedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }
};

TEST_F(UnwindRangeTest, countsTwoNestedRanges) {
    expectRows("UNWIND range(1, 100000) AS i UNWIND range(1, 1000) AS j RETURN count(*)", {{"100000000"}});
}

TEST_F(UnwindRangeTest, countsARangeLongerThanTheListLimit) {
    expectRows("UNWIND range(1, 1000000) AS x RETURN count(x), sum(x)", {{"1000000", "500000500000"}});
}

TEST_F(UnwindRangeTest, walksTheWholeInt64RangeUpToALimit) {
    expectRows("UNWIND range(-9223372036854775808, 9223372036854775807) AS x RETURN x LIMIT 3",
               {{"-9223372036854775808"}, {"-9223372036854775807"}, {"-9223372036854775806"}});
}

TEST_F(UnwindRangeTest, walksTheWholeInt64RangeDownToALimit) {
    expectRows("UNWIND range(9223372036854775807, -9223372036854775808, -1) AS x RETURN x LIMIT 2",
               {{"9223372036854775807"}, {"9223372036854775806"}});
}

TEST_F(UnwindRangeTest, countsDownByANegativeStep) {
    expectRows("UNWIND range(10, 1, -3) AS x RETURN x", {{"10"}, {"7"}, {"4"}, {"1"}});
}

TEST_F(UnwindRangeTest, unwindsNothingForAStepAwayFromTheEnd) {
    expectRows("UNWIND range(1, 10, -1) AS x RETURN count(x)", {{"0"}});
}

TEST_F(UnwindRangeTest, unwindsNothingForANullBound) {
    expectRows("UNWIND range(1, null) AS x RETURN count(x)", {{"0"}});
}

TEST_F(UnwindRangeTest, rejectsAStepOfZero) {
    runQueryExpectingError("UNWIND range(1, 3, 0) AS x RETURN x", "range() cannot count by a step of 0");
}

TEST_F(UnwindRangeTest, countsUpToTheBoundOfTheOuterRow) {
    expectRows("UNWIND range(1, 3) AS x UNWIND range(1, x) AS y RETURN x, y",
               {{"1", "1"}, {"2", "1"}, {"2", "2"}, {"3", "1"}, {"3", "2"}, {"3", "3"}});
}

TEST_F(UnwindRangeTest, splitsARowLongerThanAChunk) {
    expectSortedRows("UNWIND range(1, 3) AS x UNWIND range(1, 5000) AS y RETURN x, count(y), sum(y)",
                     {{"1", "5000", "12502500"}, {"2", "5000", "12502500"}, {"3", "5000", "12502500"}});
}

// Only Remy and Adam carry an age, 32 each: the other six Person rows read a null bound
TEST_F(UnwindRangeTest, countsUpToAStoredBound) {
    expectSortedRows("MATCH (n:Person) UNWIND range(1, n.age) AS x RETURN n.name, count(x), sum(x)",
                     {{"Adam", "32", "528"}, {"Remy", "32", "528"}});
}

TEST_F(UnwindRangeTest, repeatsAConstantRangeForEveryMatchedRow) {
    expectRows("MATCH (n:Person) UNWIND range(1, 2) AS x RETURN count(*)", {{"16"}});
}

TEST_F(UnwindRangeTest, keepsTheListAnotherProjectionReads) {
    expectRows("WITH range(1, 3) AS xs UNWIND xs AS x RETURN x, xs",
               {{"1", "1, 2, 3"}, {"2", "1, 2, 3"}, {"3", "1, 2, 3"}});
}

TEST_F(UnwindRangeTest, computesOverTheElement) {
    expectRows("UNWIND range(1, 4) AS x RETURN x * 2 + 1", {{"3"}, {"5"}, {"7"}, {"9"}});
}

TEST_F(UnwindRangeTest, filtersTheElements) {
    expectRows("UNWIND range(1, 10) AS x WITH x WHERE x % 3 = 0 RETURN x", {{"3"}, {"6"}, {"9"}});
}

TEST_F(UnwindRangeTest, filtersTheElementsAgainstAList) {
    expectRows("UNWIND range(1, 6) AS x WITH x WHERE x IN [2, 4, 9] RETURN x", {{"2"}, {"4"}});
}

TEST_F(UnwindRangeTest, aggregatesTheElements) {
    expectRows("UNWIND range(1, 10) AS x RETURN min(x), max(x), avg(x), sum(x)", {{"1", "10", "5.5", "55"}});
}

TEST_F(UnwindRangeTest, groupsByAKeyComputedFromTheElement) {
    expectRows("UNWIND range(1, 10) AS x RETURN x % 3 AS r, count(*) ORDER BY r",
               {{"0", "3"}, {"1", "4"}, {"2", "3"}});
}

TEST_F(UnwindRangeTest, countsTheDistinctKeysOfTheElements) {
    expectRows("UNWIND range(1, 100) AS x RETURN count(DISTINCT x % 7)", {{"7"}});
}

TEST_F(UnwindRangeTest, ordersTheElementsDescending) {
    expectRows("UNWIND range(1, 5000) AS x RETURN x ORDER BY x DESC LIMIT 3", {{"5000"}, {"4999"}, {"4998"}});
}

TEST_F(UnwindRangeTest, skipsPastAChunkOfElements) {
    expectRows("UNWIND range(1, 5000) AS x RETURN x SKIP 2048 LIMIT 2", {{"2049"}, {"2050"}});
}

TEST_F(UnwindRangeTest, collectsTheElements) {
    expectRows("UNWIND range(1, 5) AS x RETURN collect(x)", {{"1, 2, 3, 4, 5"}});
}

TEST_F(UnwindRangeTest, projectsTheDistinctSumsOfTwoRanges) {
    expectSortedRows("UNWIND range(1, 3) AS x UNWIND range(1, 3) AS y RETURN DISTINCT x + y",
                     {{"2"}, {"3"}, {"4"}, {"5"}, {"6"}});
}

TEST_F(UnwindRangeTest, joinsTwoRangesOnEquality) {
    expectSortedRows("UNWIND range(1, 5) AS x UNWIND range(3, 8) AS y WITH x, y WHERE x = y RETURN x, y",
                     {{"3", "3"}, {"4", "4"}, {"5", "5"}});
}

TEST_F(UnwindRangeTest, sumsAProjectionOfTheElement) {
    expectRows("UNWIND range(1, 3) AS x WITH x * 10 AS y RETURN sum(y)", {{"60"}});
}

TEST_F(UnwindRangeTest, countsFromBoundsComputedFromTheOuterElement) {
    expectSortedRows("UNWIND range(1, 3) AS x UNWIND range(x, x * 2) AS y RETURN x, sum(y)",
                     {{"1", "3"}, {"2", "9"}, {"3", "18"}});
}

TEST_F(UnwindRangeTest, branchesOnTheElement) {
    expectRows("UNWIND range(1, 3) AS x RETURN CASE WHEN x > 1 THEN 'big' ELSE 'small' END",
               {{"small"}, {"big"}, {"big"}});
}

TEST_F(UnwindRangeTest, convertsTheElementToAString) {
    expectRows("UNWIND range(1, 3) AS x RETURN toString(x) + '!'", {{"1!"}, {"2!"}, {"3!"}});
}

TEST_F(UnwindRangeTest, indexesAListByTheElement) {
    expectRows("WITH ['a', 'b', 'c'] AS xs UNWIND range(0, 2) AS i RETURN xs[i]", {{"a"}, {"b"}, {"c"}});
}

// Remy=0, Adam=1, Maxime=8 and Luc=9 are the Person nodes among the first ten IDs
TEST_F(UnwindRangeTest, matchesNodesByTheElement) {
    expectSortedRows("UNWIND range(0, 9) AS x MATCH (n:Person) WHERE id(n) = x RETURN x, n.name",
                     {{"0", "Remy"}, {"1", "Adam"}, {"8", "Maxime"}, {"9", "Luc"}});
}

TEST_F(UnwindRangeTest, comparesTheElementToAProperty) {
    expectSortedRows("MATCH (n:Person) UNWIND range(30, 33) AS x WITH n, x WHERE n.age = x RETURN n.name, x",
                     {{"Adam", "32"}, {"Remy", "32"}});
}

TEST_F(UnwindRangeTest, writesTheElementAsAProperty) {
    runWrite("UNWIND range(1, 3) AS x CREATE (:Counter {value: x * 100})");

    expectSortedRows("MATCH (c:Counter) RETURN c.value", {{"100"}, {"200"}, {"300"}});
}
