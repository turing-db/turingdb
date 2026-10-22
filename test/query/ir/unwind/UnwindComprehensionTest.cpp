#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class UnwindComprehensionTest : public CallV3Test {
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

TEST_F(UnwindComprehensionTest, unwindsAProjection) {
    expectRows("UNWIND [x IN range(1, 5) | x * 10] AS y RETURN y", {{"10"}, {"20"}, {"30"}, {"40"}, {"50"}});
}

TEST_F(UnwindComprehensionTest, unwindsAFilter) {
    expectRows("UNWIND [x IN range(1, 10) WHERE x % 3 = 0] AS y RETURN y", {{"3"}, {"6"}, {"9"}});
}

TEST_F(UnwindComprehensionTest, unwindsAFilterAndAProjection) {
    expectRows("UNWIND [x IN range(1, 10) WHERE x % 2 = 0 | x * x] AS y RETURN y",
               {{"4"}, {"16"}, {"36"}, {"64"}, {"100"}});
}

TEST_F(UnwindComprehensionTest, projectsNoElementTheFilterDrops) {
    expectRows("UNWIND [x IN [2, 0, 4] WHERE x <> 0 | 100 / x] AS y RETURN y", {{"50"}, {"25"}});
}

TEST_F(UnwindComprehensionTest, unwindsNothingWhereTheFilterDropsEveryElement) {
    expectRows("UNWIND [x IN range(1, 3) WHERE x > 5] AS y RETURN count(*)", {{"0"}});
}

TEST_F(UnwindComprehensionTest, unwindsNothingForANullList) {
    expectRows("WITH null AS xs UNWIND [x IN xs | x] AS y RETURN count(*)", {{"0"}});
}

TEST_F(UnwindComprehensionTest, keepsTheElementOrder) {
    expectRows("UNWIND [x IN [3, 'a', 2.5, 1] | x] AS y RETURN y", {{"3"}, {"a"}, {"2.5"}, {"1"}});
}

TEST_F(UnwindComprehensionTest, countsOverTheOuterElement) {
    expectRows("UNWIND range(1, 3) AS i UNWIND [x IN range(1, i) | x + i] AS y RETURN i, y",
               {{"1", "2"}, {"2", "3"}, {"2", "4"}, {"3", "4"}, {"3", "5"}, {"3", "6"}});
}

// Only Remy and Adam carry an age, 32 each: x < null drops every element of the six others
TEST_F(UnwindComprehensionTest, readsTheMatchedRowInTheFilterAndTheProjection) {
    expectSortedRows("MATCH (n:Person) UNWIND [x IN range(1, 3) WHERE x < n.age | n.name + toString(x)] AS y RETURN n.name, y",
                     {{"Adam", "Adam1"},
                      {"Adam", "Adam2"},
                      {"Adam", "Adam3"},
                      {"Remy", "Remy1"},
                      {"Remy", "Remy2"},
                      {"Remy", "Remy3"}});
}

TEST_F(UnwindComprehensionTest, filtersAgainstAMatchedProperty) {
    expectSortedRows("MATCH (n:Person) UNWIND [x IN range(30, 34) WHERE x >= n.age] AS y RETURN n.name, y",
                     {{"Adam", "32"},
                      {"Adam", "33"},
                      {"Adam", "34"},
                      {"Remy", "32"},
                      {"Remy", "33"},
                      {"Remy", "34"}});
}

TEST_F(UnwindComprehensionTest, repeatsTheListForEveryMatchedRow) {
    expectRows("MATCH (n:Person) UNWIND [x IN range(1, 4) WHERE x % 2 = 0] AS y RETURN count(*)", {{"16"}});
}

TEST_F(UnwindComprehensionTest, filtersRowsLongerThanAChunk) {
    expectSortedRows("UNWIND range(1, 3) AS i UNWIND [x IN range(1, 5000) WHERE x % 2 = 0 | x * i] AS y RETURN i, count(y), sum(y)",
                     {{"1", "2500", "6252500"}, {"2", "2500", "12505000"}, {"3", "2500", "18757500"}});
}

TEST_F(UnwindComprehensionTest, unwindsANestedComprehension) {
    expectRows("UNWIND [x IN range(1, 3) | [y IN range(1, x) | y * 10]] AS l RETURN l",
               {{"10"}, {"10, 20"}, {"10, 20, 30"}});
}

TEST_F(UnwindComprehensionTest, keepsTheListAnotherProjectionReads) {
    expectRows("WITH [x IN range(1, 3) | x * 2] AS xs UNWIND xs AS y RETURN y, xs",
               {{"2", "2, 4, 6"}, {"4", "2, 4, 6"}, {"6", "2, 4, 6"}});
}

TEST_F(UnwindComprehensionTest, stopsAtTheLimit) {
    expectRows("UNWIND range(1, 65536) AS i UNWIND [x IN range(i, i + 999) | x * 2] AS j RETURN i, j LIMIT 2",
               {{"1", "2"}, {"1", "4"}});
}

TEST_F(UnwindComprehensionTest, countsTheElementsOfEveryRowOfAChunk) {
    expectRows("UNWIND range(1, 65536) AS i UNWIND [x IN range(i, i + 999) WHERE x % 2 = 0 | x] AS j RETURN count(*)",
               {{"32768000"}});
}

TEST_F(UnwindComprehensionTest, unwindsAConstantProjectionOncePerElement) {
    expectRows("UNWIND [x IN range(1, 3) | 7] AS y RETURN y", {{"7"}, {"7"}, {"7"}});
}

TEST_F(UnwindComprehensionTest, unwindsAConstantProjectionOncePerKeptElement) {
    expectRows("UNWIND [x IN range(1, 3) WHERE x > 1 | 'k'] AS y RETURN y", {{"k"}, {"k"}});
}

TEST_F(UnwindComprehensionTest, limitsTheFilteredElements) {
    expectRows("UNWIND [x IN range(1, 10) WHERE x > 2] AS y RETURN y LIMIT 2", {{"3"}, {"4"}});
}

TEST_F(UnwindComprehensionTest, skipsPastAChunkOfFilteredElements) {
    expectRows("UNWIND [x IN range(1, 10000) WHERE x % 2 = 1] AS y RETURN y SKIP 2048 LIMIT 2", {{"4097"}, {"4099"}});
}

TEST_F(UnwindComprehensionTest, unwindsAComprehensionOverAComprehension) {
    expectRows("UNWIND [x IN [y IN range(1, 5) | y * 2] WHERE x > 4] AS z RETURN z", {{"6"}, {"8"}, {"10"}});
}
