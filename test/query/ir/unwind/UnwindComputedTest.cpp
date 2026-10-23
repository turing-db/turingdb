#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class UnwindComputedTest : public CallV3Test {
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

TEST_F(UnwindComputedTest, unwindsASplitStringOfEveryRow) {
    expectRows("UNWIND range(1, 3) AS i UNWIND split(toString(i) + ',a,b', ',') AS j RETURN i, j",
               {{"1", "1"}, {"1", "a"}, {"1", "b"},
                {"2", "2"}, {"2", "a"}, {"2", "b"},
                {"3", "3"}, {"3", "a"}, {"3", "b"}});
}

TEST_F(UnwindComputedTest, unwindsAConcatenation) {
    expectRows("UNWIND range(1, 2) AS i UNWIND range(i, i + 1) + [10 * i] AS j RETURN i, j",
               {{"1", "1"}, {"1", "2"}, {"1", "10"}, {"2", "2"}, {"2", "3"}, {"2", "20"}});
}

TEST_F(UnwindComputedTest, unwindsAReversedComprehension) {
    expectRows("UNWIND range(1, 2) AS i UNWIND reverse([x IN range(1, 3) | x * i]) AS j RETURN i, j",
               {{"1", "3"}, {"1", "2"}, {"1", "1"}, {"2", "6"}, {"2", "4"}, {"2", "2"}});
}

TEST_F(UnwindComputedTest, unwindsASliceOfAComprehension) {
    expectRows("UNWIND range(1, 2) AS i UNWIND [x IN range(1, 4) | x + i][1..3] AS j RETURN i, j",
               {{"1", "3"}, {"1", "4"}, {"2", "4"}, {"2", "5"}});
}

TEST_F(UnwindComputedTest, unwindsACollectSubquery) {
    expectRows("UNWIND range(1, 2) AS i UNWIND COLLECT { UNWIND range(1, 3) AS k RETURN k + i } AS j RETURN i, j",
               {{"1", "2"}, {"1", "3"}, {"1", "4"}, {"2", "3"}, {"2", "4"}, {"2", "5"}});
}

TEST_F(UnwindComputedTest, unwindsAReducedList) {
    expectRows("UNWIND range(1, 2) AS i UNWIND reduce(acc = [], k IN range(1, 3) | acc + [k * i]) AS j RETURN i, j",
               {{"1", "1"}, {"1", "2"}, {"1", "3"}, {"2", "2"}, {"2", "4"}, {"2", "6"}});
}

TEST_F(UnwindComputedTest, unwindsAScalarComputedPerRow) {
    expectRows("UNWIND range(1, 3) AS i UNWIND toString(i) + '!' AS j RETURN j", {{"1!"}, {"2!"}, {"3!"}});
}

TEST_F(UnwindComputedTest, unwindsNothingForANullList) {
    expectRows("UNWIND [1, null, 3] AS i UNWIND range(1, i) + [] AS j RETURN i, j",
               {{"1", "1"}, {"3", "1"}, {"3", "2"}, {"3", "3"}});
}

// Only Remy and Adam carry an age, 32 each
TEST_F(UnwindComputedTest, carriesTheMatchedRow) {
    expectSortedRows("MATCH (n:Person) WHERE n.age IS NOT NULL UNWIND reverse(range(1, n.age)[..2] + [n.name]) AS x RETURN n.name, x",
                     {{"Adam", "1"}, {"Adam", "2"}, {"Adam", "Adam"}, {"Remy", "1"}, {"Remy", "2"}, {"Remy", "Remy"}});
}

TEST_F(UnwindComputedTest, keepsTheListAnotherProjectionReads) {
    expectRows("UNWIND range(1, 2) AS i WITH i, reverse(range(1, i) + [0]) AS l UNWIND l AS j RETURN i, j, l",
               {{"1", "0", "0, 1"}, {"1", "1", "0, 1"}, {"2", "0", "0, 2, 1"}, {"2", "2", "0, 2, 1"}, {"2", "1", "0, 2, 1"}});
}

TEST_F(UnwindComputedTest, unwindsAComputedListPerElementOfAnother) {
    expectRows("UNWIND range(1, 2) AS i UNWIND split(toString(i) + ',x', ',') AS s UNWIND split(s + ';' + s, ';') AS t RETURN t",
               {{"1"}, {"1"}, {"x"}, {"x"}, {"2"}, {"2"}, {"x"}, {"x"}});
}

TEST_F(UnwindComputedTest, unwindsRowsLongerThanAChunk) {
    expectSortedRows("UNWIND range(1, 3) AS i UNWIND reverse([x IN range(1, 5000) | x * i]) AS j RETURN i, count(j), sum(j)",
                     {{"1", "5000", "12502500"}, {"2", "5000", "25005000"}, {"3", "5000", "37507500"}});
}

TEST_F(UnwindComputedTest, countsTheElementsOfEveryRowOfAChunk) {
    expectRows("UNWIND range(1, 65536) AS i UNWIND reverse([x IN range(i, i + 999) | x * 2]) AS j RETURN count(*), sum(j)",
               {{"65536000", "4360503296000"}});
}

TEST_F(UnwindComputedTest, stopsAtTheLimit) {
    expectRows("UNWIND range(1, 65536) AS i UNWIND reverse([x IN range(i, i + 999) | x]) AS j RETURN i, j LIMIT 2",
               {{"1", "1000"}, {"1", "999"}});
}

TEST_F(UnwindComputedTest, skipsAcrossRows) {
    expectRows("UNWIND range(1, 100) AS i UNWIND split(toString(i) + ',x', ',') AS j RETURN j SKIP 7 LIMIT 3",
               {{"x"}, {"5"}, {"x"}});
}

TEST_F(UnwindComputedTest, unwindsRowsMostOfWhichHoldNoElement) {
    expectRows("UNWIND range(1, 10000) AS i UNWIND range(1, i % 1000 - 998) + [] AS j RETURN collect(i)",
               {{"999, 1999, 2999, 3999, 4999, 5999, 6999, 7999, 8999, 9999"}});
}

TEST_F(UnwindComputedTest, unwindsRowsWhoseListsGrow) {
    expectRows("UNWIND range(1, 2000) AS i UNWIND reverse(range(1, i) + []) AS j RETURN count(*), sum(j)",
               {{"2001000", "1335334000"}});
}
