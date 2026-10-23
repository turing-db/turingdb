#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class UnwindRowCountTest : public CallV3Test {
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

TEST_F(UnwindRowCountTest, countsTheRowsOfAListOnlyTheCountReads) {
    expectRows("UNWIND range(1, 3) AS i WITH range(1, i) AS l UNWIND l AS j RETURN count(*)", {{"6"}});
}

TEST_F(UnwindRowCountTest, countsTheRowsOfEveryGroup) {
    expectRows("UNWIND range(1, 4) AS i WITH i, range(1, i) AS l UNWIND l AS j RETURN i % 2 AS k, count(*) ORDER BY k",
               {{"0", "6"}, {"1", "4"}});
}

TEST_F(UnwindRowCountTest, countsTheRowsAFilterKeeps) {
    expectRows("UNWIND range(1, 3) AS i WITH range(1, i) AS l UNWIND l AS j WITH l, j WHERE j > 1 RETURN count(*)", {{"3"}});
}

TEST_F(UnwindRowCountTest, laysAConstantOutOverTheRows) {
    expectRows("UNWIND range(1, 3) AS i WITH range(1, i) AS l UNWIND l AS j WITH 1 AS x RETURN sum(x)", {{"6"}});
}

TEST_F(UnwindRowCountTest, countsTheRowsOfNullElements) {
    expectRows("UNWIND [1, null, 3] AS i WITH i, [i, i] AS l UNWIND l AS j RETURN count(*)", {{"6"}});
}

TEST_F(UnwindRowCountTest, countsTheRowsOfEveryRowOfAChunk) {
    expectRows("UNWIND range(1, 65536) AS i WITH range(i, i + 999) AS l UNWIND l AS j RETURN count(*)", {{"65536000"}});
}

TEST_F(UnwindRowCountTest, keepsTheListAGroupReads) {
    expectSortedRows("UNWIND range(1, 2) AS i WITH range(1, i) AS l UNWIND l AS j RETURN l, count(*)",
                     {{"1", "1"}, {"1, 2", "2"}});
}
