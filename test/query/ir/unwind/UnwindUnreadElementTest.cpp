#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// Bodies that never read the unwound value. Where the element has no use, its loop fills
// no element column; a write still reads it, for its row count.
class UnwindUnreadElementTest : public CallV3Test {
protected:
    void expectSortedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }
};

TEST_F(UnwindUnreadElementTest, countsTheRowsOfANestedRange) {
    expectSortedRows("UNWIND range(1, 3) AS x UNWIND range(1, 5000) AS y RETURN x, count(*)",
                     {{"1", "5000"}, {"2", "5000"}, {"3", "5000"}});
}

TEST_F(UnwindUnreadElementTest, writesOncePerIntegerOfARange) {
    runWrite("UNWIND range(1, 3) AS x CREATE (:Tick)");

    expectSortedRows("MATCH (t:Tick) RETURN count(t)", {{"3"}});
}

TEST_F(UnwindUnreadElementTest, repeatsTheMatchedRowsOncePerElementOfARowList) {
    expectSortedRows("MATCH (n:Person) WHERE n.age = 32 UNWIND [n.name, n.age, n.name] AS x RETURN n.name, count(*)",
                     {{"Adam", "3"}, {"Remy", "3"}});
}

TEST_F(UnwindUnreadElementTest, writesOncePerElementOfARowList) {
    runWrite("MATCH (n:Person) WHERE n.age = 32 UNWIND [n.name, n.age] AS x CREATE (:Tick)");

    expectSortedRows("MATCH (t:Tick) RETURN count(t)", {{"4"}});
}

TEST_F(UnwindUnreadElementTest, readsTheElementWhereTheBodyDoes) {
    expectSortedRows("MATCH (n:Person) WHERE n.age = 32 UNWIND [n.name, 'x'] AS x RETURN n.name, x",
                     {{"Adam", "Adam"}, {"Adam", "x"}, {"Remy", "Remy"}, {"Remy", "x"}});
}
