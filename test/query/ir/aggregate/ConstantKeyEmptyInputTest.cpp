#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A projection whose grouping keys are all constant still groups: it returns one row when
// rows come in and none when no row does, where a projection with no key returns one row
// either way. No person of simpledb is named Nobody, and every person has an interest.
class ConstantKeyEmptyInputTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }
};

TEST_F(ConstantKeyEmptyInputTest, countsNoGroupUnderAConstantKey) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS x, count(n)", {});
}

TEST_F(ConstantKeyEmptyInputTest, countsZeroWithNoKey) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN count(n)", {{"0"}});
}

TEST_F(ConstantKeyEmptyInputTest, countsNoGroupUnderAConstantBoundBeforeTheMatch) {
    expectRows("WITH 1 AS k MATCH (n:Person {name: 'Nobody'}) RETURN k, count(n)", {});
}

TEST_F(ConstantKeyEmptyInputTest, countsNoGroupUnderTheConstantKeyOfAWith) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) WITH 1 AS x, count(n) AS c RETURN x, c", {});
}

TEST_F(ConstantKeyEmptyInputTest, countsNoGroupOverAnEmptyUnwind) {
    expectRows("UNWIND [] AS u RETURN 'a' AS x, count(u)", {});
}

TEST_F(ConstantKeyEmptyInputTest, countsNoGroupUnderNullListAndMapKeys) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN null AS x, [1, 2] AS y, {a: 1} AS z, count(n)", {});
}

TEST_F(ConstantKeyEmptyInputTest, collectsNoGroupUnderAConstantKey) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS x, collect(n.name)", {});
}

TEST_F(ConstantKeyEmptyInputTest, collectsAndCountsNoGroupUnderAConstantKey) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS x, count(n), collect(n.name)", {});
}

TEST_F(ConstantKeyEmptyInputTest, takesNoPercentileUnderAConstantKey) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS x, percentileCont(n.age, 0.5)", {});
}

TEST_F(ConstantKeyEmptyInputTest, ordersNoGroupUnderAConstantKey) {
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS x ORDER BY count(n)", {});
}

TEST_F(ConstantKeyEmptyInputTest, countsEveryPersonUnderConstantKeys) {
    expectRows("MATCH (n:Person) RETURN 1 AS x, null AS y, count(n)", {{"1", "null", "8"}});
}

TEST_F(ConstantKeyEmptyInputTest, countsTheDistinctRowsUnderAConstantKey) {
    expectRows("UNWIND [1, 1, 2] AS x WITH DISTINCT x RETURN 1 AS k, count(*)", {{"1", "2"}});
}

TEST_F(ConstantKeyEmptyInputTest, ordersTheOneGroupUnderAConstantKey) {
    expectRows("MATCH (n:Person) RETURN 1 AS x ORDER BY count(n)", {{"1"}});
}

TEST_F(ConstantKeyEmptyInputTest, ordersTheOneRowOfAQueryWithNoMatch) {
    expectRows("RETURN 1 AS x ORDER BY count(*)", {{"1"}});
}

TEST_F(ConstantKeyEmptyInputTest, callReturnsNoRowForAPersonWithNoMatch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Nobody' RETURN 1 AS x, count(i) AS c } "
               "RETURN p.name, x, c",
               {});
}

TEST_F(ConstantKeyEmptyInputTest, callCountsTheInterestsOfEachPerson) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN 1 AS x, count(i) AS c } "
               "RETURN p.name AS name, x, c ORDER BY name",
               {
                   {"Adam", "1", "2"},
                   {"Cyrus", "1", "2"},
                   {"Doruk", "1", "1"},
                   {"Luc", "1", "2"},
                   {"Martina", "1", "1"},
                   {"Maxime", "1", "2"},
                   {"Remy", "1", "3"},
                   {"Suhas", "1", "2"},
               });
}

TEST_F(ConstantKeyEmptyInputTest, countSubqueryCountsNoGroup) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name AS name, "
               "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Nobody' RETURN 1 AS x, count(i) AS c } AS groups "
               "ORDER BY name",
               {
                   {"Adam", "0"},
                   {"Cyrus", "0"},
                   {"Doruk", "0"},
                   {"Luc", "0"},
                   {"Martina", "0"},
                   {"Maxime", "0"},
                   {"Remy", "0"},
                   {"Suhas", "0"},
               });
}

TEST_F(ConstantKeyEmptyInputTest, existsSubqueryFindsNoGroup) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name AS name, "
               "EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Nobody' RETURN 1 AS x, count(i) AS c } AS found "
               "ORDER BY name",
               {
                   {"Adam", "false"},
                   {"Cyrus", "false"},
                   {"Doruk", "false"},
                   {"Luc", "false"},
                   {"Martina", "false"},
                   {"Maxime", "false"},
                   {"Remy", "false"},
                   {"Suhas", "false"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
