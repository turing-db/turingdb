#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// An expression over constants alone is computed once, at the top of the function, among the
// constants and handles hoisted there. Its body may read any of them, including one hoisted
// for an earlier expression or after its own operands, so it sits below all of them.
class ComputationBelowHoistedValuesTest : public CallV3Test {
protected:
    static constexpr std::string_view COUNT_INTERESTS =
        "COUNT { MATCH (q:Person)-[:INTERESTED_IN]->(i) WHERE q.name = name RETURN i LIMIT 2 }";

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << query;
    }
};

// The MATCH reads name before the comprehension does. Remy counts 2 interests and Doruk 1.
TEST_F(ComputationBelowHoistedValuesTest, readsAPropertyAMatchReadBefore) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "RETURN p.name, [name IN ['Remy', 'Doruk'] | "
               "COUNT { MATCH (q:Person)-[:INTERESTED_IN]->(i) WHERE q.name = name RETURN i LIMIT 2 }]",
               {{"Remy", "[2, 1]"}});
}

TEST_F(ComputationBelowHoistedValuesTest, readsAPropertyEveryRowProjectedBefore) {
    std::string query = "MATCH (p:Person) WITH p.name AS n RETURN n, [name IN ['Remy', 'Doruk'] | ";
    query += COUNT_INTERESTS;
    query += "] AS c";

    expectRows(query, {
        {"Remy", "[2, 1]"},
        {"Adam", "[2, 1]"},
        {"Maxime", "[2, 1]"},
        {"Luc", "[2, 1]"},
        {"Martina", "[2, 1]"},
        {"Suhas", "[2, 1]"},
        {"Cyrus", "[2, 1]"},
        {"Doruk", "[2, 1]"},
    });
}

TEST_F(ComputationBelowHoistedValuesTest, readsAPropertyAWhereFilteredOn) {
    std::string query = "MATCH (p:Person) WHERE p.name = 'Remy' WITH p RETURN p.name, [name IN ['Remy', 'Doruk'] | ";
    query += COUNT_INTERESTS;
    query += "] AS c";

    expectRows(query, {{"Remy", "[2, 1]"}});
}

TEST_F(ComputationBelowHoistedValuesTest, readsAPropertyAWithProjectedBeside) {
    std::string query = "MATCH (p:Person {name: 'Remy'}) WITH p.name AS n, 1 AS one RETURN n, [name IN ['Remy', 'Doruk'] | ";
    query += COUNT_INTERESTS;
    query += "] AS c";

    expectRows(query, {{"Remy", "[2, 1]"}});
}

TEST_F(ComputationBelowHoistedValuesTest, readsAPropertyAWithProjected) {
    std::string query = "MATCH (p:Person {name: 'Remy'}) WITH p.name AS n RETURN n, [name IN ['Remy', 'Doruk'] | ";
    query += COUNT_INTERESTS;
    query += "] AS c";

    expectRows(query, {{"Remy", "[2, 1]"}});
}

// The MATCH hops INTERESTED_IN before the comprehension does. Adam counts 2 interests and Doruk 1.
TEST_F(ComputationBelowHoistedValuesTest, hopsAnEdgeTypeAMatchHoppedBefore) {
    expectRows("MATCH (:Person {name: 'Remy'})-[:INTERESTED_IN]->(i:Interest {name: 'Ghosts'}) "
               "RETURN i.name, [name IN ['Adam', 'Doruk'] | "
               "COUNT { MATCH (q:Person)-[:INTERESTED_IN]->(j) WHERE q.name = name RETURN j LIMIT 2 }]",
               {{"Ghosts", "[2, 1]"}});
}

TEST_F(ComputationBelowHoistedValuesTest, readsAConstantHoistedAfterItsList) {
    expectRows("WITH ['Remy', 'Adam'] AS names RETURN 5 AS five, [name IN names | name + '!'] AS shouted",
               {{"5", "[Remy!, Adam!]"}});
}
