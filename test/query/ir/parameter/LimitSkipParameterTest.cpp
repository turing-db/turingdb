#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

class LimitSkipParameterTest : public CallV3Test {
protected:
    void expectRowsInOrder(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectSortedRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectRowCount(std::string_view query, const ParameterMap& parameters, size_t expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        EXPECT_EQ(sink.getRows().size(), expected) << query;
    }

    void programOf(std::string_view dialect, std::string_view query, const ParameterMap& parameters, std::string& program) {
        StringRowSink sink;
        runQuery("EXPLAIN (" + std::string(dialect) + ") " + std::string(query), parameters, sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }
};

TEST_F(LimitSkipParameterTest, limitsTheRowsToAParameter) {
    ParameterMap parameters;
    parameters.insert("i").setInt64(2);

    expectRowCount("MATCH (n) RETURN n LIMIT $i", parameters, 2);
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $i", parameters, {{"Adam"}, {"Cyrus"}});
}

TEST_F(LimitSkipParameterTest, skipsTheRowsAParameterCounts) {
    ParameterMap parameters;
    parameters.insert("s").setInt64(6);

    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $s", parameters, {{"Remy"}, {"Suhas"}});
    expectRowCount("MATCH (n:Person) RETURN n.name SKIP $s", parameters, 2);
}

TEST_F(LimitSkipParameterTest, cutsTheWindowTwoParametersBound) {
    ParameterMap parameters;
    parameters.insert("s").setInt64(2);
    parameters.insert("l").setInt64(3);

    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $s LIMIT $l",
                      parameters,
                      {{"Doruk"}, {"Luc"}, {"Martina"}});
}

TEST_F(LimitSkipParameterTest, computesTheCountsFromParameters) {
    ParameterMap parameters;
    parameters.insert("n").setInt64(1);
    parameters.insert("page").setInt64(1);
    parameters.insert("size").setInt64(3);

    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $n + 1", parameters, {{"Adam"}, {"Cyrus"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $page * $size LIMIT $size",
                      parameters,
                      {{"Luc"}, {"Martina"}, {"Maxime"}});
    expectRowCount("MATCH (n:Person) RETURN n.name SKIP $size - 1 LIMIT $n + $page", parameters, 2);
}

TEST_F(LimitSkipParameterTest, computesTheCountsFromFunctionsOfParameters) {
    ParameterMap parameters;
    parameters.insert("s").setString("2");

    ParameterValue::List& list = parameters.insert("list").setList();
    list.emplace_back().setInt64(3);
    list.emplace_back().setInt64(1);

    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT toInteger($s)", parameters, {{"Adam"}, {"Cyrus"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT size($list)", parameters, {{"Adam"}, {"Cyrus"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $list[0] LIMIT $list[1]", parameters, {{"Luc"}});
}

TEST_F(LimitSkipParameterTest, computesTheCountsWithOpsThatMayRaise) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(4);
    parameters.insert("a").setInt64(8);
    parameters.insert("b").setInt64(3);

    ParameterValue::Map& map = parameters.insert("m").setMap();
    map["k"].setInt64(1);

    expectRowsInOrder("RETURN 1 AS x LIMIT 10 / $k", parameters, {{"1"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 10 / $k", parameters, {{"Adam"}, {"Cyrus"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $m.k + 1", parameters, {{"Adam"}, {"Cyrus"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $a % $b LIMIT $a / $k", parameters, {{"Doruk"}, {"Luc"}});
    expectRowCount("MATCH (n:Person) RETURN n.name SKIP $a % $b", parameters, 6);
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] "
                      "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY i.name LIMIT $a / $k - 1 } "
                      "RETURN p.name, interest ORDER BY p.name, interest",
                      parameters,
                      {{"Adam", "Bio"}, {"Remy", "Computers"}});
}

TEST_F(LimitSkipParameterTest, raisesWhereTheCountRaisesWhenTheLoopStarts) {
    ParameterMap parameters;
    parameters.insert("zero").setInt64(0);

    runQueryExpectingError("RETURN 1 AS x LIMIT 10 / $zero", parameters, "divide by zero");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 10 / $zero", parameters, "divide by zero");
    runQueryExpectingError("MATCH (n:NoSuchLabel) RETURN n LIMIT 10 / $zero", parameters, "divide by zero");
    runQueryExpectingError("MATCH (n:NoSuchLabel) RETURN n SKIP 10 % $zero", parameters, "modulo by zero");
}

TEST_F(LimitSkipParameterTest, readsTheCountOutOfAMapOrAListParameter) {
    ParameterMap parameters;

    ParameterValue::Map& map = parameters.insert("m").setMap();
    map["k"].setInt64(2);

    ParameterValue::List& list = parameters.insert("l").setList();
    list.emplace_back().setInt64(1);
    list.emplace_back().setString("x");
    list.emplace_back().setInt64(6);

    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $m.k", parameters, {{"Adam"}, {"Cyrus"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $l[2] LIMIT $l[0]", parameters, {{"Remy"}});
    expectRowCount("MATCH (n:Person) RETURN n.name SKIP $l[2]", parameters, 2);
}

TEST_F(LimitSkipParameterTest, limitsToAParameterTheRowsAlsoCarry) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);

    expectRowsInOrder("WITH $k AS x MATCH (n:Person) RETURN n.name, x ORDER BY n.name LIMIT $k",
                      parameters,
                      {{"Adam", "2"}, {"Cyrus", "2"}});
    expectRowsInOrder("RETURN $k AS x LIMIT $k", parameters, {{"2"}});
    expectRowsInOrder("RETURN $k AS x SKIP $k", parameters, {});
}

TEST_F(LimitSkipParameterTest, cutsAgainTheRowsACutLeft) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);
    parameters.insert("one").setInt64(1);

    expectRowsInOrder("MATCH (n:Person) WITH n SKIP $k WITH n SKIP $k RETURN count(n)", parameters, {{"4"}});
    expectRowsInOrder("MATCH (n:Person) WITH n LIMIT $k WITH n ORDER BY n.name LIMIT $one RETURN count(n)", parameters, {{"1"}});
    expectRowsInOrder("MATCH (n:Person) WITH n ORDER BY n.name LIMIT $k "
                      "MATCH (n)-[:INTERESTED_IN]->(i) RETURN n.name, i.name ORDER BY n.name, i.name LIMIT $k + 1",
                      parameters,
                      {{"Adam", "Bio"}, {"Adam", "Cooking"}, {"Cyrus", "Gym"}});
}

TEST_F(LimitSkipParameterTest, stopsTheScanAtTheBoundLimit) {
    ParameterMap parameters;
    parameters.insert("n").setInt64(3);

    expectRowCount("MATCH (n:Person) RETURN n.name LIMIT $n", parameters, 3);
    expectRowsInOrder("MATCH (n:Person) WITH n LIMIT $n RETURN count(n)", parameters, {{"3"}});
    expectRowCount("MATCH (a:Person)-[:INTERESTED_IN]->(i) RETURN a.name, i.name LIMIT $n", parameters, 3);
}

TEST_F(LimitSkipParameterTest, limitsToNoRowOnAZeroParameter) {
    ParameterMap parameters;
    parameters.insert("n").setInt64(0);

    expectRowsInOrder("MATCH (n:Person) RETURN n.name LIMIT $n", parameters, {});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $n", parameters, {});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $n LIMIT 1", parameters, {{"Adam"}});
}

TEST_F(LimitSkipParameterTest, keepsEveryRowUnderALargerLimit) {
    ParameterMap parameters;
    parameters.insert("n").setInt64(100);

    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $n",
                      parameters,
                      {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $n", parameters, {});
}

TEST_F(LimitSkipParameterTest, cutsTheRowsAWithPasses) {
    ParameterMap parameters;
    parameters.insert("s").setInt64(1);
    parameters.insert("l").setInt64(2);

    expectRowsInOrder("MATCH (n:Person) WITH n ORDER BY n.name SKIP $s LIMIT $l RETURN n.name",
                      parameters,
                      {{"Cyrus"}, {"Doruk"}});
    expectRowsInOrder("MATCH (n:Person) WITH n.name AS name ORDER BY name LIMIT $l RETURN name ORDER BY name",
                      parameters,
                      {{"Adam"}, {"Cyrus"}});
}

TEST_F(LimitSkipParameterTest, cutsTheRowsAMatchWindows) {
    ParameterMap parameters;
    parameters.insert("s").setInt64(1);
    parameters.insert("l").setInt64(2);

    expectRowsInOrder("MATCH (n:Person) ORDER BY n.name SKIP $s LIMIT $l RETURN n.name",
                      parameters,
                      {{"Cyrus"}, {"Doruk"}});
}

TEST_F(LimitSkipParameterTest, cutsTheRowsOfACallSubqueryForEachRow) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(1);

    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] "
                      "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY i.name LIMIT $k } "
                      "RETURN p.name, interest ORDER BY p.name, interest",
                      parameters,
                      {{"Adam", "Bio"}, {"Remy", "Computers"}});

    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] "
                      "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY i.name SKIP $k } "
                      "RETURN p.name, interest ORDER BY p.name, interest",
                      parameters,
                      {{"Adam", "Cooking"}, {"Remy", "Eighties"}, {"Remy", "Ghosts"}});

    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] "
                      "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest LIMIT $k + 1 } "
                      "RETURN p.name, count(interest) ORDER BY p.name",
                      parameters,
                      {{"Adam", "2"}, {"Remy", "2"}});
}

TEST_F(LimitSkipParameterTest, cutsEachBranchOfAUnion) {
    ParameterMap parameters;
    parameters.insert("a").setInt64(1);
    parameters.insert("b").setInt64(2);
    parameters.insert("s").setInt64(7);

    expectSortedRows("MATCH (n:Person) RETURN n.name AS name ORDER BY name LIMIT $a "
                     "UNION "
                     "MATCH (i:Interest) RETURN i.name AS name ORDER BY name LIMIT $b",
                     parameters,
                     {{"Adam"}, {"Animals"}, {"Bio"}});
    expectSortedRows("MATCH (n:Person) RETURN n.name AS name ORDER BY name LIMIT $a "
                     "UNION ALL "
                     "MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP $s LIMIT $a",
                     parameters,
                     {{"Adam"}, {"Suhas"}});
}

TEST_F(LimitSkipParameterTest, cutsTheRowsOfACollectSubquery) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);
    parameters.insert("one").setInt64(1);

    expectRowsInOrder("MATCH (p:Person) WHERE p.name = 'Remy' "
                      "RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name ORDER BY i.name LIMIT $k }",
                      parameters,
                      {{"Computers, Eighties"}});
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc', 'Doruk'] "
                      "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name ORDER BY i.name SKIP $one LIMIT $one } "
                      "ORDER BY p.name",
                      parameters,
                      {{"Doruk", ""}, {"Luc", "Computers"}, {"Remy", "Eighties"}});
}

TEST_F(LimitSkipParameterTest, cutsTheRowsOfACountSubquery) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);

    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] "
                      "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP $k - 1 } ORDER BY p.name",
                      parameters,
                      {{"Adam", "1"}, {"Remy", "2"}});
    expectRowsInOrder("MATCH (p:Person) WHERE COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT $k } = 2 "
                      "RETURN p.name ORDER BY p.name",
                      parameters,
                      {{"Adam"}, {"Cyrus"}, {"Luc"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
    expectRowsInOrder("MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP $k } RETURN p.name",
                      parameters,
                      {{"Remy"}});
}

TEST_F(LimitSkipParameterTest, cutsTheRowsOfAWhenBranch) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);
    parameters.insert("one").setInt64(1);

    expectRowsInOrder("WHEN $k > 5 THEN RETURN 1 AS x "
                      "ELSE MATCH (n:Person) RETURN n.name AS x ORDER BY x SKIP $k + 4 LIMIT $one * 2",
                      parameters,
                      {{"Remy"}, {"Suhas"}});
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Adam', 'Luc'] "
                      "CALL (p) { WHEN p.name = 'Adam' THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS x ORDER BY x LIMIT $k - 1 "
                      "ELSE RETURN 'none' AS x } "
                      "RETURN p.name, x ORDER BY p.name, x",
                      parameters,
                      {{"Adam", "Bio"}, {"Luc", "none"}});
}

TEST_F(LimitSkipParameterTest, cutsTheWalksOfAVariableLengthPath) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(1);

    expectRowsInOrder("MATCH (a:Person {name: 'Remy'})-[:KNOWS_WELL*1..3]->(b) RETURN b.name ORDER BY b.name LIMIT $k",
                      parameters,
                      {{"Adam"}});
    expectRowCount("MATCH (a:Person)-[:INTERESTED_IN*1..2]->(b) RETURN a.name, b.name LIMIT $k", parameters, 1);
}

TEST_F(LimitSkipParameterTest, cutsTheRowsOfAJoin) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);
    parameters.insert("s").setInt64(5);

    expectRowsInOrder("MATCH (a:Person), (b:Person) WHERE a.name = b.name RETURN a.name ORDER BY a.name LIMIT $k",
                      parameters,
                      {{"Adam"}, {"Cyrus"}});
    expectRowCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name RETURN a.name LIMIT $k", parameters, 2);
    expectRowCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name RETURN a.name SKIP $s LIMIT $k", parameters, 2);
}

TEST_F(LimitSkipParameterTest, cutsTheRowsOfAnOptionalMatch) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(3);

    expectRowsInOrder("MATCH (p:Person) OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) "
                      "RETURN p.name, i.name ORDER BY p.name, i.name LIMIT $k",
                      parameters,
                      {{"Adam", "Bio"}, {"Adam", "Cooking"}, {"Cyrus", "Gym"}});
    expectRowCount("MATCH (p:Person) OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name LIMIT $k", parameters, 3);
}

TEST_F(LimitSkipParameterTest, cutsTheDistinctAndTheGroupedRows) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);

    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.name ORDER BY i.name LIMIT $k",
                      parameters,
                      {{"Animals"}, {"Bio"}});
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) RETURN i.name, count(p) AS c ORDER BY c DESC, i.name LIMIT $k",
                      parameters,
                      {{"Gym", "3"}, {"Bio", "2"}});
    expectRowCount("MATCH (p:Person)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.name LIMIT $k", parameters, 2);
}

TEST_F(LimitSkipParameterTest, compilesOneProgramForEveryBoundCount) {
    ParameterMap one;
    one.insert("n").setInt64(1);

    ParameterMap five;
    five.insert("n").setInt64(5);

    const std::vector<std::string_view> queries {
        "MATCH (n:Person) RETURN n.name LIMIT $n",
        "MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP $n LIMIT $n + 1",
        "MATCH (a:Person), (b:Person) WHERE a.name = b.name RETURN a.name LIMIT $n",
    };

    for (const std::string_view query : queries) {
        for (const std::string_view dialect : {"db", "nl"}) {
            std::string programForOne;
            programOf(dialect, query, one, programForOne);

            std::string programForFive;
            programOf(dialect, query, five, programForFive);

            EXPECT_EQ(programForOne, programForFive) << query;
        }
    }
}

TEST_F(LimitSkipParameterTest, rejectsANegativeCountWhenTheLoopStarts) {
    ParameterMap parameters;
    parameters.insert("n").setInt64(-1);

    runQueryExpectingError("MATCH (n:Person) RETURN n.name LIMIT $n", parameters, "LIMIT requires a non-negative integer, got -1");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $n", parameters, "LIMIT requires a non-negative integer, got -1");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name SKIP $n", parameters, "SKIP requires a non-negative integer, got -1");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name LIMIT $n - 1", parameters, "LIMIT requires a non-negative integer, got -2");
    runQueryExpectingError("MATCH (n:NoSuchLabel) RETURN n LIMIT $n", parameters, "LIMIT requires a non-negative integer, got -1");
}

TEST_F(LimitSkipParameterTest, rejectsANullCountWhenTheLoopStarts) {
    ParameterMap parameters;
    parameters.insert("s").setString("x");

    runQueryExpectingError("MATCH (n:Person) RETURN n.name LIMIT toInteger($s)", parameters, "LIMIT requires a non-negative integer, got null");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name SKIP toInteger($s)", parameters, "SKIP requires a non-negative integer, got null");
}

TEST_F(LimitSkipParameterTest, rejectsACountOfAnotherType) {
    ParameterMap parameters;
    parameters.insert("f").setDouble(1.5);
    parameters.insert("s").setString("2");

    runQueryExpectingError("MATCH (n:Person) RETURN n.name LIMIT $f", parameters, "LIMIT expression must be an integer");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name SKIP $s", parameters, "SKIP expression must be an integer");
}

TEST_F(LimitSkipParameterTest, rejectsACellOfAnotherTypeWhenTheLoopStarts) {
    ParameterMap parameters;

    ParameterValue::Map& map = parameters.insert("m").setMap();
    map["f"].setDouble(1.5);
    map["s"].setString("2");
    map["negative"].setInt64(-3);

    runQueryExpectingError("MATCH (n:Person) RETURN n.name LIMIT $m.f", parameters, "LIMIT requires a non-negative integer, got a Double");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name SKIP $m.s", parameters, "SKIP requires a non-negative integer, got a String");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name LIMIT $m.missing", parameters, "LIMIT requires a non-negative integer, got null");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $m.negative", parameters, "LIMIT requires a non-negative integer, got -3");
}

TEST_F(LimitSkipParameterTest, rejectsACountReadFromTheRows) {
    const ParameterMap parameters;

    runQueryExpectingError("MATCH (n:Person) RETURN n.name LIMIT n.age", parameters, "LIMIT expression");
    runQueryExpectingError("MATCH (n:Person) RETURN n.name SKIP n.age", parameters, "SKIP expression");
}
