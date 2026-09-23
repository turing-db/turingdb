#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// EXISTS { ... }: one boolean per row in flight, true for a row the body produces a row of
// its own for. Remy and Adam are the only two people a KNOWS_WELL edge leaves; every person
// is INTERESTED_IN something, and only Martina and Doruk in one thing alone.
class ExistsSubqueryTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    // The rows as the query emitted them, for a test of the order itself
    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was expected to fail";
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ExistsSubqueryTest, patternShorthandKeepsTheRowsItHoldsFor) {
    expectRows("MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS_WELL]->() } RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(ExistsSubqueryTest, negatingItKeepsTheOtherSix) {
    expectRows("MATCH (p:Person) WHERE NOT EXISTS { (p)-[:KNOWS_WELL]->() } RETURN p.name",
               {{"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// The expression stands beside the row rather than cutting it: all 8 people come back
TEST_F(ExistsSubqueryTest, returnsTheBooleanForEveryRow) {
    expectRows("MATCH (p:Person) RETURN p.name, EXISTS { (p)-[:KNOWS_WELL]->() }",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ExistsSubqueryTest, matchBodyReadsTheRowItAnswersFor) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' } "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ExistsSubqueryTest, whereInThePatternShorthandCutsTheBodyRows) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Computers' } "
               "RETURN p.name",
               {{"Remy"}, {"Luc"}});
}

// A body naming nothing of the scope around it answers the same for every row
TEST_F(ExistsSubqueryTest, anUncorrelatedBodyHoldsForEveryRow) {
    expectRows("MATCH (p:Person) WHERE EXISTS { (i:Interest) } RETURN count(p)", {{"8"}});
}

TEST_F(ExistsSubqueryTest, anUncorrelatedBodyThatMatchesNothingHoldsForNoRow) {
    expectRows("MATCH (p:Person) WHERE EXISTS { (i:Interest {name: 'Skydiving'}) } RETURN count(p)",
               {{"0"}});
}

// Nothing is in flight, so the body joins onto the single empty row the query starts from
TEST_F(ExistsSubqueryTest, answersOverNoRowInFlight) {
    expectRows("RETURN EXISTS { (p:Person) }", {{"true"}});
    expectRows("RETURN EXISTS { (p:Person {name: 'Nobody'}) }", {{"false"}});
}

TEST_F(ExistsSubqueryTest, standsBesideOtherPredicates) {
    expectRows("MATCH (p:Person) "
               "WHERE p.hasPhD = true AND EXISTS { (p)-[:KNOWS_WELL]->() } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}});

    expectRows("MATCH (p:Person) "
               "WHERE p.name = 'Doruk' OR EXISTS { (p)-[:KNOWS_WELL]->() } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Doruk"}});
}

// A second hop inside the body: the people who share an interest with somebody else
TEST_F(ExistsSubqueryTest, walksATwoHopBody) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) WHERE q.name <> p.name } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ExistsSubqueryTest, nestsInsideAnotherExists) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { (p)-[:INTERESTED_IN]->(i) WHERE EXISTS { (i)-[:KNOWS_WELL]->() } } "
               "RETURN p.name",
               {{"Remy"}});
}

// A barrier inside the body carries the row tag on, so the rows past it still say which
// input row they came from: the five people interested in something real
TEST_F(ExistsSubqueryTest, carriesTheTagPastABarrierInTheBody) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WITH i WHERE i.isReal = true RETURN i } "
               "RETURN p.name",
               {{"Remy"}, {"Luc"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}


// Both factors of the product are in flight, so the body reads the pair: the 15 edges
TEST_F(ExistsSubqueryTest, answersForEachFactorOfACrossProduct) {
    expectRows("MATCH (p:Person), (i:Interest) WHERE EXISTS { (p)-[:INTERESTED_IN]->(i) } RETURN p.name, i.name",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                {"Adam", "Bio"}, {"Adam", "Cooking"},
                {"Maxime", "Bio"}, {"Maxime", "Padel"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"}, {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

TEST_F(ExistsSubqueryTest, answersOnTheRowsAnOptionalMatchLeft) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(k) "
               "RETURN p.name, k.name, EXISTS { (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' }",
               {{"Remy", "Adam", "false"},
                {"Adam", "Remy", "false"},
                {"Maxime", "null", "false"},
                {"Luc", "null", "false"},
                {"Martina", "null", "false"},
                {"Suhas", "null", "true"},
                {"Cyrus", "null", "true"},
                {"Doruk", "null", "true"}});
}

// A WITH inside the body does not descope the row the EXISTS answers for: the pattern below
// it still reads that row's own p. Were p descoped, a pattern naming it would bind a fresh
// scan instead and the body would answer for anyone who knows somebody well - Remy, Luc,
// Suhas, Cyrus and Doruk - rather than for Remy alone.
TEST_F(ExistsSubqueryTest, aBarrierInTheBodyKeepsTheRowItAnswersFor) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WITH i WHERE i.isReal = true "
               "               MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } "
               "RETURN p.name",
               {{"Remy"}});
}

// The same over a WITH that binds a constant and projects nothing of the body
TEST_F(ExistsSubqueryTest, aConstantBarrierInTheBodyKeepsTheRowItAnswersFor) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { WITH 'Gym' AS wanted "
               "               MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = wanted } "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// A body that aggregates, dedups, sorts, skips or limits cannot keep its rows paired with
// the ones it was given, so it runs one input row at a time. One test per clause that
// triggers it, each answering for a proper subset of the eight people so a body answering
// for the wrong rows shows up as the wrong set rather than as all of them.
//
// Three people are INTERESTED_IN Gym; six have a second interest, Martina and Doruk one.

TEST_F(ExistsSubqueryTest, anAggregatingBodyRunsOneRowAtATime) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS interests WHERE interests > 1 RETURN interests } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

TEST_F(ExistsSubqueryTest, aDedupingBodyRunsOneRowAtATime) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN DISTINCT i.name AS interest } "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ExistsSubqueryTest, aSortingBodyRunsOneRowAtATime) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i.name AS interest ORDER BY interest } "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// Only the six with a second interest survive the SKIP
TEST_F(ExistsSubqueryTest, aSkippingBodyRunsOneRowAtATime) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP 1 } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

TEST_F(ExistsSubqueryTest, aLimitingBodyRunsOneRowAtATime) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 } "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// The cut a MATCH inside the body carries stops it carrying rows too, not just the RETURN's
TEST_F(ExistsSubqueryTest, aMatchCutInTheBodyRunsItOneRowAtATime) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) SKIP 1 RETURN i } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

// Both the ORDER BY and the SKIP would trigger it on their own; together they still do
TEST_F(ExistsSubqueryTest, runsABodyEndingOnACutOneRowAtATime) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY interest SKIP 1 } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

// A LIMIT inside a body run per row budgets that row alone, so every person still answers.
// The bare LIMIT streams its budget; the one under an ORDER BY is fused into the sort's
// top-K and carries no handle, so both shapes are checked.
TEST_F(ExistsSubqueryTest, budgetsALimitInThePerRowBodyPerRow) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1 } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});

    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY interest LIMIT 1 } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// The rest of the query runs inside the loop a per-row body opened, so a later part still
// reads the rows the EXISTS answered for
TEST_F(ExistsSubqueryTest, carriesOnPastAPerRowBody) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1 } "
               "WITH p MATCH (p)-[:KNOWS_WELL]->(k) "
               "RETURN p.name, k.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

// A keyless count yields a row whatever it counted, so the body holds for every person
TEST_F(ExistsSubqueryTest, anAggregatingBodyHoldsForEveryRow) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) AS known } "
               "RETURN count(p)",
               {{"8"}});
}

// A barrier reads the boolean back as an ordinary column
TEST_F(ExistsSubqueryTest, publishesThroughAWith) {
    expectRows("MATCH (p:Person) "
               "WITH p, EXISTS { (p)-[:KNOWS_WELL]->() } AS knows "
               "WHERE knows "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(ExistsSubqueryTest, standsInACaseExpression) {
    expectRows("MATCH (p:Person) "
               "WHERE p.name = 'Remy' OR p.name = 'Luc' "
               "RETURN p.name, CASE WHEN EXISTS { (p)-[:KNOWS_WELL]->() } THEN 'knows' ELSE 'alone' END",
               {{"Remy", "knows"}, {"Luc", "alone"}});
}

// The body compares a property of the row it answers for against one of its own
TEST_F(ExistsSubqueryTest, readsAPropertyOfTheRowItAnswersFor) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { (p)-[:KNOWS_WELL]->(k) WHERE k.age = p.age } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

// Nothing row-varying is in flight behind the aggregate, so the body answers for the one
// row the count left
TEST_F(ExistsSubqueryTest, answersBesideAnAggregate) {
    expectRows("MATCH (p:Person) WITH count(p) AS people RETURN people, EXISTS { (i:Interest) }",
               {{"8", "true"}});
}

TEST_F(ExistsSubqueryTest, answersOverUnwoundRows) {
    expectRows("UNWIND [1, 2] AS x RETURN x, EXISTS { (p:Person) }",
               {{"1", "true"}, {"2", "true"}});
}

TEST_F(ExistsSubqueryTest, comparesAgainstABoolean) {
    expectRows("MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS_WELL]->() } = true RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

// The key varies per row, so the six people no KNOWS_WELL edge leaves come first
TEST_F(ExistsSubqueryTest, ordersOnTheBoolean) {
    expectOrderedRows("MATCH (p:Person) RETURN p.name ORDER BY EXISTS { (p)-[:KNOWS_WELL]->() }, p.name",
                      {{"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"},
                       {"Adam"}, {"Remy"}});
}

TEST_F(ExistsSubqueryTest, bindsNothingOutsideItself) {
    expectError("MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS_WELL]->(k) } RETURN k.name",
                "Variable 'k' not found");
}

TEST_F(ExistsSubqueryTest, readsTheGraphOnly) {
    expectError("MATCH (p:Person) WHERE EXISTS { MATCH (p) CREATE (n:Person) } RETURN p.name",
                "read-only");
}
