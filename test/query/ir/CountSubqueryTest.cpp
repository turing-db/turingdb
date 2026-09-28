#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryStatus.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// COUNT { ... }: one count per row in flight, the number of rows the body produces for it.
// INTERESTED_IN leaves Remy 3 times, Martina and Doruk once and the other five people twice.
// KNOWS_WELL leaves Remy and Adam once each. Of the 10 interests, 6 are real: Remy and Luc
// have 2 of them, Suhas and Cyrus 2, Doruk 1, and Adam, Maxime and Martina none.
class CountSubqueryTest : public WriteQueryTest {
protected:
    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was expected to fail";
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(CountSubqueryTest, patternShorthandCountsTheRowsItMatches) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->() }",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, countsZeroForARowTheBodyMatchesNothingFor) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { (p)-[:KNOWS_WELL]->() }",
               {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(CountSubqueryTest, filtersOnTheCount) {
    expectRows("MATCH (p:Person) WHERE COUNT { (p)-[:INTERESTED_IN]->() } > 1 RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

TEST_F(CountSubqueryTest, matchBodyReadsTheRowItCountsFor) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal = true }",
               {{"Remy", "2"}, {"Adam", "0"}, {"Maxime", "0"}, {"Luc", "2"},
                {"Martina", "0"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, whereInThePatternShorthandCutsTheBodyRows) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' } = 1 "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(CountSubqueryTest, readsAPropertyOfTheRowItCountsFor) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { (p)-[:KNOWS_WELL]->(k) WHERE k.age = p.age }",
               {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(CountSubqueryTest, isCaseInsensitive) {
    expectRows("MATCH (p:Person) WHERE count { (p)-[:KNOWS_WELL]->() } = 1 RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(CountSubqueryTest, anUncorrelatedBodyCountsTheSameForEveryRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Doruk' RETURN p.name, COUNT { (i:Interest) }",
               {{"Remy", "10"}, {"Doruk", "10"}});
}

TEST_F(CountSubqueryTest, countsOverNoRowInFlight) {
    expectRows("RETURN COUNT { (p:Person) }", {{"8"}});
    expectRows("RETURN COUNT { MATCH (n) }", {{"18"}});
    expectRows("RETURN COUNT { (p:Person {name: 'Nobody'}) }", {{"0"}});
}

// Pairs of an interest and another person sharing it
TEST_F(CountSubqueryTest, walksATwoHopBody) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) WHERE q.name <> p.name }",
               {{"Remy", "1"}, {"Adam", "2"}, {"Maxime", "1"}, {"Luc", "1"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "2"}});
}

// Computers, Bio, Cooking and Gym are the interests more than one person shares
TEST_F(CountSubqueryTest, nestsInsideAnotherCount) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE COUNT { (i)<-[:INTERESTED_IN]-() } > 1 }",
               {{"Remy", "1"}, {"Adam", "2"}, {"Maxime", "1"}, {"Luc", "1"},
                {"Martina", "1"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "1"}});
}

// Ghosts is the one interest that knows somebody well
TEST_F(CountSubqueryTest, nestsAnExists) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE EXISTS { (i)-[:KNOWS_WELL]->() } } > 0 "
               "RETURN p.name",
               {{"Remy"}});
}

TEST_F(CountSubqueryTest, carriesTheTagPastABarrierInTheBody) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WITH i WHERE i.isReal = true RETURN i }",
               {{"Remy", "2"}, {"Adam", "0"}, {"Maxime", "0"}, {"Luc", "2"},
                {"Martina", "0"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

// Were p descoped by the WITH, the second MATCH would bind a fresh scan and count Adam's
// KNOWS_WELL edge for Luc, Suhas, Cyrus and Doruk too
TEST_F(CountSubqueryTest, aBarrierInTheBodyKeepsTheRowItCountsFor) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WITH i WHERE i.isReal = true "
               "                       MATCH (p)-[:KNOWS_WELL]->(k) RETURN k }",
               {{"Remy", "2"}, {"Adam", "0"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(CountSubqueryTest, aConstantBarrierInTheBodyKeepsTheRowItCountsFor) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { WITH 'Gym' AS wanted MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = wanted } = 1 "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(CountSubqueryTest, aKeylessAggregateYieldsOneRowForEveryRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Maxime' "
               "RETURN p.name, COUNT { MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) AS known }",
               {{"Remy", "1"}, {"Maxime", "1"}});
}

TEST_F(CountSubqueryTest, anAggregatingBodyCountsItsGroups) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.isReal AS real, count(i) AS interests }",
               {{"Remy", "2"}, {"Adam", "1"}, {"Maxime", "1"}, {"Luc", "1"},
                {"Martina", "1"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, aDedupingBodyCountsDistinctRows) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.isReal AS real }",
               {{"Remy", "2"}, {"Adam", "1"}, {"Maxime", "1"}, {"Luc", "1"},
                {"Martina", "1"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, aSortingBodyCountsEveryRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY interest }",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, aSkippingBodyCountsTheRowsPastTheSkip) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP 1 }",
               {{"Remy", "2"}, {"Adam", "1"}, {"Maxime", "1"}, {"Luc", "1"},
                {"Martina", "0"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "0"}});
}

TEST_F(CountSubqueryTest, aMatchCutInTheBodyCountsTheRowsPastTheCut) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) SKIP 1 RETURN i }",
               {{"Remy", "2"}, {"Adam", "1"}, {"Maxime", "1"}, {"Luc", "1"},
                {"Martina", "0"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "0"}});
}

// The bare LIMIT streams its budget; the one under an ORDER BY is fused into the sort's
// top-K. Either budgets each row alone.
TEST_F(CountSubqueryTest, budgetsALimitInTheBodyPerRow) {
    const Rows expected {{"Remy", "2"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                         {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}};

    expectRows("MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }",
               expected);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY interest LIMIT 2 }",
               expected);
}

TEST_F(CountSubqueryTest, aPerRowBodyStandsInAConjunction) {
    expectRows("MATCH (p:Person) "
               "WHERE p.hasPhD = true AND COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } = 2 "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Luc"}});
}

TEST_F(CountSubqueryTest, twoPerRowBodiesStandSideBySide) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, "
               "       COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }, "
               "       COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.isReal AS real }",
               {{"Remy", "2", "2"}, {"Adam", "2", "1"}, {"Maxime", "2", "1"}, {"Luc", "2", "1"},
                {"Martina", "1", "1"}, {"Suhas", "2", "1"}, {"Cyrus", "2", "1"}, {"Doruk", "1", "1"}});
}

TEST_F(CountSubqueryTest, carriesOnPastAPerRowBody) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1 } = 1 "
               "WITH p MATCH (p)-[:KNOWS_WELL]->(k) "
               "RETURN p.name, k.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

// Remy's interests are real, real and not, and the one person he knows well has no isReal
TEST_F(CountSubqueryTest, aUnionCountsTheDistinctRowsOfItsBranches) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.isReal AS real "
               "                       UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.isReal AS real }",
               {{"Remy", "3"}, {"Adam", "1"}, {"Maxime", "1"}, {"Luc", "1"},
                {"Martina", "1"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, aUnionAllCountsEveryRowOfItsBranches) {
    const Rows expected {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
                         {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}};

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.isReal AS real "
               "                       UNION ALL MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.isReal AS real }",
               expected);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }",
               expected);
}

// UNION is left associative: A UNION ALL B UNION C is distinct(A ++ B ++ C), and
// A UNION B UNION ALL C is distinct(A ++ B) ++ C
TEST_F(CountSubqueryTest, aMixedUnionDedupsUpToItsLastUnion) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                       UNION ALL MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                       UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }",
               {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                       UNION MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                       UNION ALL MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name }",
               {{"Remy", "6"}, {"Adam", "4"}, {"Maxime", "4"}, {"Luc", "4"},
                {"Martina", "2"}, {"Suhas", "4"}, {"Cyrus", "4"}, {"Doruk", "2"}});
}

TEST_F(CountSubqueryTest, sumsBesideAnotherCount) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->() } + COUNT { (p)-[:KNOWS_WELL]->() }",
               {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, publishesThroughAWith) {
    expectRows("MATCH (p:Person) "
               "WITH p, COUNT { (p)-[:INTERESTED_IN]->() } AS interests "
               "WHERE interests = 1 "
               "RETURN p.name, interests",
               {{"Martina", "1"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, standsInACaseExpression) {
    expectRows("MATCH (p:Person) "
               "WHERE p.name = 'Remy' OR p.name = 'Martina' "
               "RETURN p.name, CASE WHEN COUNT { (p)-[:INTERESTED_IN]->() } > 1 THEN 'many' ELSE 'one' END",
               {{"Remy", "many"}, {"Martina", "one"}});
}

TEST_F(CountSubqueryTest, ordersOnTheCount) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name ORDER BY COUNT { (p)-[:INTERESTED_IN]->() } DESC, p.name",
                      {{"Remy"},
                       {"Adam"}, {"Cyrus"}, {"Luc"}, {"Maxime"}, {"Suhas"},
                       {"Doruk"}, {"Martina"}});
}

TEST_F(CountSubqueryTest, groupsOnTheCount) {
    expectRows("MATCH (p:Person) RETURN COUNT { (p)-[:INTERESTED_IN]->() } AS interests, count(p) AS people",
               {{"1", "2"}, {"2", "5"}, {"3", "1"}});
}

TEST_F(CountSubqueryTest, countsBesideAnAggregate) {
    expectRows("MATCH (p:Person) WITH count(p) AS people RETURN people, COUNT { (i:Interest) }",
               {{"8", "10"}});
}

TEST_F(CountSubqueryTest, countsOverUnwoundRows) {
    expectRows("UNWIND [1, 2] AS x RETURN x, COUNT { (p:Person) }",
               {{"1", "8"}, {"2", "8"}});

    expectRows("UNWIND ['Remy', 'Doruk'] AS name "
               "RETURN name, COUNT { MATCH (p:Person)-[:INTERESTED_IN]->() WHERE p.name = name }",
               {{"Remy", "3"}, {"Doruk", "1"}});
}

TEST_F(CountSubqueryTest, countsForEachFactorOfACrossProduct) {
    expectRows("MATCH (p:Person), (i:Interest {name: 'Gym'}) RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->(i) }",
               {{"Remy", "0"}, {"Adam", "0"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "1"}});
}

// A null node matches no pattern, so the six people nobody knows well count 0
TEST_F(CountSubqueryTest, countsOnTheRowsAnOptionalMatchLeft) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(k) "
               "RETURN p.name, k.name, COUNT { (k)-[:INTERESTED_IN]->() }",
               {{"Remy", "Adam", "2"}, {"Adam", "Remy", "3"},
                {"Maxime", "null", "0"}, {"Luc", "null", "0"}, {"Martina", "null", "0"},
                {"Suhas", "null", "0"}, {"Cyrus", "null", "0"}, {"Doruk", "null", "0"}});
}

TEST_F(CountSubqueryTest, setsAPropertyToTheCount) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) "
                    "SET p.interests = COUNT { (p)-[:INTERESTED_IN]->() } "
                    "RETURN p.interests",
                    {{"3"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.interests", {{"3"}});
}

TEST_F(CountSubqueryTest, bindsNothingOutsideItself) {
    expectError("MATCH (p:Person) WHERE COUNT { (p)-[:KNOWS_WELL]->(k) } > 0 RETURN k.name",
                "Variable 'k' not found");
}

TEST_F(CountSubqueryTest, readsTheGraphOnly) {
    expectError("MATCH (p:Person) WHERE COUNT { MATCH (p) CREATE (n:Person) } > 0 RETURN p.name",
                "read-only");
}

TEST_F(CountSubqueryTest, needsAReturnInEveryBranchOfAUnion) {
    expectError("MATCH (p:Person) RETURN COUNT { MATCH (p)-->(x) UNION MATCH (p)<--(y) }",
                "must end with a RETURN clause");
}

TEST_F(CountSubqueryTest, needsTheSameColumnsInEveryBranchOfAUnion) {
    expectError("MATCH (p:Person) RETURN COUNT { MATCH (p)-->(x) RETURN x UNION ALL MATCH (p)<--(y) RETURN y }",
                "same column names");
}
