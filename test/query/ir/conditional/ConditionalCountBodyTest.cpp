#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// COUNT { WHEN ... THEN ... ELSE ... }: for each row, the body of the first branch whose
// predicate holds runs, and the COUNT is the number of rows it returns, 0 when no branch runs.
// Remy, Adam, Maxime and Luc are French; Remy, Adam, Luc and Martina have a PhD; only Remy and
// Adam have an age (32). Remy is interested in Ghosts, Computers and Eighties, Adam in Bio and
// Cooking, Maxime in Bio and Padel, Luc in Animals and Computers, Martina in Cooking, Suhas in
// Gym and JiuJitsu, Cyrus in Gym and Travel and Doruk in Gym. KNOWS_WELL leaves Remy for Adam
// and Adam for Remy.
class ConditionalCountBodyTest : public WriteQueryTest {
};

TEST_F(ConditionalCountBodyTest, isZeroForTheRowsNoBranchRunsFor) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->() }",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "0"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

// Martina shares Cooking with Adam, and Suhas, Cyrus and Doruk each share Gym with the other two
TEST_F(ConditionalCountBodyTest, countsTheRowsOfTheBranchTaken) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->()<-[:INTERESTED_IN]-(q) WHERE q <> p "
               "}",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "2"}});
}

TEST_F(ConditionalCountBodyTest, runsOnlyTheFirstBranchWhosePredicateHolds) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.age IS NOT NULL THEN { MATCH (p)-[:KNOWS_WELL]->(x) RETURN x.name AS x } "
               "  WHEN p.hasPhD THEN { MATCH (p)-[:INTERESTED_IN]->(x) RETURN x.name AS x } "
               "  ELSE { UNWIND ['a', 'b', 'c', 'd'] AS x RETURN x } "
               "}",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "4"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "4"},
                {"Cyrus", "4"},
                {"Doruk", "4"}});
}

TEST_F(ConditionalCountBodyTest, takesANullPredicateAsFalse) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.age = 32 THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:KNOWS_WELL]->() "
               "}",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(ConditionalCountBodyTest, filtersTheRowsInAWhere) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({isReal: true}) "
               "} >= 2 "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

TEST_F(ConditionalCountBodyTest, countsOneRowForABranchThatAggregates) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS n } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN size(i.name) AS n } "
               "}",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

TEST_F(ConditionalCountBodyTest, countsTheRowsADistinctLeaves) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT p.isFrench AS v } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.isFrench AS v } "
               "}",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

TEST_F(ConditionalCountBodyTest, countsTheRowsACutLeaves) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 1 } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP 1 } "
               "}",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "0"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "0"}});
}

// Remy's and Luc's Computers is also SoftwareEngineering, which the UNION counts once and
// UNION ALL would count twice. Gym is the only interest the second UNION ALL operand matches.
TEST_F(ConditionalCountBodyTest, holdsAUnionInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n "
               "    UNION "
               "    MATCH (p)-[:INTERESTED_IN]->(i:SoftwareEngineering) RETURN i.name AS n "
               "  } "
               "  ELSE { "
               "    MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n "
               "    UNION ALL "
               "    MATCH (p)-[:INTERESTED_IN]->(i {name: 'Gym'}) RETURN i.name AS n "
               "  } "
               "}",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "3"},
                {"Cyrus", "3"},
                {"Doruk", "2"}});
}

TEST_F(ConditionalCountBodyTest, isAUnionAllOperand) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n } "
               "  UNION ALL "
               "  MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS n "
               "}",
               {{"Remy", "4"},
                {"Adam", "3"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

// The UNION dedups the rows of each person on their own: 'none' once, beside their interests
TEST_F(ConditionalCountBodyTest, isAUnionOperand) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n "
               "    ELSE RETURN 'none' AS n } "
               "  UNION "
               "  RETURN 'none' AS n "
               "}",
               {{"Remy", "4"},
                {"Adam", "3"},
                {"Maxime", "3"},
                {"Luc", "3"},
                {"Martina", "1"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"}});
}

TEST_F(ConditionalCountBodyTest, isEveryOperandOfAUnionAll) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  { WHEN p.isFrench THEN RETURN 'french' AS t } "
               "  UNION ALL "
               "  { WHEN p.hasPhD THEN RETURN 'doctor' AS t ELSE UNWIND [1, 2, 3] AS x RETURN 'other' AS t } "
               "}",
               {{"Remy", "2"},
                {"Adam", "2"},
                {"Maxime", "4"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "3"},
                {"Cyrus", "3"},
                {"Doruk", "3"}});
}

TEST_F(ConditionalCountBodyTest, nestsAWhenInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { "
               "    WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->() "
               "    ELSE MATCH (p)-[:KNOWS_WELL]->() "
               "  } "
               "  ELSE { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->()<-[:INTERESTED_IN]-(q) WHERE q <> p } "
               "}",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "0"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(ConditionalCountBodyTest, readsARealInterestThroughAWith) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) WITH i WHERE i.isReal RETURN i } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) WITH i, i.name AS n WHERE n STARTS WITH 'G' RETURN i } "
               "}",
               {{"Remy", "2"},
                {"Adam", "0"},
                {"Maxime", "0"},
                {"Luc", "2"},
                {"Martina", "0"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"}});
}

TEST_F(ConditionalCountBodyTest, countsTheRowAnOptionalMatchPads) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { WHEN p.isFrench THEN { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } }",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

// Remy reaches Adam in one hop and himself back in two, and Adam the other way round
TEST_F(ConditionalCountBodyTest, followsAVariableLengthPatternInABranch) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL*1..2]->() }",
               {{"Remy", "2"},
                {"Adam", "2"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

// Each of Remy's interests is held by Remy and, for Computers, by Luc as well
TEST_F(ConditionalCountBodyTest, chainsSegmentsInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:INTERESTED_IN]->(i) RETURN i "
               "    NEXT "
               "    MATCH (i)<-[:INTERESTED_IN]-(q) RETURN q "
               "  } "
               "}",
               {{"Remy", "4"},
                {"Adam", "4"},
                {"Maxime", "3"},
                {"Luc", "3"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(ConditionalCountBodyTest, countsOverTheRowsOfAnUnwind) {
    expectRows("UNWIND [1, 2, 3, 4] AS x "
               "RETURN x, COUNT { "
               "  WHEN x % 2 = 0 THEN { UNWIND range(1, x) AS y RETURN y } "
               "  ELSE { MATCH (p:Person) WHERE p.isFrench RETURN p.age AS y } "
               "}",
               {{"1", "4"}, {"2", "2"}, {"3", "4"}, {"4", "4"}});
}

TEST_F(ConditionalCountBodyTest, readsAGroupingKey) {
    expectRows("MATCH (p:Person) "
               "WITH p.isFrench AS french, collect(p) AS people "
               "RETURN french, COUNT { "
               "  WHEN french THEN { UNWIND people AS q MATCH (q)-[:INTERESTED_IN]->() RETURN q } "
               "  ELSE { UNWIND people AS q RETURN q } "
               "}",
               {{"true", "9"}, {"false", "4"}});
}

TEST_F(ConditionalCountBodyTest, isSummedByAnAggregate) {
    expectRows("MATCH (p:Person) RETURN sum(COUNT { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() })",
               {{"9"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.hasPhD, sum(COUNT { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Gym'}) "
               "})",
               {{"true", "7"}, {"false", "5"}});
}

TEST_F(ConditionalCountBodyTest, ordersTheRows) {
    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name "
                      "ORDER BY COUNT { "
                      "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() "
                      "  ELSE MATCH (p)-[:KNOWS_WELL]->() "
                      "} DESC, p.name",
                      {{"Remy"}, {"Adam"}, {"Luc"}, {"Maxime"}, {"Cyrus"}, {"Doruk"}, {"Martina"}, {"Suhas"}});
}

TEST_F(ConditionalCountBodyTest, answersWithNoRowInFlight) {
    expectRows("RETURN COUNT { WHEN true THEN MATCH (p:Person) }", {{"8"}});
    expectRows("RETURN COUNT { WHEN false THEN MATCH (p:Person) }", {{"0"}});
    expectRows("RETURN COUNT { WHEN false THEN MATCH (p:Person) ELSE MATCH (i:Interest) }", {{"10"}});
}

TEST_F(ConditionalCountBodyTest, rejectsAWritingBranch) {
    expectError("MATCH (p:Person) RETURN COUNT { WHEN p.isFrench THEN { CREATE (t:Thing) RETURN t } }",
                "A COUNT subquery is read-only: its body cannot write to the graph");
}

TEST_F(ConditionalCountBodyTest, rejectsBranchesReturningDifferentColumns) {
    expectError("MATCH (p:Person) "
                "RETURN COUNT { WHEN p.isFrench THEN { RETURN 1 AS x } ELSE { RETURN 2 AS y } }",
                "All branches of a conditional query must return the same column names");

    expectError("MATCH (p:Person) "
                "RETURN COUNT { WHEN p.isFrench THEN { RETURN 1 AS x } ELSE MATCH (p)-[:KNOWS_WELL]->() }",
                "All branches of a conditional query must return the same number of columns");
}
