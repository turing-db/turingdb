#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// CALL (p) { WHEN ... THEN { ... } ELSE { ... } }: for each row in flight, the body of the
// first branch whose predicate is true runs, and the ELSE body runs when none is
class ConditionalSubqueryTest : public WriteQueryTest {
};

TEST_F(ConditionalSubqueryTest, runsTheFirstBranchWhosePredicateHolds) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench AND p.hasPhD THEN { RETURN 'french doctor' AS kind } "
               "WHEN p.isFrench THEN { RETURN 'french' AS kind } "
               "ELSE { RETURN 'other' AS kind } "
               "} "
               "RETURN p.name, kind",
               {{"Remy", "french doctor"}, {"Adam", "french doctor"}, {"Luc", "french doctor"},
                {"Maxime", "french"},
                {"Martina", "other"}, {"Suhas", "other"}, {"Cyrus", "other"}, {"Doruk", "other"}});
}

TEST_F(ConditionalSubqueryTest, dropsTheRowsNoBranchRunsFor) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN { RETURN p.name AS name } } "
               "RETURN name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

// Only Remy and Adam have an age: the predicate is null for the 6 others
TEST_F(ConditionalSubqueryTest, takesANullPredicateAsFalse) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.age > 30 THEN { RETURN 'known' AS age } ELSE { RETURN 'unknown' AS age } } "
               "RETURN p.name, age",
               {{"Remy", "known"}, {"Adam", "known"},
                {"Maxime", "unknown"}, {"Luc", "unknown"}, {"Martina", "unknown"},
                {"Suhas", "unknown"}, {"Cyrus", "unknown"}, {"Doruk", "unknown"}});
}

// A count over no row is 0, so a branch that ran for the 4 others would give them a row
TEST_F(ConditionalSubqueryTest, runsNoAggregateOfABranchNotTaken) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS n } } "
               "RETURN p.name, n",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"}});
}

TEST_F(ConditionalSubqueryTest, aggregatesInTheBranchTaken) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS n } "
               "ELSE { MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) AS n } "
               "} "
               "RETURN p.name, n",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(ConditionalSubqueryTest, fansOutTheRowsOfTheBranchTaken) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.hasPhD THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS z } "
               "ELSE { RETURN 'none' AS z } "
               "} "
               "RETURN p.name, z",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                {"Adam", "Bio"}, {"Adam", "Cooking"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Maxime", "none"}, {"Suhas", "none"}, {"Cyrus", "none"}, {"Doruk", "none"}});
}

TEST_F(ConditionalSubqueryTest, ordersAndLimitsInsideABranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS z ORDER BY z LIMIT 1 } "
               "ELSE { RETURN 'none' AS z } "
               "} "
               "RETURN p.name, z",
               {{"Remy", "Computers"}, {"Adam", "Bio"}, {"Maxime", "Bio"}, {"Luc", "Animals"},
                {"Martina", "none"}, {"Suhas", "none"}, {"Cyrus", "none"}, {"Doruk", "none"}});
}

TEST_F(ConditionalSubqueryTest, evaluatesAConstantPredicate) {
    expectRows("CALL () { WHEN true THEN { RETURN 1 AS x } ELSE { RETURN 2 AS x } } RETURN x",
               {{"1"}});

    expectRows("CALL () { WHEN false THEN { RETURN 1 AS x } } RETURN x", {});

    expectRows("MATCH (p:Person) "
               "CALL () { WHEN false THEN { RETURN 1 AS x } ELSE { RETURN 2 AS x } } "
               "RETURN x, count(*)",
               {{"2", "8"}});
}

TEST_F(ConditionalSubqueryTest, readsAnImportedUnwoundValue) {
    expectRows("UNWIND [1, 2, 3] AS k "
               "CALL (k) { WHEN k > 1 THEN { RETURN k * 10 AS y } ELSE { RETURN 0 AS y } } "
               "RETURN k, y",
               {{"1", "0"}, {"2", "20"}, {"3", "30"}});
}

TEST_F(ConditionalSubqueryTest, takesBranchesWithoutBraces) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN RETURN 'fr' AS k ELSE RETURN 'other' AS k } "
               "RETURN k, count(*)",
               {{"fr", "4"}, {"other", "4"}});
}

TEST_F(ConditionalSubqueryTest, padsTheRowsNoBranchRunsForUnderOptional) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { WHEN p.age > 30 THEN { RETURN p.name AS n } } "
               "RETURN p.name, n",
               {{"Remy", "Remy"}, {"Adam", "Adam"},
                {"Maxime", "null"}, {"Luc", "null"}, {"Martina", "null"},
                {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(ConditionalSubqueryTest, nestsAConditionalInABranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench THEN { "
               "CALL (p) { WHEN p.hasPhD THEN { RETURN 'a' AS t } ELSE { RETURN 'b' AS t } } "
               "RETURN t "
               "} "
               "ELSE { RETURN 'c' AS t } "
               "} "
               "RETURN p.name, t",
               {{"Remy", "a"}, {"Adam", "a"}, {"Luc", "a"}, {"Maxime", "b"},
                {"Martina", "c"}, {"Suhas", "c"}, {"Cyrus", "c"}, {"Doruk", "c"}});
}

// Remy KNOWS_WELL Adam, who is interested in Bio and Cooking
TEST_F(ConditionalSubqueryTest, matchesFromTheNodesABranchReturns) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.name = 'Remy' THEN { MATCH (p)-[:KNOWS_WELL]->(f) RETURN f } } "
               "MATCH (f)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, i.name",
               {{"Remy", "Bio"}, {"Remy", "Cooking"}});
}

TEST_F(ConditionalSubqueryTest, typesAColumnOneBranchReturnsNullIn) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN { RETURN p.name AS n } ELSE { RETURN null AS n } } "
               "RETURN n",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"},
                {"null"}, {"null"}, {"null"}, {"null"}});
}

// Remy and Adam know each other well
TEST_F(ConditionalSubqueryTest, decidesOnAnExistsPredicate) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN EXISTS { (p)-[:KNOWS_WELL]->() } THEN { RETURN 'social' AS s } "
               "ELSE { RETURN 'alone' AS s } "
               "} "
               "RETURN s, count(*)",
               {{"social", "2"}, {"alone", "6"}});
}

TEST_F(ConditionalSubqueryTest, decidesOnAnImportedConstant) {
    expectRows("WITH 5 AS k MATCH (p:Person) "
               "CALL (k) { WHEN k > 3 THEN { RETURN 'big' AS s } } "
               "RETURN s, count(*)",
               {{"big", "8"}});
}

TEST_F(ConditionalSubqueryTest, unwindsAnImportedListInABranch) {
    expectRows("UNWIND [[1, 2], [3]] AS l "
               "CALL (l) { WHEN size(l) > 1 THEN { UNWIND l AS x RETURN x } ELSE { RETURN -1 AS x } } "
               "RETURN x",
               {{"1"}, {"2"}, {"-1"}});
}

TEST_F(ConditionalSubqueryTest, limitsTheRowsOfOneBranchPerRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1 } } "
               "RETURN count(*)",
               {{"4"}});
}

TEST_F(ConditionalSubqueryTest, dedupsInsideABranch) {
    expectRows("CALL () { "
               "WHEN true THEN { MATCH (:Person)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.name AS n } "
               "} "
               "RETURN count(n)",
               {{"10"}});
}

TEST_F(ConditionalSubqueryTest, chainsTwoConditionalCalls) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN { RETURN 'fr' AS nationality } ELSE { RETURN 'other' AS nationality } } "
               "CALL (p) { WHEN p.hasPhD THEN { RETURN 'phd' AS degree } ELSE { RETURN 'none' AS degree } } "
               "RETURN nationality, degree, count(*)",
               {{"fr", "phd", "3"}, {"fr", "none", "1"}, {"other", "phd", "1"}, {"other", "none", "3"}});
}

TEST_F(ConditionalSubqueryTest, aggregatesWhatTheBranchesReturn) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN { RETURN 1 AS one } ELSE { RETURN 2 AS one } } "
               "RETURN sum(one)",
               {{"12"}});
}

TEST_F(ConditionalSubqueryTest, filtersWhatTheBranchesReturn) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN { RETURN 'fr' AS k } ELSE { RETURN 'other' AS k } } "
               "WITH p, k WHERE k = 'fr' "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

TEST_F(ConditionalSubqueryTest, takesANullLiteralPredicateAsFalse) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN null THEN { RETURN 1 AS x } ELSE { RETURN 2 AS x } } "
               "RETURN x, count(*)",
               {{"2", "8"}});
}

TEST_F(ConditionalSubqueryTest, importsThroughALeadingWithOfABranch) {
    expectRows("MATCH (p:Person) "
               "CALL { WHEN true THEN { WITH p RETURN p.name AS n } } "
               "RETURN n",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"},
                {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ConditionalSubqueryTest, rejectsAPredicateReadingWhatABranchImports) {
    expectError("MATCH (p:Person) CALL { WHEN p.isFrench THEN { WITH p RETURN 1 AS x } } RETURN x",
                "Variable 'p' not found");
}

TEST_F(ConditionalSubqueryTest, rejectsAPredicateReadingAVariableNotImported) {
    expectError("MATCH (p:Person) CALL () { WHEN p.isFrench THEN { RETURN 1 AS x } } RETURN x",
                "Variable 'p' not found");
}

TEST_F(ConditionalSubqueryTest, rejectsBranchesReturningDifferentColumns) {
    expectError("MATCH (p:Person) "
                "CALL (p) { WHEN p.isFrench THEN { RETURN 1 AS x } ELSE { RETURN 2 AS y } } "
                "RETURN p",
                "All branches of a conditional query must return the same column names");

    expectError("MATCH (p:Person) "
                "CALL (p) { WHEN p.isFrench THEN { RETURN 1 AS x, 2 AS y } ELSE { RETURN 2 AS x } } "
                "RETURN p",
                "All branches of a conditional query must return the same number of columns");

    expectError("MATCH (p:Person) "
                "CALL (p) { WHEN p.isFrench THEN { RETURN 1 AS x } ELSE { SET p.visited = true } } "
                "RETURN p",
                "All branches of a conditional query must return the same number of columns");
}

TEST_F(ConditionalSubqueryTest, rejectsAPredicateThatIsNotABoolean) {
    expectError("MATCH (p:Person) CALL (p) { WHEN p.name THEN { RETURN 1 AS x } } RETURN x",
                "WHEN predicate must be a boolean");

    expectError("MATCH (p:Person) CALL (p) { WHEN count(p) > 1 THEN { RETURN 1 AS x } } RETURN x",
                "Invalid use of aggregate expression in this context");
}
