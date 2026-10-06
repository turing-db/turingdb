#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// EXISTS { WHEN ... } and COUNT { WHEN ... }: the body is correlated, and only the branch
// it takes for a row answers for it
class ConditionalSubqueryExpressionTest : public WriteQueryTest {
};

// The French who know someone well, and the others interested in the gym
TEST_F(ConditionalSubqueryExpressionTest, filtersOnAnExistsThatTakesABranch) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { "
               "WHEN p.isFrench THEN { MATCH (p)-[:KNOWS_WELL]->(f) RETURN p AS who } "
               "ELSE { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN p AS who } "
               "} "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ConditionalSubqueryExpressionTest, answersAnExistsWhoseBranchesReturnNothing) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() "
               "ELSE MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' "
               "}",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "true"}, {"Cyrus", "true"}, {"Doruk", "true"}});
}

TEST_F(ConditionalSubqueryExpressionTest, answersFalseWhereNoBranchIsTaken) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() }",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}});
}

// Only Remy and Adam have an age, 32
TEST_F(ConditionalSubqueryExpressionTest, answersForTheRowsABranchKeeps) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { WHEN true THEN { WITH p WHERE p.age > 30 } }",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}});
}

TEST_F(ConditionalSubqueryExpressionTest, computesNothingOfABranchNotTaken) {
    expectRows("WITH 0 AS d MATCH (p:Person) "
               "RETURN EXISTS { WHEN d = 0 THEN RETURN 1 AS x ELSE RETURN 10 / d AS x } AS e, count(*)",
               {{"true", "8"}});

    expectRows("WITH 0 AS d MATCH (p:Person) "
               "RETURN COUNT { WHEN d = 0 THEN RETURN 1 AS x WHEN 10 / d > 1 THEN RETURN 2 AS x } AS c, count(*)",
               {{"1", "8"}});
}

TEST_F(ConditionalSubqueryExpressionTest, groupsOnAnExistsThatTakesABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() } AS social, count(*)",
               {{"true", "2"}, {"false", "6"}});
}

// The French count every interest, the others only the gym
TEST_F(ConditionalSubqueryExpressionTest, countsTheRowsOfTheBranchTaken) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS thing } "
               "ELSE { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i.name AS thing } "
               "}",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "0"}, {"Suhas", "1"}, {"Cyrus", "1"}, {"Doruk", "1"}});
}

TEST_F(ConditionalSubqueryExpressionTest, countsTheRowsOfABranchThatReturnsNothing) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->() }",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "0"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(ConditionalSubqueryExpressionTest, countsTheDistinctRowsOfTheBranchTaken) {
    expectRows("RETURN COUNT { WHEN true THEN { MATCH (:Person)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.name AS n } } AS c",
               {{"10"}});
}

TEST_F(ConditionalSubqueryExpressionTest, filtersOnACount) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() ELSE MATCH (p)-[:KNOWS_WELL]->() } > 2 "
               "RETURN p.name",
               {{"Remy"}});
}

TEST_F(ConditionalSubqueryExpressionTest, rejectsABranchThatWrites) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN true THEN { CREATE (:Tag) RETURN 1 AS x } }",
                "An EXISTS subquery is read-only: its body cannot write to the graph");
    expectError("MATCH (p:Person) RETURN COUNT { WHEN true THEN { CREATE (:Tag) RETURN 1 AS x } }",
                "A COUNT subquery is read-only: its body cannot write to the graph");
}

TEST_F(ConditionalSubqueryExpressionTest, rejectsBranchesReturningDifferentColumns) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN p.isFrench THEN RETURN 1 AS x ELSE RETURN 2 AS y }",
                "All branches of a conditional query must return the same column names");
    expectError("MATCH (p:Person) RETURN COUNT { WHEN p.isFrench THEN RETURN 1 AS x ELSE MATCH (p)-->() }",
                "All branches of a conditional query must return the same number of columns");
}

TEST_F(ConditionalSubqueryExpressionTest, rejectsAPredicateThatIsNotABoolean) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN p.name THEN RETURN 1 AS x }",
                "WHEN predicate must be a boolean");
}

TEST_F(ConditionalSubqueryExpressionTest, rejectsAUnionInABranch) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN true THEN { RETURN 1 AS x UNION RETURN 2 AS x } }",
                "Not implemented: UNION inside a WHEN branch of an EXISTS body");
    expectError("MATCH (p:Person) RETURN COUNT { WHEN true THEN { RETURN 1 AS x UNION RETURN 2 AS x } }",
                "Not implemented: UNION inside a WHEN branch of a COUNT body");
}

TEST_F(ConditionalSubqueryExpressionTest, decidesOnAnEntityAMergeWrote) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n "
                    "RETURN n.name, EXISTS { WHEN n.age IS NULL THEN RETURN 1 AS x }, "
                    "COUNT { WHEN n.age IS NULL THEN RETURN 1 AS x ELSE { UNWIND [1, 2] AS y RETURN y AS x } }",
                    {{"Nia", "true", "1"}, {"Remy", "false", "2"}});
}
