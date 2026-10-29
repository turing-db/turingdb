#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// EXISTS { WHEN ... THEN ... ELSE ... }: for each row, the body of the first branch whose
// predicate holds runs, and the EXISTS is true when it returns a row. Remy, Adam, Maxime and
// Luc are French; Remy, Adam, Luc and Martina have a PhD; only Remy and Adam have an age (32).
// Remy is interested in Ghosts, Computers and Eighties, Adam in Bio and Cooking, Maxime in Bio
// and Padel, Luc in Animals and Computers, Martina in Cooking, Suhas in Gym and JiuJitsu, Cyrus
// in Gym and Travel and Doruk in Gym. KNOWS_WELL leaves Remy for Adam and Adam for Remy.
class ConditionalExistsBodyTest : public WriteQueryTest {
};

TEST_F(ConditionalExistsBodyTest, isFalseForTheRowsNoBranchRunsFor) {
    expectRows("MATCH (p:Person) RETURN p.name, EXISTS { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->(:Exotic) }",
               {{"Remy", "true"},
                {"Adam", "false"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

// Luc has a PhD and no KNOWS_WELL edge: his first branch runs and finds nothing, and the
// second, which would have found his interests, does not run
TEST_F(ConditionalExistsBodyTest, runsOnlyTheFirstBranchWhosePredicateHolds) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.hasPhD THEN MATCH (p)-[:KNOWS_WELL]->() "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Gym'}) "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "true"},
                {"Cyrus", "true"},
                {"Doruk", "true"}});
}

TEST_F(ConditionalExistsBodyTest, takesANullPredicateAsFalse) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.age > 30 THEN MATCH (p)-[:KNOWS_WELL]->() "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->(:SleepDisturber) "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "true"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, evaluatesAConstantPredicate) {
    const Rows knowsWell = {{"Remy", "true"},
                            {"Adam", "true"},
                            {"Maxime", "false"},
                            {"Luc", "false"},
                            {"Martina", "false"},
                            {"Suhas", "false"},
                            {"Cyrus", "false"},
                            {"Doruk", "false"}};

    expectRows("MATCH (p:Person) RETURN p.name, EXISTS { WHEN true THEN MATCH (p)-[:KNOWS_WELL]->() }", knowsWell);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN false THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:KNOWS_WELL]->() "
               "}",
               knowsWell);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN null THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:KNOWS_WELL]->() "
               "}",
               knowsWell);
}

TEST_F(ConditionalExistsBodyTest, filtersTheRowsInAWhere) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->(:Exotic) "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Gym'}) "
               "} "
               "RETURN p.name",
               {{"Remy"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ConditionalExistsBodyTest, isNegatedByNot) {
    expectRows("MATCH (p:Person) "
               "WHERE NOT EXISTS { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->(:Exotic) "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Gym'}) "
               "} "
               "RETURN p.name",
               {{"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}});
}

TEST_F(ConditionalExistsBodyTest, filtersBesideAnotherPredicate) {
    expectRows("MATCH (p:Person) "
               "WHERE p.hasPhD AND EXISTS { "
               "  WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->() "
               "} "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Martina"}});

    expectRows("MATCH (p:Person) "
               "WHERE p.hasPhD OR EXISTS { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->({name: 'Padel'}) } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}});
}

// Two people share an interest when someone else is interested in it too: Remy and Luc share
// Computers, Adam shares Bio with Maxime and Cooking with Martina. Suhas, Cyrus and Doruk only
// share Gym with each other, and none of them is French.
TEST_F(ConditionalExistsBodyTest, readsTheRowInABranchWhere) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q) WHERE q <> p RETURN q "
               "  } "
               "  ELSE { "
               "    MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q) WHERE q.isFrench RETURN q "
               "  } "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "true"},
                {"Martina", "true"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

// A count returns one row over no match, so the branch answers true for every row it runs for
TEST_F(ConditionalExistsBodyTest, isTrueForABranchThatAggregates) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { WHEN p.isFrench THEN { MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) AS n } }",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "true"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

// Only Remy has more than 2 interests, and LIMIT 0 leaves no row to anyone else
TEST_F(ConditionalExistsBodyTest, cutsTheRowsOfABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP 2 } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 0 } "
               "}",
               {{"Remy", "true"},
                {"Adam", "false"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, sortsAndDedupsInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.isReal AS real ORDER BY real LIMIT 1 } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal RETURN i.name AS real ORDER BY real DESC } "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "true"},
                {"Martina", "false"},
                {"Suhas", "true"},
                {"Cyrus", "true"},
                {"Doruk", "true"}});
}

TEST_F(ConditionalExistsBodyTest, unwindsAListInFlight) {
    expectRows("UNWIND [[1, 2], [], [3], [0, 1]] AS l "
               "RETURN size(l), EXISTS { "
               "  WHEN size(l) > 1 THEN { UNWIND l AS x WITH x WHERE x > 1 RETURN x } "
               "  ELSE { UNWIND l AS x RETURN x } "
               "}",
               {{"2", "true"}, {"0", "false"}, {"1", "true"}, {"2", "false"}});
}

TEST_F(ConditionalExistsBodyTest, nestsAWhenInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { "
               "    WHEN p.hasPhD THEN MATCH (p)-[:KNOWS_WELL]->() "
               "    ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Padel'}) "
               "  } "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Travel'}) "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "true"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, holdsAUnionInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:KNOWS_WELL]->(x) RETURN x "
               "    UNION "
               "    MATCH (p)-[:INTERESTED_IN]->(x:Exotic) RETURN x "
               "  } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(x {name: 'Gym'}) RETURN x } "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "true"},
                {"Cyrus", "true"},
                {"Doruk", "true"}});
}

TEST_F(ConditionalExistsBodyTest, isAUnionOperand) {
    const Rows expected = {{"Remy", "true"},
                           {"Adam", "true"},
                           {"Maxime", "false"},
                           {"Luc", "false"},
                           {"Martina", "false"},
                           {"Suhas", "true"},
                           {"Cyrus", "true"},
                           {"Doruk", "true"}};

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->(x) RETURN x } "
               "  UNION "
               "  MATCH (p)-[:INTERESTED_IN]->(x {name: 'Gym'}) RETURN x "
               "}",
               expected);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  MATCH (p)-[:INTERESTED_IN]->(x {name: 'Gym'}) RETURN x "
               "  UNION ALL "
               "  { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->(x) RETURN x } "
               "}",
               expected);
}

// Martina has a PhD and no French nationality, and Maxime the other way round
TEST_F(ConditionalExistsBodyTest, isEveryOperandOfAUnion) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->(x) RETURN x } "
               "  UNION "
               "  { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->(x {name: 'Cooking'}) RETURN x "
               "    ELSE MATCH (p)-[:INTERESTED_IN]->(x {name: 'Travel'}) RETURN x } "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "true"},
                {"Suhas", "false"},
                {"Cyrus", "true"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, decidesOnAProjectedVariable) {
    expectRows("MATCH (p:Person) "
               "WITH p, p.isFrench AND p.hasPhD AS frenchDoctor "
               "RETURN p.name, EXISTS { WHEN frenchDoctor THEN MATCH (p)-[:INTERESTED_IN]->(:SoftwareEngineering) }",
               {{"Remy", "true"},
                {"Adam", "false"},
                {"Maxime", "false"},
                {"Luc", "true"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, decidesOnASubquery) {
    const Rows expected = {{"Remy", "true"},
                           {"Adam", "false"},
                           {"Maxime", "true"},
                           {"Luc", "false"},
                           {"Martina", "false"},
                           {"Suhas", "false"},
                           {"Cyrus", "false"},
                           {"Doruk", "false"}};

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN EXISTS { (p)-[:KNOWS_WELL]->() } THEN MATCH (p)-[:INTERESTED_IN]->(:Exotic) "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Bio'}) "
               "}",
               expected);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN (p)-[:KNOWS_WELL]->() THEN MATCH (p)-[:INTERESTED_IN]->(:Exotic) "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Bio'}) "
               "}",
               expected);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN COUNT { (p)-[:KNOWS_WELL]->() } > 0 THEN MATCH (p)-[:INTERESTED_IN]->(:Exotic) "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Bio'}) "
               "}",
               expected);
}

TEST_F(ConditionalExistsBodyTest, decidesOnARowAnOptionalMatchMissed) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, EXISTS { "
               "  WHEN f IS NULL THEN MATCH (p)-[:INTERESTED_IN]->(:SleepDisturber) "
               "  ELSE MATCH (f)-[:INTERESTED_IN]->(:Exotic) "
               "}",
               {{"Remy", "false"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "true"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, answersWithNoRowInFlight) {
    expectRows("RETURN EXISTS { WHEN true THEN RETURN 1 AS x }", {{"true"}});
    expectRows("RETURN EXISTS { WHEN false THEN RETURN 1 AS x }", {{"false"}});
    expectRows("RETURN EXISTS { WHEN false THEN RETURN 1 AS x ELSE MATCH (n:Person) RETURN 1 AS x }", {{"true"}});
    expectRows("RETURN EXISTS { WHEN false THEN RETURN 1 AS x ELSE MATCH (n:Nobody) RETURN 1 AS x }", {{"false"}});
}

TEST_F(ConditionalExistsBodyTest, groupsTheRowsOnItsAnswer) {
    expectRows("MATCH (p:Person) "
               "RETURN EXISTS { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->({name: 'Computers'}) "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Gym'}) "
               "} AS e, count(*) AS n",
               {{"true", "5"}, {"false", "3"}});

    expectRows("MATCH (p:Person) "
               "RETURN DISTINCT EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() } AS e",
               {{"true"}, {"false"}});
}

TEST_F(ConditionalExistsBodyTest, importsThroughALeadingWithOfABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { WITH p MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } "
               "  ELSE { WITH p AS q MATCH (q)-[:INTERESTED_IN]->({name: 'Cooking'}) RETURN q AS k } "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "true"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, followsAVariableLengthPatternInABranch) {
    expectRows("MATCH (i:Interest) "
               "RETURN i.name, EXISTS { "
               "  WHEN i.isReal THEN MATCH (i)-[:KNOWS_WELL*2]->(:Person {name: 'Adam'}) "
               "  ELSE MATCH (i)<-[:INTERESTED_IN]-(:Person {name: 'Martina'}) "
               "}",
               {{"Computers", "false"},
                {"Eighties", "false"},
                {"Bio", "false"},
                {"Cooking", "true"},
                {"Ghosts", "true"},
                {"Padel", "false"},
                {"Animals", "false"},
                {"Gym", "false"},
                {"Travel", "false"},
                {"JiuJitsu", "false"}});
}

TEST_F(ConditionalExistsBodyTest, holdsACallInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } RETURN k } "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalExistsBodyTest, rejectsAWritingBranch) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN p.isFrench THEN CREATE (:Thing) }",
                "An EXISTS subquery is read-only: its body cannot write to the graph");

    expectError("MATCH (p:Person) "
                "RETURN EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() ELSE { SET p.seen = true } }",
                "An EXISTS subquery is read-only: its body cannot write to the graph");
}

TEST_F(ConditionalExistsBodyTest, rejectsBranchesReturningDifferentColumns) {
    expectError("MATCH (p:Person) "
                "RETURN EXISTS { WHEN p.isFrench THEN { RETURN 1 AS x } ELSE { RETURN 2 AS y } }",
                "All branches of a conditional query must return the same column names");
}

TEST_F(ConditionalExistsBodyTest, rejectsAPredicateThatIsNotABoolean) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN p.name THEN MATCH (p)-[:KNOWS_WELL]->() }",
                "WHEN predicate must be a boolean");

    expectError("MATCH (p:Person) RETURN EXISTS { WHEN count(p) > 1 THEN MATCH (p)-[:KNOWS_WELL]->() }",
                "Invalid use of aggregate expression in this context");
}

TEST_F(ConditionalExistsBodyTest, rejectsAPredicateReadingAVariableOfABranch) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN k.isFrench THEN MATCH (p)-[:KNOWS_WELL]->(k) }",
                "Variable 'k' not found");
}
