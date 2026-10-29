#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// COLLECT { WHEN ... THEN ... ELSE ... }: for each row, the body of the first branch whose
// predicate holds runs, and the COLLECT is the list of the values it returns, [] when no branch
// runs. Remy, Adam, Maxime and Luc are French; Remy, Adam, Luc and Martina have a PhD; only Remy
// and Adam have an age (32). Remy is interested in Ghosts, Computers and Eighties, Adam in Bio
// and Cooking, Maxime in Bio and Padel, Luc in Animals and Computers, Martina in Cooking, Suhas
// in Gym and JiuJitsu, Cyrus in Gym and Travel and Doruk in Gym. KNOWS_WELL leaves Remy for Adam,
// Adam for Remy and Ghosts for Remy. Eighties is not real; Bio, Cooking and Padel say nothing.
class ConditionalCollectBodyTest : public WriteQueryTest {
};

TEST_F(ConditionalCollectBodyTest, collectsTheRowsOfTheBranchTaken) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN toUpper(i.name) AS n ORDER BY n } "
               "}",
               {{"Remy", "[Computers, Eighties, Ghosts]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[COOKING]"},
                {"Suhas", "[GYM, JIUJITSU]"},
                {"Cyrus", "[GYM, TRAVEL]"},
                {"Doruk", "[GYM]"}});
}

TEST_F(ConditionalCollectBodyTest, isEmptyForTheRowsNoBranchRunsFor) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS n }",
               {{"Remy", "[Adam]"},
                {"Adam", "[Remy]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(ConditionalCollectBodyTest, runsOnlyTheFirstBranchWhosePredicateHolds) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.age IS NOT NULL THEN RETURN 'aged' AS k "
               "  WHEN p.hasPhD THEN RETURN 'doctor' AS k "
               "  WHEN p.isFrench THEN RETURN 'french' AS k "
               "  ELSE RETURN 'other' AS k "
               "}",
               {{"Remy", "[aged]"},
                {"Adam", "[aged]"},
                {"Maxime", "[french]"},
                {"Luc", "[doctor]"},
                {"Martina", "[doctor]"},
                {"Suhas", "[other]"},
                {"Cyrus", "[other]"},
                {"Doruk", "[other]"}});
}

TEST_F(ConditionalCollectBodyTest, takesANullPredicateAsFalse) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { WHEN p.age > 30 THEN RETURN p.age AS a ELSE RETURN -1 AS a }",
               {{"Remy", "[32]"},
                {"Adam", "[32]"},
                {"Maxime", "[-1]"},
                {"Luc", "[-1]"},
                {"Martina", "[-1]"},
                {"Suhas", "[-1]"},
                {"Cyrus", "[-1]"},
                {"Doruk", "[-1]"}});
}

TEST_F(ConditionalCollectBodyTest, sortsAndCutsInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.hasPhD THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n DESC LIMIT 2 } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n SKIP 1 } "
               "}",
               {{"Remy", "[Ghosts, Eighties]"},
                {"Adam", "[Cooking, Bio]"},
                {"Maxime", "[Padel]"},
                {"Luc", "[Computers, Animals]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[JiuJitsu]"},
                {"Cyrus", "[Travel]"},
                {"Doruk", "[]"}});
}

TEST_F(ConditionalCollectBodyTest, collectsBranchesOfDifferentTypes) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.age IS NOT NULL THEN RETURN p.age AS v "
               "  WHEN p.hasPhD THEN RETURN p.name AS v "
               "  ELSE RETURN p.isFrench AS v "
               "}",
               {{"Remy", "[32]"},
                {"Adam", "[32]"},
                {"Maxime", "[true]"},
                {"Luc", "[Luc]"},
                {"Martina", "[Martina]"},
                {"Suhas", "[false]"},
                {"Cyrus", "[false]"},
                {"Doruk", "[false]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { UNWIND [p.age, p.name] AS v RETURN v } "
               "  ELSE { RETURN [p.name] AS v } "
               "}",
               {{"Remy", "[32, Remy]"},
                {"Adam", "[32, Adam]"},
                {"Maxime", "[null, Maxime]"},
                {"Luc", "[null, Luc]"},
                {"Martina", "[[Martina]]"},
                {"Suhas", "[[Suhas]]"},
                {"Cyrus", "[[Cyrus]]"},
                {"Doruk", "[[Doruk]]"}});
}

TEST_F(ConditionalCollectBodyTest, keepsTheNullsABranchReturns) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { WHEN p.hasPhD THEN RETURN p.age AS v ELSE RETURN null AS v }",
               {{"Remy", "[32]"},
                {"Adam", "[32]"},
                {"Maxime", "[null]"},
                {"Luc", "[null]"},
                {"Martina", "[null]"},
                {"Suhas", "[null]"},
                {"Cyrus", "[null]"},
                {"Doruk", "[null]"}});
}

TEST_F(ConditionalCollectBodyTest, collectsNodes) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, [k IN COLLECT { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal RETURN i "
               "} | k.name]",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, size(COLLECT { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]-(k) RETURN k })",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(ConditionalCollectBodyTest, collectsWhatABranchAggregates) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS n } "
               "  ELSE { RETURN 0 AS n } "
               "}",
               {{"Remy", "[3]"},
                {"Adam", "[2]"},
                {"Maxime", "[2]"},
                {"Luc", "[2]"},
                {"Martina", "[0]"},
                {"Suhas", "[0]"},
                {"Cyrus", "[0]"},
                {"Doruk", "[0]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN collect(i.name) AS n } "
               "  ELSE { RETURN [] AS n } "
               "}",
               {{"Remy", "[[Ghosts, Computers, Eighties]]"},
                {"Adam", "[[Bio, Cooking]]"},
                {"Maxime", "[[Bio, Padel]]"},
                {"Luc", "[[Animals, Computers]]"},
                {"Martina", "[[]]"},
                {"Suhas", "[[]]"},
                {"Cyrus", "[[]]"},
                {"Doruk", "[[]]"}});
}

TEST_F(ConditionalCollectBodyTest, collectsTheRowsADistinctLeaves) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT p.isFrench AS v } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.isFrench AS v } "
               "}",
               {{"Remy", "[true]"},
                {"Adam", "[true]"},
                {"Maxime", "[true]"},
                {"Luc", "[true]"},
                {"Martina", "[false]"},
                {"Suhas", "[false, false]"},
                {"Cyrus", "[false, false]"},
                {"Doruk", "[false]"}});
}

// Adam knows Remy well both ways, which the UNION collects once and UNION ALL twice
TEST_F(ConditionalCollectBodyTest, holdsAUnionInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS n "
               "    UNION "
               "    MATCH (p)<-[:KNOWS_WELL]-(k) RETURN k.name AS n "
               "  } "
               "  ELSE { RETURN 'none' AS n } "
               "}",
               {{"Remy", "[Adam, Ghosts]"},
                {"Adam", "[Remy]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[none]"},
                {"Suhas", "[none]"},
                {"Cyrus", "[none]"},
                {"Doruk", "[none]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS n "
               "    UNION ALL "
               "    MATCH (p)<-[:KNOWS_WELL]-(k) RETURN k.name AS n "
               "  } "
               "  ELSE { RETURN 'none' AS n } "
               "}",
               {{"Remy", "[Adam, Adam, Ghosts]"},
                {"Adam", "[Remy, Remy]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[none]"},
                {"Suhas", "[none]"},
                {"Cyrus", "[none]"},
                {"Doruk", "[none]"}});
}

TEST_F(ConditionalCollectBodyTest, isAUnionAllOperand) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  { WHEN p.isFrench THEN RETURN 'french' AS t ELSE RETURN 'other' AS t } "
               "  UNION ALL "
               "  { WHEN p.hasPhD THEN RETURN 'doctor' AS t } "
               "}",
               {{"Remy", "[french, doctor]"},
                {"Adam", "[french, doctor]"},
                {"Maxime", "[french]"},
                {"Luc", "[french, doctor]"},
                {"Martina", "[other, doctor]"},
                {"Suhas", "[other]"},
                {"Cyrus", "[other]"},
                {"Doruk", "[other]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS t "
               "  UNION ALL "
               "  { WHEN p.isFrench THEN RETURN 'french' AS t } "
               "}",
               {{"Remy", "[Adam, french]"},
                {"Adam", "[Remy, french]"},
                {"Maxime", "[french]"},
                {"Luc", "[french]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(ConditionalCollectBodyTest, isAUnionOperand) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  { WHEN p.isFrench THEN RETURN 'x' AS t } "
               "  UNION "
               "  { WHEN p.hasPhD THEN RETURN 'x' AS t } "
               "}",
               {{"Remy", "[x]"},
                {"Adam", "[x]"},
                {"Maxime", "[x]"},
                {"Luc", "[x]"},
                {"Martina", "[x]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  { WHEN p.isFrench THEN RETURN 'french' AS t } "
               "  UNION "
               "  { WHEN p.hasPhD THEN RETURN 'doctor' AS t } "
               "  UNION "
               "  RETURN 'french' AS t "
               "}",
               {{"Remy", "[french, doctor]"},
                {"Adam", "[french, doctor]"},
                {"Maxime", "[french]"},
                {"Luc", "[french, doctor]"},
                {"Martina", "[doctor, french]"},
                {"Suhas", "[french]"},
                {"Cyrus", "[french]"},
                {"Doruk", "[french]"}});
}

TEST_F(ConditionalCollectBodyTest, nestsAWhenInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { WHEN p.hasPhD THEN RETURN 'french doctor' AS t ELSE RETURN 'french' AS t } "
               "  ELSE { WHEN p.hasPhD THEN RETURN 'doctor' AS t } "
               "}",
               {{"Remy", "[french doctor]"},
                {"Adam", "[french doctor]"},
                {"Maxime", "[french]"},
                {"Luc", "[french doctor]"},
                {"Martina", "[doctor]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(ConditionalCollectBodyTest, nestsAConditionalCollectInABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:INTERESTED_IN]->(i) "
               "    RETURN COLLECT { WHEN i.isReal THEN RETURN i.name AS n ELSE RETURN '?' AS n } AS n "
               "  } "
               "}",
               {{"Remy", "[[Ghosts], [Computers], [?]]"},
                {"Adam", "[[?], [?]]"},
                {"Maxime", "[[?], [?]]"},
                {"Luc", "[[Animals], [Computers]]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(ConditionalCollectBodyTest, isAnOperandOfAListExpression) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, "
               "       COLLECT { WHEN p.isFrench THEN RETURN 'french' AS t } "
               "       + COLLECT { WHEN p.hasPhD THEN RETURN 'doctor' AS t }",
               {{"Remy", "[french, doctor]"},
                {"Adam", "[french, doctor]"},
                {"Maxime", "[french]"},
                {"Luc", "[french, doctor]"},
                {"Martina", "[doctor]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n } "
               "}[0]",
               {{"Remy", "Computers"},
                {"Adam", "Bio"},
                {"Maxime", "Bio"},
                {"Luc", "Animals"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(ConditionalCollectBodyTest, isUnwoundAfterAWith) {
    expectRows("MATCH (p:Person) "
               "WITH p, COLLECT { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n } AS names "
               "UNWIND names AS n "
               "RETURN p.name, n",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Maxime", "Bio"},
                {"Maxime", "Padel"},
                {"Luc", "Animals"},
                {"Luc", "Computers"}});
}

TEST_F(ConditionalCollectBodyTest, returnsAPatternComprehensionFromABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN RETURN [(p)-[:INTERESTED_IN]->(i) | i.name] AS n "
               "  ELSE RETURN [] AS n "
               "}",
               {{"Remy", "[[Ghosts, Computers, Eighties]]"},
                {"Adam", "[[Bio, Cooking]]"},
                {"Maxime", "[[Bio, Padel]]"},
                {"Luc", "[[Animals, Computers]]"},
                {"Martina", "[[]]"},
                {"Suhas", "[[]]"},
                {"Cyrus", "[[]]"},
                {"Doruk", "[[]]"}});
}

TEST_F(ConditionalCollectBodyTest, readsAGroupingKey) {
    expectRows("MATCH (p:Person) "
               "WITH p.hasPhD AS phd, collect(p.name) AS names "
               "RETURN phd, COLLECT { "
               "  WHEN phd THEN { UNWIND names AS n RETURN n ORDER BY n } "
               "  ELSE { UNWIND names AS n RETURN size(n) AS n ORDER BY n } "
               "}",
               {{"true", "[Adam, Luc, Martina, Remy]"}, {"false", "[5, 5, 5, 6]"}});
}

TEST_F(ConditionalCollectBodyTest, answersWithNoRowInFlight) {
    expectRows("RETURN COLLECT { WHEN false THEN RETURN 1 AS x ELSE UNWIND [1, 2] AS x RETURN x }", {{"[1, 2]"}});
    expectRows("RETURN COLLECT { WHEN true THEN MATCH (p:Person) WHERE p.age = 32 RETURN p.name AS n ORDER BY n }",
               {{"[Adam, Remy]"}});
    expectRows("RETURN COLLECT { WHEN null THEN RETURN 1 AS x }", {{"[]"}});
}

TEST_F(ConditionalCollectBodyTest, rejectsABranchReturningTwoColumns) {
    expectError("MATCH (p:Person) "
                "RETURN COLLECT { WHEN p.isFrench THEN RETURN p.name AS a ELSE RETURN p.name AS a, p.age AS b }",
                "A COLLECT subquery must return exactly one column, not 2");
}

TEST_F(ConditionalCollectBodyTest, rejectsABranchWithNoReturn) {
    expectError("MATCH (p:Person) "
                "RETURN COLLECT { WHEN p.isFrench THEN RETURN p.name AS a ELSE MATCH (p)-[:KNOWS_WELL]->() }",
                "A COLLECT subquery must end with a RETURN clause");
}

TEST_F(ConditionalCollectBodyTest, rejectsBranchesReturningDifferentColumns) {
    expectError("MATCH (p:Person) "
                "RETURN COLLECT { WHEN p.isFrench THEN RETURN p.name AS a ELSE RETURN p.name AS b }",
                "All branches of a conditional query must return the same column names");
}

TEST_F(ConditionalCollectBodyTest, rejectsAWritingBranch) {
    expectError("MATCH (p:Person) "
                "RETURN COLLECT { WHEN p.isFrench THEN RETURN p.name AS a ELSE { CREATE (t:Thing) RETURN t AS a } }",
                "A COLLECT subquery is read-only: its body cannot write to the graph");
}
