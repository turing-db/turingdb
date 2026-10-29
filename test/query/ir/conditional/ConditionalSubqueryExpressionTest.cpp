#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An EXISTS, COUNT or COLLECT whose body is a WHEN, in every place an expression can stand:
// a CALL body, a CASE, an aggregate, a SET, a WHEN predicate, after a cut or a grouping, and
// nested in another such body. Remy, Adam, Maxime and Luc are French; Remy, Adam, Luc and
// Martina have a PhD; only Remy and Adam have an age (32). Remy is interested in Ghosts,
// Computers and Eighties, Adam in Bio and Cooking, Maxime in Bio and Padel, Luc in Animals and
// Computers, Martina in Cooking, Suhas in Gym and JiuJitsu, Cyrus in Gym and Travel and Doruk
// in Gym. KNOWS_WELL leaves Remy for Adam and Adam for Remy.
class ConditionalSubqueryExpressionTest : public WriteQueryTest {
};

TEST_F(ConditionalSubqueryExpressionTest, standsBesideTheOtherKinds) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, "
               "       EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() }, "
               "       COUNT { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->() }, "
               "       COLLECT { WHEN p.age IS NOT NULL THEN RETURN p.age AS a }",
               {{"Remy", "true", "3", "[32]"},
                {"Adam", "true", "2", "[32]"},
                {"Maxime", "false", "0", "[]"},
                {"Luc", "false", "2", "[]"},
                {"Martina", "false", "1", "[]"},
                {"Suhas", "false", "0", "[]"},
                {"Cyrus", "false", "0", "[]"},
                {"Doruk", "false", "0", "[]"}});
}

TEST_F(ConditionalSubqueryExpressionTest, decidesTheBranchOfAConditionalCall) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN COUNT { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() } > 2 THEN RETURN 'many' AS k "
               "  ELSE RETURN 'few' AS k "
               "} "
               "RETURN p.name, k",
               {{"Remy", "many"},
                {"Adam", "few"},
                {"Maxime", "few"},
                {"Luc", "few"},
                {"Martina", "few"},
                {"Suhas", "few"},
                {"Cyrus", "few"},
                {"Doruk", "few"}});

    expectRows("WHEN EXISTS { WHEN true THEN MATCH (p:Person {name: 'Remy'}) } THEN RETURN 1 AS x ELSE RETURN 2 AS x",
               {{"1"}});
}

TEST_F(ConditionalSubqueryExpressionTest, standsInACallBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  RETURN EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() } AS e, "
               "         COUNT { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->() } AS c "
               "} "
               "RETURN p.name, e, c",
               {{"Remy", "true", "3"},
                {"Adam", "true", "2"},
                {"Maxime", "false", "0"},
                {"Luc", "false", "2"},
                {"Martina", "false", "1"},
                {"Suhas", "false", "0"},
                {"Cyrus", "false", "0"},
                {"Doruk", "false", "0"}});
}

TEST_F(ConditionalSubqueryExpressionTest, standsInABranchOfAConditionalCall) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN p.isFrench THEN "
               "    RETURN COUNT { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->() ELSE MATCH (p)-[:KNOWS_WELL]-() } AS c "
               "  ELSE RETURN -1 AS c "
               "} "
               "RETURN p.name, c",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "0"},
                {"Luc", "2"},
                {"Martina", "-1"},
                {"Suhas", "-1"},
                {"Cyrus", "-1"},
                {"Doruk", "-1"}});
}

TEST_F(ConditionalSubqueryExpressionTest, standsInABranchOfAStandaloneWhen) {
    expectRows("WHEN true THEN "
               "  MATCH (p:Person) "
               "  RETURN p.name AS n, COUNT { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() } AS c "
               "ELSE RETURN 'none' AS n, 0 AS c",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

// Computers is the only real interest more than one person holds: Remy and Luc
TEST_F(ConditionalSubqueryExpressionTest, nestsInTheWhereOfABranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:INTERESTED_IN]->(i) "
               "    WHERE COUNT { WHEN i.isReal THEN MATCH (i)<-[:INTERESTED_IN]-() } > 1 "
               "  } "
               "}",
               {{"Remy", "true"},
                {"Adam", "false"},
                {"Maxime", "false"},
                {"Luc", "true"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(ConditionalSubqueryExpressionTest, standsInACase) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE "
               "  WHEN EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() } THEN 'friend' "
               "  WHEN COUNT { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->() } > 1 THEN 'curious' "
               "  ELSE 'other' "
               "END",
               {{"Remy", "friend"},
                {"Adam", "friend"},
                {"Maxime", "other"},
                {"Luc", "curious"},
                {"Martina", "other"},
                {"Suhas", "other"},
                {"Cyrus", "other"},
                {"Doruk", "other"}});
}

TEST_F(ConditionalSubqueryExpressionTest, feedsAnAggregate) {
    expectRows("MATCH (p:Person) "
               "RETURN p.isFrench, "
               "       sum(size(COLLECT { WHEN p.hasPhD THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i })), "
               "       count(EXISTS { WHEN p.hasPhD THEN MATCH (p)-[:KNOWS_WELL]->() })",
               {{"true", "7", "4"}, {"false", "1", "4"}});
}

TEST_F(ConditionalSubqueryExpressionTest, filtersAGroup) {
    expectRows("MATCH (p:Person) "
               "WITH p.isFrench AS french, count(*) AS n "
               "WHERE EXISTS { WHEN french THEN MATCH (q:Person) WHERE q.isFrench AND q.age = 32 } "
               "RETURN french, n",
               {{"true", "4"}});
}

TEST_F(ConditionalSubqueryExpressionTest, readsTheRowsACutLeaves) {
    expectRows("MATCH (p:Person) "
               "WITH p ORDER BY p.name LIMIT 3 "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Gym'}) "
               "}",
               {{"Adam", "2"}, {"Cyrus", "1"}, {"Doruk", "1"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, COLLECT { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS k } "
                      "ORDER BY p.name DESC LIMIT 3",
                      {{"Suhas", "[]"}, {"Remy", "[Adam]"}, {"Maxime", "[]"}});
}

// The branches read both variables of the row: the person and the interest
TEST_F(ConditionalSubqueryExpressionTest, readsEveryVariableOfTheRow) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, i.name, EXISTS { "
               "  WHEN p.isFrench THEN MATCH (i)<-[:INTERESTED_IN]-(q) WHERE q <> p "
               "  ELSE MATCH (i)<-[:INTERESTED_IN]-(:Bioinformatics) "
               "}",
               {{"Remy", "Ghosts", "false"},
                {"Remy", "Computers", "true"},
                {"Remy", "Eighties", "false"},
                {"Adam", "Bio", "true"},
                {"Adam", "Cooking", "true"},
                {"Maxime", "Bio", "true"},
                {"Maxime", "Padel", "false"},
                {"Luc", "Animals", "false"},
                {"Luc", "Computers", "true"},
                {"Martina", "Cooking", "true"},
                {"Suhas", "Gym", "false"},
                {"Suhas", "JiuJitsu", "false"},
                {"Cyrus", "Gym", "false"},
                {"Cyrus", "Travel", "false"},
                {"Doruk", "Gym", "false"}});
}

// i is the Gym node in flight, not a variable the branch binds anew
TEST_F(ConditionalSubqueryExpressionTest, readsANodeInFlightInAPattern) {
    expectRows("MATCH (p:Person), (i:Interest {name: 'Gym'}) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->(x) WHERE x <> i "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->(i) "
               "}",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "0"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"}});
}

TEST_F(ConditionalSubqueryExpressionTest, isWrittenBySet) {
    applyWrite("MATCH (p:Person) "
               "SET p.score = COUNT { "
               "  WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() "
               "  ELSE MATCH (p)-[:KNOWS_WELL]->() "
               "}, "
               "p.tags = COLLECT { WHEN p.hasPhD THEN RETURN 'doctor' AS t ELSE RETURN 'student' AS t }");

    expectRows("MATCH (p:Person) RETURN p.name, p.score, p.tags",
               {{"Remy", "3", "[doctor]"},
                {"Adam", "2", "[doctor]"},
                {"Maxime", "2", "[student]"},
                {"Luc", "2", "[doctor]"},
                {"Martina", "0", "[doctor]"},
                {"Suhas", "0", "[student]"},
                {"Cyrus", "0", "[student]"},
                {"Doruk", "0", "[student]"}});
}

TEST_F(ConditionalSubqueryExpressionTest, filtersTheRowsASetWrites) {
    applyWrite("MATCH (p:Person) "
               "WHERE EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() ELSE MATCH (p)-[:INTERESTED_IN]->({name: 'Gym'}) } "
               "SET p.social = true");

    expectRows("MATCH (p:Person) WHERE p.social RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ConditionalSubqueryExpressionTest, isAPropertyOfACreatedNode) {
    applyWrite("MATCH (p:Person) "
               "CREATE (:Card {name: p.name, friends: COLLECT { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS n }})");

    expectRows("MATCH (c:Card) RETURN c.name, c.friends",
               {{"Remy", "[Adam]"},
                {"Adam", "[Remy]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}
