#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An EXISTS, COUNT or COLLECT whose body runs one input row at a time - it cuts, sorts,
// aggregates, unions or branches - answers for every row it stands beside, wherever it
// stands: a CASE, a comprehension, a reduce, a list predicate, an UNWIND. Remy, Adam, Maxime
// and Luc are French; Martina has a PhD. Remy is interested in Ghosts, Computers and Eighties,
// Adam in Bio and Cooking, Maxime in Bio and Padel, Luc in Animals and Computers, Martina in
// Cooking, Suhas in Gym and JiuJitsu, Cyrus in Gym and Travel and Doruk in Gym. KNOWS_WELL
// leaves Remy for Adam and Adam for Remy.
class PerRowSubqueryExpressionTest : public WriteQueryTest {
};

TEST_F(PerRowSubqueryExpressionTest, decidesACaseCondition) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE "
               "  WHEN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } > 1 THEN 'many' "
               "  ELSE 'few' "
               "END",
               {{"Remy", "many"},
                {"Adam", "many"},
                {"Maxime", "many"},
                {"Luc", "many"},
                {"Martina", "few"},
                {"Suhas", "many"},
                {"Cyrus", "many"},
                {"Doruk", "few"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE "
               "  WHEN EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c WHERE c > 2 RETURN c } THEN 'curious' "
               "  WHEN EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() } THEN 'friend' "
               "  ELSE 'other' "
               "END",
               {{"Remy", "curious"},
                {"Adam", "friend"},
                {"Maxime", "other"},
                {"Luc", "other"},
                {"Martina", "other"},
                {"Suhas", "other"},
                {"Cyrus", "other"},
                {"Doruk", "other"}});
}

TEST_F(PerRowSubqueryExpressionTest, isTheValueOfACaseBranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE "
               "  WHEN p.isFrench THEN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n } "
               "  ELSE [] "
               "END",
               {{"Remy", "[Computers, Eighties, Ghosts]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE "
               "  WHEN p.isFrench THEN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } "
               "  WHEN p.hasPhD THEN COUNT { "
               "    MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n "
               "    UNION "
               "    MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS n "
               "  } "
               "  ELSE -1 "
               "END",
               {{"Remy", "2"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "-1"},
                {"Cyrus", "-1"},
                {"Doruk", "-1"}});
}

TEST_F(PerRowSubqueryExpressionTest, filtersTheElementsOfAComprehension) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, [x IN range(1, 3) WHERE x <= COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }]",
               {{"Remy", "[1, 2]"},
                {"Adam", "[1, 2]"},
                {"Maxime", "[1, 2]"},
                {"Luc", "[1, 2]"},
                {"Martina", "[1]"},
                {"Suhas", "[1, 2]"},
                {"Cyrus", "[1, 2]"},
                {"Doruk", "[1]"}});

    expectRows("RETURN [name IN ['Remy', 'Luc', 'Nobody'] "
               "  WHERE EXISTS { MATCH (q:Person) WHERE q.name = name RETURN q LIMIT 1 }]",
               {{"[Remy, Luc]"}});
}

TEST_F(PerRowSubqueryExpressionTest, projectsTheElementsOfAComprehension) {
    expectRows("RETURN [name IN ['Remy', 'Martina', 'Doruk'] | "
               "  COUNT { MATCH (q:Person)-[:INTERESTED_IN]->(i) WHERE q.name = name RETURN i LIMIT 2 }]",
               {{"[2, 1, 1]"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, [x IN [1, 2] | CASE "
               "  WHEN x = 1 THEN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } "
               "  ELSE 0 "
               "END]",
               {{"Remy", "[2, 0]"},
                {"Adam", "[2, 0]"},
                {"Maxime", "[2, 0]"},
                {"Luc", "[2, 0]"},
                {"Martina", "[1, 0]"},
                {"Suhas", "[2, 0]"},
                {"Cyrus", "[2, 0]"},
                {"Doruk", "[1, 0]"}});
}

TEST_F(PerRowSubqueryExpressionTest, foldsInAReduce) {
    expectRows("RETURN reduce(total = 0, name IN ['Remy', 'Adam', 'Doruk'] | "
               "  total + COUNT { MATCH (q:Person)-[:INTERESTED_IN]->(i) WHERE q.name = name RETURN i LIMIT 2 })",
               {{"5"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, reduce(total = 0, x IN range(1, 2) | "
               "  total + x * COUNT { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() })",
               {{"Remy", "3"},
                {"Adam", "3"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(PerRowSubqueryExpressionTest, decidesAListPredicate) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, "
               "       all(x IN range(1, 2) WHERE x <= COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }), "
               "       any(x IN range(2, 3) WHERE x <= COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 })",
               {{"Remy", "true", "true"},
                {"Adam", "true", "true"},
                {"Maxime", "true", "true"},
                {"Luc", "true", "true"},
                {"Martina", "false", "false"},
                {"Suhas", "true", "true"},
                {"Cyrus", "true", "true"},
                {"Doruk", "false", "false"}});
}

TEST_F(PerRowSubqueryExpressionTest, isUnwound) {
    expectRows("MATCH (p:Person) "
               "WHERE p.isFrench "
               "UNWIND COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n DESC LIMIT 1 } AS n "
               "RETURN p.name, n",
               {{"Remy", "Ghosts"}, {"Adam", "Cooking"}, {"Maxime", "Padel"}, {"Luc", "Computers"}});

    expectRows("MATCH (p:Person) "
               "WITH p, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n } AS names "
               "WHERE p.hasPhD "
               "UNWIND names AS n "
               "RETURN p.name, n",
               {{"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Luc", "Animals"},
                {"Luc", "Computers"},
                {"Martina", "Cooking"}});
}

TEST_F(PerRowSubqueryExpressionTest, keepsWhatWasReadBeforeIt) {
    expectRows("MATCH (p:Person) "
               "WITH p, p.name AS name "
               "RETURN name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }, p.hasPhD",
               {{"Remy", "2", "true"},
                {"Adam", "2", "true"},
                {"Maxime", "2", "false"},
                {"Luc", "2", "true"},
                {"Martina", "1", "true"},
                {"Suhas", "2", "false"},
                {"Cyrus", "2", "false"},
                {"Doruk", "1", "false"}});
}

TEST_F(PerRowSubqueryExpressionTest, feedsAnotherOne) {
    expectRows("MATCH (p:Person) "
               "WITH p, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c "
               "RETURN p.name, c, COLLECT { UNWIND range(1, c) AS x RETURN x ORDER BY x DESC }",
               {{"Remy", "2", "[2, 1]"},
                {"Adam", "2", "[2, 1]"},
                {"Maxime", "2", "[2, 1]"},
                {"Luc", "2", "[2, 1]"},
                {"Martina", "1", "[1]"},
                {"Suhas", "2", "[2, 1]"},
                {"Cyrus", "2", "[2, 1]"},
                {"Doruk", "1", "[1]"}});
}

TEST_F(PerRowSubqueryExpressionTest, isCutByAnOuterLimit) {
    expectRows("MATCH (p:Person) "
               "WITH p, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c "
               "LIMIT 3 "
               "RETURN count(*)",
               {{"3"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n LIMIT 1 } "
                      "ORDER BY p.name LIMIT 3",
                      {{"Adam", "[Bio]"}, {"Cyrus", "[Gym]"}, {"Doruk", "[Gym]"}});
}

TEST_F(PerRowSubqueryExpressionTest, filtersTheRows) {
    expectRows("MATCH (p:Person) "
               "WHERE p.hasPhD AND COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } > 1 "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Luc"}});
}
