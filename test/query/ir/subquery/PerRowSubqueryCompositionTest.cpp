#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

const Rows interestCountsCappedAtTwo = {{"Adam", "2"},
                                        {"Cyrus", "2"},
                                        {"Doruk", "1"},
                                        {"Luc", "2"},
                                        {"Martina", "1"},
                                        {"Maxime", "2"},
                                        {"Remy", "2"},
                                        {"Suhas", "2"}};

}

// A COUNT, EXISTS or COLLECT body run one row at a time, composed with the clauses around it.
// INTERESTED_IN leaves Remy 3 times, Martina and Doruk once and the other five people twice.
// Gym has 3 people, Computers, Bio and Cooking 2, every other interest 1.
class PerRowSubqueryCompositionTest : public WriteQueryTest {
};

TEST_F(PerRowSubqueryCompositionTest, ordersOnThePerRowValueBeforeTheLimit) {
    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 2 } AS c "
                      "ORDER BY c DESC, p.name "
                      "LIMIT 3",
                      {{"Adam", "2"}, {"Cyrus", "2"}, {"Luc", "2"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name "
                      "ORDER BY COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i } DESC, p.name "
                      "LIMIT 3",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 2 } AS c "
                      "ORDER BY p.name "
                      "SKIP 5 "
                      "LIMIT 2",
                      {{"Maxime", "2"}, {"Remy", "2"}});
}

TEST_F(PerRowSubqueryCompositionTest, followsAWithThatSortsAndCuts) {
    expectRows("MATCH (p:Person) "
               "WITH p ORDER BY p.name DESC LIMIT 3 "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n DESC LIMIT 1 } AS top",
               {{"Suhas", "[JiuJitsu]"}, {"Remy", "[Ghosts]"}, {"Maxime", "[Padel]"}});

    expectRows("MATCH (p:Person) "
               "WITH p ORDER BY COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i } DESC, p.name SKIP 1 LIMIT 3 "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 1 } AS c",
               {{"Adam", "1"}, {"Cyrus", "1"}, {"Luc", "1"}});

    expectRows("MATCH (p:Person) "
               "WITH p ORDER BY p.name SKIP 2 "
               "RETURN count(*), sum(COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 })",
               {{"6", "10"}});
}

TEST_F(PerRowSubqueryCompositionTest, filtersOnThePerRowValueBeforeTheLimit) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 2 } = 1 "
               "RETURN p.name "
               "ORDER BY p.name DESC "
               "LIMIT 1",
               {{"Martina"}});

    expectRows("MATCH (p:Person) "
               "WITH p, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c "
               "WHERE c = 1 "
               "RETURN p.name, c "
               "LIMIT 2",
               {{"Martina", "1"}, {"Doruk", "1"}});

    expectRows("MATCH (p:Person) "
               "WITH p, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c "
               "MATCH (p)-[:INTERESTED_IN]->(:Interest {name: 'Gym'}) "
               "RETURN p.name, c "
               "LIMIT 3",
               {{"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "WITH p, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c "
                      "MATCH (p)-[:INTERESTED_IN]->(:Interest {name: 'Gym'}) "
                      "RETURN p.name, c "
                      "ORDER BY p.name "
                      "LIMIT 2",
                      {{"Cyrus", "2"}, {"Doruk", "1"}});

    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "RETURN DISTINCT p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(j) RETURN j LIMIT 2 } AS c "
                      "ORDER BY p.name "
                      "LIMIT 4",
                      {{"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "UNWIND range(1, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }) AS k "
                      "WITH p, k WHERE k = 2 "
                      "RETURN p.name "
                      "ORDER BY p.name "
                      "LIMIT 3",
                      {{"Adam"}, {"Cyrus"}, {"Luc"}});
}

TEST_F(PerRowSubqueryCompositionTest, dedupsAndAggregatesThePerRowValue) {
    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN DISTINCT COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c "
                      "ORDER BY c",
                      {{"1"}, {"2"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c, count(*) AS n "
                      "ORDER BY c",
                      {{"1", "2"}, {"2", "6"}});

    expectRows("MATCH (p:Person) "
               "RETURN collect(COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name DESC LIMIT 1 }) AS cs",
               {{"[1, 1, 1, 1, 1, 1, 1, 1]"}});

    expectRows("MATCH (p:Person) "
               "RETURN count(DISTINCT COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 3 }) AS d, "
               "       size(collect(DISTINCT COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 3 })) AS l",
               {{"3", "3"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "WHERE p.hasPhD "
                      "   OR COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 1 } = 0 "
                      "   OR EXISTS { MATCH (p)-[:INTERESTED_IN]->(i:Interest {name: 'Gym'}) RETURN i LIMIT 1 } "
                      "RETURN p.isFrench AS f, sum(COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i }) AS s, count(*) AS n "
                      "ORDER BY s",
                      {{"false", "6", "4"}, {"true", "7", "3"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "WITH p.isFrench AS f, collect(p) AS ps "
                      "RETURN f, COUNT { UNWIND ps AS q MATCH (q)-[:INTERESTED_IN]->(i) RETURN DISTINCT i } AS c "
                      "ORDER BY c",
                      {{"false", "4"}, {"true", "7"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "WITH p.isFrench AS f, count(*) AS n "
                      "RETURN f, n, COUNT { MATCH (x:Person) WHERE x.isFrench = f RETURN x ORDER BY x.name LIMIT 3 } AS c "
                      "ORDER BY f",
                      {{"false", "4", "3"}, {"true", "4", "3"}});
}

TEST_F(PerRowSubqueryCompositionTest, projectsSeveralPerRowSubqueriesSideBySide) {
    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, "
                      "       COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS a, "
                      "       EXISTS { MATCH (p)-[:KNOWS_WELL]->(f) RETURN f ORDER BY f.name LIMIT 1 } AS b, "
                      "       COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.name AS n ORDER BY n } AS l "
                      "ORDER BY p.name",
                      {{"Adam", "2", "true", "[Bio, Cooking]"},
                       {"Cyrus", "2", "false", "[Gym, Travel]"},
                       {"Doruk", "1", "false", "[Gym]"},
                       {"Luc", "2", "false", "[Animals, Computers]"},
                       {"Martina", "1", "false", "[Cooking]"},
                       {"Maxime", "2", "false", "[Bio, Padel]"},
                       {"Remy", "2", "true", "[Computers, Eighties, Ghosts]"},
                       {"Suhas", "2", "false", "[Gym, JiuJitsu]"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "WITH p, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 5 } AS c "
                      "WHERE c >= 2 "
                      "MATCH (p)-[:INTERESTED_IN]->(j) "
                      "RETURN p.name, c, j.name "
                      "ORDER BY p.name, j.name",
                      {{"Adam", "2", "Bio"},
                       {"Adam", "2", "Cooking"},
                       {"Cyrus", "2", "Gym"},
                       {"Cyrus", "2", "Travel"},
                       {"Luc", "2", "Animals"},
                       {"Luc", "2", "Computers"},
                       {"Maxime", "2", "Bio"},
                       {"Maxime", "2", "Padel"},
                       {"Remy", "3", "Computers"},
                       {"Remy", "3", "Eighties"},
                       {"Remy", "3", "Ghosts"},
                       {"Suhas", "2", "Gym"},
                       {"Suhas", "2", "JiuJitsu"}});
}

TEST_F(PerRowSubqueryCompositionTest, matchesNothingFromANullInput) {
    expectRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) "
               "RETURN p.name, COUNT { MATCH (p)-->(x) RETURN x LIMIT 1 } AS c",
               {{"null", "0"}});

    expectRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) "
               "RETURN EXISTS { MATCH (p)-->(x) RETURN x LIMIT 1 } AS e, "
               "       COLLECT { MATCH (p)-->(x) RETURN x.name AS n ORDER BY n } AS l",
               {{"false", "[]"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                      "RETURN p.name, f.name, COUNT { MATCH (f)-[:INTERESTED_IN]->(x) RETURN x LIMIT 2 } AS c "
                      "ORDER BY p.name",
                      {{"Adam", "Remy", "2"},
                       {"Cyrus", "null", "0"},
                       {"Doruk", "null", "0"},
                       {"Luc", "null", "0"},
                       {"Martina", "null", "0"},
                       {"Maxime", "null", "0"},
                       {"Remy", "Adam", "2"},
                       {"Suhas", "null", "0"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                      "WHERE COUNT { MATCH (f)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 5 } > 2 "
                      "RETURN p.name, f.name "
                      "ORDER BY p.name",
                      {{"Adam", "Remy"},
                       {"Cyrus", "null"},
                       {"Doruk", "null"},
                       {"Luc", "null"},
                       {"Martina", "null"},
                       {"Maxime", "null"},
                       {"Remy", "null"},
                       {"Suhas", "null"}});

    expectRows("MATCH (p:Person {name: 'Nobody'}) "
               "RETURN count(*) AS n, "
               "       sum(COUNT { MATCH (p)-->(x) RETURN x LIMIT 1 }) AS s, "
               "       collect(COLLECT { MATCH (p)-->(x) RETURN x.name AS m ORDER BY m }) AS l",
               {{"0", "0", "[]"}});
}

TEST_F(PerRowSubqueryCompositionTest, branchesPerRowOverOptionalRows) {
    expectRowsInOrder("MATCH (p:Person) "
                      "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                      "RETURN p.name, COUNT { "
                      "  WHEN f IS NULL THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i "
                      "  ELSE MATCH (f)-[:INTERESTED_IN]->(i) RETURN i "
                      "} AS c "
                      "ORDER BY c DESC, p.name "
                      "LIMIT 4",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Luc", "2"}, {"Maxime", "2"}});
}

TEST_F(PerRowSubqueryCompositionTest, countsEveryRowOfAManyChunkInput) {
    expectRows("UNWIND range(1, 70000) AS i "
               "WITH i WHERE COUNT { UNWIND [i] AS j RETURN j LIMIT 1 } = 1 "
               "RETURN count(*)",
               {{"70000"}});

    expectRows("UNWIND range(1, 70000) AS i "
               "WITH i WHERE EXISTS { UNWIND [i, i] AS j RETURN j ORDER BY j LIMIT 1 } "
               "RETURN count(*), sum(i)",
               {{"70000", "2450035000"}});

    expectRows("UNWIND range(1, 70000) AS i "
               "RETURN sum(COUNT { UNWIND range(1, i % 3) AS j RETURN j LIMIT 1 }) AS s, count(*) AS n",
               {{"46667", "70000"}});

    expectRows("UNWIND range(1, 300) AS a "
               "UNWIND range(1, 300) AS b "
               "WITH a, b WHERE EXISTS { UNWIND [a, b] AS x WITH x WHERE x % 2 = 0 RETURN DISTINCT x } "
               "RETURN count(*) AS n, sum(COUNT { UNWIND [a, b] AS x RETURN DISTINCT x }) AS s",
               {{"67500", "134850"}});
}

TEST_F(PerRowSubqueryCompositionTest, capsAndCollectsTheBodyOverAManyChunkInput) {
    expectRows("UNWIND range(1, 70000) AS i "
               "WITH i, COUNT { UNWIND range(1, i % 4) AS j RETURN j ORDER BY j DESC LIMIT 2 } AS c "
               "RETURN count(*) AS n, sum(c) AS s, min(i), max(i)",
               {{"70000", "87500", "1", "70000"}});

    expectRows("UNWIND range(1, 70000) AS i "
               "WITH i WHERE COLLECT { UNWIND range(1, i % 4) AS j RETURN j ORDER BY j DESC LIMIT 1 } = [3] "
               "RETURN count(*), min(i), max(i)",
               {{"17500", "3", "69999"}});
}

TEST_F(PerRowSubqueryCompositionTest, ordersAndCutsAManyChunkInput) {
    expectRowsInOrder("UNWIND range(1, 70000) AS i "
                      "RETURN i, COUNT { UNWIND range(1, i % 4) AS j RETURN j ORDER BY j DESC LIMIT 2 } AS c "
                      "ORDER BY i "
                      "SKIP 65534 "
                      "LIMIT 4",
                      {{"65535", "2"}, {"65536", "0"}, {"65537", "1"}, {"65538", "2"}});

    expectRows("UNWIND range(1, 70000) AS i "
               "WITH i, COUNT { UNWIND range(1, i % 4) AS j RETURN j ORDER BY j DESC LIMIT 2 } AS c "
               "SKIP 65534 "
               "LIMIT 4 "
               "RETURN count(*)",
               {{"4"}});

    expectRowsInOrder("UNWIND range(1, 70000) AS i "
                      "RETURN i, COUNT { UNWIND range(1, i % 7) AS j RETURN DISTINCT j } AS c "
                      "ORDER BY c DESC, i DESC "
                      "LIMIT 3",
                      {{"69999", "6"}, {"69992", "6"}, {"69985", "6"}});
}

TEST_F(PerRowSubqueryCompositionTest, runsInsideACallBody) {
    expectRows("MATCH (p:Person) "
               "CALL { WITH p RETURN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c } "
               "RETURN p.name, c",
               interestCountsCappedAtTwo);

    expectRows("MATCH (p:Person) "
               "CALL { "
               "  WITH p "
               "  MATCH (p)-[:INTERESTED_IN]->(i) "
               "  RETURN i, COUNT { MATCH (i)<-[:INTERESTED_IN]-(q) RETURN q ORDER BY q.name LIMIT 9 } AS k "
               "} "
               "RETURN p.name, sum(k) AS s",
               {{"Adam", "4"},
                {"Cyrus", "4"},
                {"Doruk", "3"},
                {"Luc", "3"},
                {"Martina", "2"},
                {"Maxime", "3"},
                {"Remy", "4"},
                {"Suhas", "4"}});
}

TEST_F(PerRowSubqueryCompositionTest, dedupsAUnionBodyPerRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) RETURN i "
               "  UNION "
               "  MATCH (p)-[:KNOWS_WELL]->(i) RETURN i "
               "} AS c",
               {{"Adam", "3"},
                {"Cyrus", "2"},
                {"Doruk", "1"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Maxime", "2"},
                {"Remy", "4"},
                {"Suhas", "2"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, p.dob, "
                      "       COUNT { UNWIND [p.dob, p.dob] AS d WITH d WHERE d IS NOT NULL RETURN DISTINCT d } AS c, "
                      "       COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n ORDER BY n SKIP 1 } AS rest, "
                      "       EXISTS { MATCH (p)-[:KNOWS_WELL]->(x) RETURN x UNION MATCH (p)<-[:KNOWS_WELL]-(x) RETURN x } AS e "
                      "ORDER BY p.name",
                      {{"Adam", "18/08", "1", "[Cooking]", "true"},
                       {"Cyrus", "null", "0", "[Travel]", "false"},
                       {"Doruk", "null", "0", "[]", "false"},
                       {"Luc", "28/05", "1", "[Computers]", "false"},
                       {"Martina", "null", "0", "[]", "false"},
                       {"Maxime", "24/07", "1", "[Padel]", "false"},
                       {"Remy", "18/01", "1", "[Eighties, Ghosts]", "true"},
                       {"Suhas", "null", "0", "[JiuJitsu]", "false"}});
}

TEST_F(PerRowSubqueryCompositionTest, nestsAPerRowBodyInsideAnother) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) "
               "  WHERE COUNT { MATCH (i)<-[:INTERESTED_IN]-(q) RETURN q LIMIT 5 } > 1 "
               "  RETURN i LIMIT 5 "
               "} AS c",
               {{"Adam", "2"},
                {"Cyrus", "1"},
                {"Doruk", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Maxime", "1"},
                {"Remy", "1"},
                {"Suhas", "1"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) "
               "  RETURN i.name + ':' + toString(COUNT { MATCH (i)<-[:INTERESTED_IN]-(q) RETURN q LIMIT 9 }) AS s "
               "  ORDER BY s "
               "} AS l",
               {{"Adam", "[Bio:2, Cooking:2]"},
                {"Cyrus", "[Gym:3, Travel:1]"},
                {"Doruk", "[Gym:3]"},
                {"Luc", "[Animals:1, Computers:2]"},
                {"Martina", "[Cooking:2]"},
                {"Maxime", "[Bio:2, Padel:1]"},
                {"Remy", "[Computers:2, Eighties:1, Ghosts:1]"},
                {"Suhas", "[Gym:3, JiuJitsu:1]"}});
}

TEST_F(PerRowSubqueryCompositionTest, rebindsAnOuterEdgeInTheBody) {
    expectRows("MATCH (p:Person)-[e:INTERESTED_IN]->(i) "
               "RETURN count(*) AS n, sum(COUNT { MATCH (a)-[e]->(b) RETURN a ORDER BY a.name LIMIT 1 }) AS s",
               {{"15", "15"}});
}

TEST_F(PerRowSubqueryCompositionTest, runsInBothBranchesOfAnOuterUnion) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "RETURN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } AS c "
               "UNION "
               "MATCH (p:Person {name: 'Martina'}) "
               "RETURN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name } AS c",
               {{"2"}, {"1"}});
}

TEST_F(PerRowSubqueryCompositionTest, answersAnAggregatingBodyOverAnEmptyMatch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, "
               "       COLLECT { MATCH (p)-[:KNOWS_WELL]->(f) RETURN count(f) } AS l, "
               "       COUNT { MATCH (p)-[:KNOWS_WELL]->(f) RETURN count(f) } AS c, "
               "       EXISTS { MATCH (p)-[:KNOWS_WELL]->(f) RETURN count(f) } AS e",
               {{"Adam", "[1]", "1", "true"},
                {"Cyrus", "[0]", "1", "true"},
                {"Doruk", "[0]", "1", "true"},
                {"Luc", "[0]", "1", "true"},
                {"Martina", "[0]", "1", "true"},
                {"Maxime", "[0]", "1", "true"},
                {"Remy", "[1]", "1", "true"},
                {"Suhas", "[0]", "1", "true"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q) "
               "  WHERE q <> p "
               "  RETURN q.name, count(*) "
               "} AS g",
               {{"Adam", "2"},
                {"Cyrus", "2"},
                {"Doruk", "2"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Maxime", "1"},
                {"Remy", "1"},
                {"Suhas", "2"}});
}

TEST_F(PerRowSubqueryCompositionTest, readsTheVariablesOfAComprehensionAndAReduce) {
    expectRows("RETURN [x IN range(1, 3) | COUNT { UNWIND range(1, x) AS y RETURN y ORDER BY y LIMIT 2 }] AS l, "
               "       reduce(acc = 0, x IN range(1, 4) | acc + COUNT { UNWIND range(1, acc + x) AS y RETURN DISTINCT y % 3 }) AS r",
               {{"[1, 2, 2]", "10"}});
}

TEST_F(PerRowSubqueryCompositionTest, runsUnderACrossProduct) {
    expectRows("MATCH (p:Person), (q:Interest) "
               "RETURN count(*) AS n, sum(COUNT { MATCH (p)-[:INTERESTED_IN]->(q) RETURN q ORDER BY q.name LIMIT 1 }) AS s",
               {{"80", "15"}});
}

TEST_F(PerRowSubqueryCompositionTest, broadcastsAnUncorrelatedBody) {
    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, "
                      "       COUNT { MATCH (i:Interest) RETURN i ORDER BY i.name LIMIT 3 } AS c, "
                      "       COLLECT { MATCH (i:Interest) RETURN i.name AS n ORDER BY n LIMIT 2 } AS l "
                      "ORDER BY p.name DESC "
                      "LIMIT 3",
                      {{"Suhas", "3", "[Animals, Bio]"},
                       {"Remy", "3", "[Animals, Bio]"},
                       {"Maxime", "3", "[Animals, Bio]"}});
}

TEST_F(PerRowSubqueryCompositionTest, readsAConstantImport) {
    expectRowsInOrder("WITH 2 AS k "
                      "MATCH (p:Person) "
                      "WHERE p.isFrench "
                      "RETURN p.name, k, COUNT { UNWIND range(1, k) AS x RETURN x ORDER BY x LIMIT 5 } AS c "
                      "ORDER BY p.name",
                      {{"Adam", "2", "2"}, {"Luc", "2", "2"}, {"Maxime", "2", "2"}, {"Remy", "2", "2"}});
}

TEST_F(PerRowSubqueryCompositionTest, readsBothEndsOfAnExpansion) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "WHERE COUNT { MATCH (i)<-[:INTERESTED_IN]-(q) WHERE q <> p RETURN q ORDER BY q.name LIMIT 5 } >= 1 "
                      "RETURN p.name, i.name "
                      "ORDER BY p.name, i.name",
                      {{"Adam", "Bio"},
                       {"Adam", "Cooking"},
                       {"Cyrus", "Gym"},
                       {"Doruk", "Gym"},
                       {"Luc", "Computers"},
                       {"Martina", "Cooking"},
                       {"Maxime", "Bio"},
                       {"Remy", "Computers"},
                       {"Suhas", "Gym"}});
}

TEST_F(PerRowSubqueryCompositionTest, writesThePerRowValue) {
    applyWrite("MATCH (p:Person) "
               "SET p.ic = COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 2 }");

    expectRows("MATCH (p:Person) RETURN p.name, p.ic", interestCountsCappedAtTwo);

    applyWrite("MATCH (p:Person) WHERE p.name = 'Remy' "
               "CREATE (q:Tmp {"
               "n: COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i }, "
               "l: COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS x ORDER BY x LIMIT 2 }"
               "})");

    expectRows("MATCH (q:Tmp) RETURN q.n, q.l", {{"3", "[Computers, Eighties]"}});
}
