#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// allReduce composed with the clauses and expressions around it. Only Remy and Adam have an
// age (32), KNOWS_WELL runs Remy->Adam and Adam->Remy (20) and Ghosts->Remy (200), and
// Remy->Computers is the one INTERESTED_IN edge of Remy without a duration.
class AllReduceCompositionTest : public WriteQueryTest {
};

TEST_F(AllReduceCompositionTest, foldsAListAccumulator) {
    expectRows("RETURN allReduce(acc = [], x IN [1,2,3] | acc + x, size(acc) < 3)", {{"false"}});
    expectRows("RETURN allReduce(acc = [], x IN [1,2,3] | acc + x, size(acc) <= 3)", {{"true"}});
}

TEST_F(AllReduceCompositionTest, foldsAStringAccumulator) {
    expectRows("RETURN allReduce(acc = '', x IN ['a','b'] | acc + x, size(acc) <= 1)", {{"false"}});
    expectRows("RETURN allReduce(acc = '', x IN ['a','b'] | acc + x, size(acc) <= 2)", {{"true"}});
}

TEST_F(AllReduceCompositionTest, answersNullFromANullInitialValue) {
    expectRows("RETURN allReduce(s = null, x IN [1,2] | s + x, s < 10)", {{"null"}});
}

TEST_F(AllReduceCompositionTest, ordersOnAnAllReduce) {
    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name "
                      "ORDER BY allReduce(s = 0, x IN [p.age, 1] | s + x, s < 34), p.name",
                      {{"Adam"},
                       {"Remy"},
                       {"Cyrus"},
                       {"Doruk"},
                       {"Luc"},
                       {"Martina"},
                       {"Maxime"},
                       {"Suhas"}});
}

TEST_F(AllReduceCompositionTest, foldsTheRelationshipsOfAVariableLengthPath) {
    expectRows("MATCH p = (a)-[:KNOWS_WELL*1..3]->(b) "
               "WHERE allReduce(s = 0, e IN relationships(p) | s + e.duration, s < 100) "
               "RETURN a.name, b.name",
               {{"Remy", "Adam"},
                {"Remy", "Remy"},
                {"Adam", "Remy"},
                {"Adam", "Adam"}});

    expectRows("MATCH p = (a)-[:KNOWS_WELL*1..3]->(b) "
               "RETURN a.name, b.name, allReduce(s = 0, e IN relationships(p) | s + e.duration, s < 100) AS k "
               "ORDER BY a.name, b.name, length(p)",
               {{"Adam", "Adam", "true"},
                {"Adam", "Remy", "true"},
                {"Ghosts", "Adam", "false"},
                {"Ghosts", "Remy", "false"},
                {"Ghosts", "Remy", "false"},
                {"Remy", "Adam", "true"},
                {"Remy", "Remy", "true"}});
}

TEST_F(AllReduceCompositionTest, foldsTheNodesOfAVariableLengthPath) {
    expectRowsInOrder("MATCH p = (a {name:'Ghosts'})-[:KNOWS_WELL*1..3]->(b) "
                      "RETURN b.name, length(p), allReduce(s = '', n IN nodes(p) | s + n.name, size(s) < 12) AS k "
                      "ORDER BY length(p)",
                      {{"Remy", "1", "true"},
                       {"Adam", "2", "false"},
                       {"Remy", "3", "false"}});
}

TEST_F(AllReduceCompositionTest, answersNullOnARelationshipWithoutTheProperty) {
    expectRows("MATCH p = (a:Person {name:'Remy'})-[:INTERESTED_IN]->(i) "
               "RETURN i.name, allReduce(s = 0, e IN relationships(p) | s + e.duration, s < 100) AS k "
               "ORDER BY i.name",
               {{"Computers", "null"},
                {"Eighties", "true"},
                {"Ghosts", "true"}});
}

TEST_F(AllReduceCompositionTest, holdsOverTheEmptyRelationshipsOfAZeroLengthPath) {
    expectRowsInOrder("MATCH p = (a {name:'Remy'})-[:KNOWS_WELL*0..1]->(b) "
                      "RETURN b.name, allReduce(s = 0, e IN relationships(p) | s + e.duration, s > 1000) AS k "
                      "ORDER BY length(p)",
                      {{"Remy", "true"},
                       {"Adam", "false"}});
}

TEST_F(AllReduceCompositionTest, sendsANullAllReduceToTheElseOfACase) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE WHEN allReduce(s = 0, x IN [p.age, 1] | s + x, s < 34) THEN 'young' ELSE 'other' END AS c "
               "ORDER BY p.name",
               {{"Adam", "young"},
                {"Cyrus", "other"},
                {"Doruk", "other"},
                {"Luc", "other"},
                {"Martina", "other"},
                {"Maxime", "other"},
                {"Remy", "young"},
                {"Suhas", "other"}});
}

TEST_F(AllReduceCompositionTest, filtersOnAProjectedAllReduce) {
    expectRows("MATCH (p:Person) "
               "WITH p, allReduce(s = 0, x IN [p.age, 1] | s + x, s < 34) AS k "
               "WHERE k "
               "RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Remy"}});

    expectRows("MATCH (p:Person) "
               "WITH p, allReduce(s = 0, x IN [p.age, 1] | s + x, s < 34) AS k "
               "WHERE k IS NULL "
               "RETURN count(*)",
               {{"6"}});
}

TEST_F(AllReduceCompositionTest, groupsOnAnAllReduceThatIsNullForSomeRows) {
    expectRows("MATCH (p:Person) "
               "RETURN allReduce(s = 0, x IN [p.age, 1] | s + x, s < 34) AS k, count(*) AS c "
               "ORDER BY c",
               {{"true", "2"},
                {"null", "6"}});

    expectRows("UNWIND [[1,2],[5],[1,null]] AS l "
               "WITH allReduce(s = 0, x IN l | s + x, s < 4) AS k "
               "RETURN k, count(*) AS c "
               "ORDER BY c DESC, k",
               {{"false", "1"},
                {"true", "1"},
                {"null", "1"}});
}

TEST_F(AllReduceCompositionTest, deduplicatesAnAllReduce) {
    expectRows("RETURN DISTINCT allReduce(s = 0, x IN [1,2] | s + x, s < 3) AS k", {{"false"}});
}

TEST_F(AllReduceCompositionTest, foldsAGroupedCollect) {
    expectRows("MATCH (p:Person) "
               "WITH p.isFrench AS f, collect(p.name) AS ns "
               "RETURN f, allReduce(s = 0, n IN ns | s + size(n), s < 20) AS k "
               "ORDER BY f",
               {{"false", "false"},
                {"true", "true"}});
}

TEST_F(AllReduceCompositionTest, nestsAnAllReduceInThePredicate) {
    expectRows("RETURN allReduce(s = 0, l IN [[1,2],[3]] | s + size(l), allReduce(t = s, x IN l | t + x, t < 10))",
               {{"true"}});
}

TEST_F(AllReduceCompositionTest, filtersInsideACountSubquery) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE allReduce(s = '', c IN [i.name] | s + c, size(s) < 5) } > 0 "
               "RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Maxime"}, {"Suhas"}});
}

TEST_F(AllReduceCompositionTest, filtersInsideAnExistsSubquery) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE allReduce(s = 0, x IN [1] | s + size(i.name), s > 6) } "
               "RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Luc"}, {"Martina"}, {"Remy"}, {"Suhas"}});
}

TEST_F(AllReduceCompositionTest, foldsACollectSubquery) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, allReduce(s = 0, n IN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } | s + size(n), s < 10) AS k "
               "ORDER BY p.name",
               {{"Adam", "false"},
                {"Cyrus", "true"},
                {"Doruk", "true"},
                {"Luc", "false"},
                {"Martina", "true"},
                {"Maxime", "true"},
                {"Remy", "false"},
                {"Suhas", "false"}});
}

TEST_F(AllReduceCompositionTest, testsAPatternPredicateAfterEveryStep) {
    expectRows("MATCH (p:Person) "
               "WITH collect(p) AS ps "
               "RETURN allReduce(c = 0, x IN ps | c + 1, exists((x)-[:INTERESTED_IN]->())) AS k",
               {{"true"}});
    expectRows("MATCH (p:Person) "
               "WITH collect(p) AS ps "
               "RETURN allReduce(c = 0, x IN ps | c + 1, exists((x)-[:KNOWS_WELL]->())) AS k",
               {{"false"}});

    expectRows("MATCH p = (a:Person {name:'Remy'})-[:INTERESTED_IN]->(i) "
               "RETURN i.name, allReduce(s = 0, n IN nodes(p) | s + 1, exists((n)-[:KNOWS_WELL]->())) AS k "
               "ORDER BY i.name",
               {{"Computers", "false"},
                {"Eighties", "false"},
                {"Ghosts", "true"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, allReduce(s = 0, x IN [1,2] | s + x, exists((p)-[:KNOWS_WELL]->()) OR s < 2) AS k "
               "ORDER BY p.name",
               {{"Adam", "true"},
                {"Cyrus", "false"},
                {"Doruk", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Maxime", "false"},
                {"Remy", "true"},
                {"Suhas", "false"}});
}

TEST_F(AllReduceCompositionTest, foldsABooleanAccumulatorOfPatternPredicates) {
    expectRows("MATCH (p:Person) "
               "WITH collect(p) AS ps "
               "RETURN allReduce(c = true, x IN ps | c AND exists((x)-[:INTERESTED_IN]->()), c) AS k",
               {{"true"}});
}

TEST_F(AllReduceCompositionTest, filtersAnOptionalMatch) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WHERE allReduce(s = 0, x IN [f.age] | s + x, s > 30) "
               "RETURN p.name, f.name ORDER BY p.name",
               {{"Adam", "Remy"},
                {"Cyrus", "null"},
                {"Doruk", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Maxime", "null"},
                {"Remy", "Adam"},
                {"Suhas", "null"}});
}

TEST_F(AllReduceCompositionTest, setsAPropertyToAnAllReduce) {
    applyWrite("MATCH (p:Person) SET p.ar = allReduce(s = 0, x IN [p.age, 1] | s + x, s < 34)");

    expectRows("MATCH (p:Person) RETURN p.name, p.ar",
               {{"Adam", "true"},
                {"Cyrus", "null"},
                {"Doruk", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Maxime", "null"},
                {"Remy", "true"},
                {"Suhas", "null"}});
}
