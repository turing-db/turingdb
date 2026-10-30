#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// exists(<pattern>) composed with the clauses and expressions around it. KNOWS_WELL leaves
// Remy for Adam, Adam for Remy and Ghosts for Remy. Cyrus, Doruk and Suhas are interested in
// Gym, and Remy in Ghosts and Eighties, both Exotic.
class ExistsFunctionCompositionTest : public WriteQueryTest {
};

TEST_F(ExistsFunctionCompositionTest, groupsOnAPatternPredicate) {
    expectRows("MATCH (p:Person) RETURN exists((p)-[:KNOWS_WELL]->()) AS k, count(*)",
               {{"true", "2"},
                {"false", "6"}});

    expectRows("MATCH (p:Person) "
               "RETURN sum(CASE WHEN exists((p)-[:KNOWS_WELL]->()) THEN 1 ELSE 0 END) AS s, count(*) AS c",
               {{"2", "8"}});

    expectRowsInOrder("MATCH (p:Person) RETURN DISTINCT exists((p)-[:INTERESTED_IN]->(:Exotic)) AS k ORDER BY k",
                      {{"false"},
                       {"true"}});
}

TEST_F(ExistsFunctionCompositionTest, filtersAComprehensionOverCollectedNodes) {
    expectRows("MATCH (p:Person) "
               "WITH p ORDER BY p.name "
               "WITH collect(p) AS ps "
               "RETURN [x IN ps WHERE exists((x)-[:KNOWS_WELL]->()) | x.name] AS n",
               {{"[Adam, Remy]"}});

    expectRows("MATCH (a:Person {name:'Remy'}) "
               "MATCH (b:Person) "
               "WITH a, collect(b) AS bs "
               "RETURN [x IN bs WHERE exists((a)-[:KNOWS_WELL]->(x)) | x.name] AS n",
               {{"[Adam]"}});
}

TEST_F(ExistsFunctionCompositionTest, filtersAPatternComprehension) {
    expectRows("MATCH (p:Person {name:'Remy'}) "
               "RETURN [(p)-[:INTERESTED_IN]->(i) WHERE exists((i)-[:KNOWS_WELL]->()) | i.name] AS n",
               {{"[Ghosts]"}});
}

TEST_F(ExistsFunctionCompositionTest, countsInAReduce) {
    expectRows("MATCH (p:Person) "
               "WITH collect(p) AS ps "
               "RETURN reduce(c = 0, x IN ps | c + CASE WHEN exists((x)-[:INTERESTED_IN]->(:Exotic)) THEN 1 ELSE 0 END) AS n",
               {{"1"}});
}

TEST_F(ExistsFunctionCompositionTest, combinesWithTheLogicalOperators) {
    expectRows("MATCH (p:Person) "
               "WHERE exists((p)-[:KNOWS_WELL]->()) XOR p.name = 'Luc' "
               "RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Luc"}, {"Remy"}});

    expectRows("MATCH (p:Person) "
               "WHERE exists((p)-[:KNOWS_WELL]->()) OR exists((p)-[:INTERESTED_IN]->(:SleepDisturber)) "
               "RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Luc"}, {"Remy"}});

    expectRows("MATCH (p:Person) "
               "WHERE NOT exists((p)-[:KNOWS_WELL]->()) AND exists((p)-[:INTERESTED_IN]->({name:'Gym'})) "
               "RETURN p.name ORDER BY p.name",
               {{"Cyrus"}, {"Doruk"}, {"Suhas"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, exists((p)-[:KNOWS_WELL]->()) = exists((p)<-[:KNOWS_WELL]-()) AS same "
                      "ORDER BY p.name LIMIT 3",
                      {{"Adam", "true"},
                       {"Cyrus", "true"},
                       {"Doruk", "true"}});
}

TEST_F(ExistsFunctionCompositionTest, matchesTypeAndLabelDisjunctions) {
    expectRows("MATCH (p) "
               "WHERE exists((p)-[:KNOWS_WELL|INTERESTED_IN]->(:Exotic|Person)) "
               "RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Ghosts"}, {"Remy"}});
}

TEST_F(ExistsFunctionCompositionTest, readsAnOuterPropertyInAPropertyMap) {
    expectRows("MATCH (p:Person) WHERE exists((p)-[:KNOWS_WELL]->({age: p.age})) RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(ExistsFunctionCompositionTest, matchesLongerPatterns) {
    expectRows("MATCH (p:Person) "
               "WHERE exists((p)-[:KNOWS_WELL]->(:Person)-[:INTERESTED_IN]->(:Exotic)) "
               "RETURN p.name",
               {{"Adam"}});

    expectRows("MATCH (p:Person) "
               "WHERE exists((p)-[:KNOWS_WELL*1..2]-({name: 'Ghosts'})) "
               "RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Remy"}});

    expectRows("UNWIND [1, 2] AS i "
               "MATCH (p:Person {name: 'Remy'}) "
               "RETURN i, exists((p)-[:KNOWS_WELL*2]->(p)) AS k",
               {{"1", "true"},
                {"2", "true"}});
}

TEST_F(ExistsFunctionCompositionTest, acceptsSpacesInsideTheCall) {
    expectRows("MATCH (p:Person) WHERE EXISTS( (p)-[:KNOWS_WELL]->() ) RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(ExistsFunctionCompositionTest, ordersOnAPatternPredicate) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name ORDER BY exists((p)-[:KNOWS_WELL]->()) DESC, p.name",
                      {{"Adam"},
                       {"Remy"},
                       {"Cyrus"},
                       {"Doruk"},
                       {"Luc"},
                       {"Martina"},
                       {"Maxime"},
                       {"Suhas"}});

    expectRowsInOrder("MATCH (p:Person) "
                      "WITH p, exists((p)-[:KNOWS_WELL]->()) AS k ORDER BY k DESC, p.name LIMIT 3 "
                      "RETURN p.name, k",
                      {{"Adam", "true"},
                       {"Remy", "true"},
                       {"Cyrus", "false"}});
}

TEST_F(ExistsFunctionCompositionTest, filtersOnAProjectedPatternPredicateBeforeAMatch) {
    expectRows("MATCH (p:Person) "
               "WITH p, exists((p)-[:KNOWS_WELL]->()) AS k "
               "WHERE k "
               "MATCH (p)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, i.name ORDER BY p.name, i.name",
               {{"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"}});

    expectRows("MATCH (p:Person) "
               "WITH p, exists((p)-[:KNOWS_WELL]->()) AS k "
               "WHERE NOT k "
               "MATCH (p)-[:INTERESTED_IN]->(i:Interest {name:'Gym'}) "
               "RETURN p.name ORDER BY p.name",
               {{"Cyrus"}, {"Doruk"}, {"Suhas"}});
}

TEST_F(ExistsFunctionCompositionTest, relatesTwoOuterVariables) {
    expectRows("MATCH (a:Person), (b:Person) WHERE exists((a)-[:KNOWS_WELL]->(b)) RETURN a.name, b.name ORDER BY a.name",
               {{"Adam", "Remy"},
                {"Remy", "Adam"}});
}

TEST_F(ExistsFunctionCompositionTest, startsFromAnUnwoundNode) {
    expectRows("MATCH (p:Person) "
               "WITH collect(p) AS ps "
               "UNWIND ps AS x "
               "RETURN x.name, exists((x)-[:KNOWS_WELL]->()) AS k ORDER BY x.name",
               {{"Adam", "true"},
                {"Cyrus", "false"},
                {"Doruk", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Maxime", "false"},
                {"Remy", "true"},
                {"Suhas", "false"}});
}

TEST_F(ExistsFunctionCompositionTest, filtersAnOptionalMatch) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) WHERE exists((i)-[:KNOWS_WELL]->()) "
               "RETURN p.name, i.name ORDER BY p.name",
               {{"Adam", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Maxime", "null"},
                {"Remy", "Ghosts"},
                {"Suhas", "null"}});
}

TEST_F(ExistsFunctionCompositionTest, matchesNothingOverAnUnknownTypeOrLabel) {
    expectRows("MATCH (p:Person) WHERE exists((p)-[:NOPE]->()) RETURN p.name", {});
    expectRows("MATCH (p:Person) WHERE NOT exists((p)-[:INTERESTED_IN]->(:NoSuchLabel)) RETURN count(*)", {{"8"}});
}

TEST_F(ExistsFunctionCompositionTest, rejectsANewVariableInThePattern) {
    expectError("MATCH (p:Person) WHERE exists((p)-[:KNOWS_WELL]->(q)) RETURN p.name ORDER BY p.name",
                "cannot introduce new variables");
}

TEST_F(ExistsFunctionCompositionTest, setsAPropertyToAPatternPredicate) {
    applyWrite("MATCH (p:Person) SET p.kw = exists((p)-[:KNOWS_WELL]->())");

    expectRows("MATCH (p:Person) RETURN p.name, p.kw",
               {{"Adam", "true"},
                {"Cyrus", "false"},
                {"Doruk", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Maxime", "false"},
                {"Remy", "true"},
                {"Suhas", "false"}});
}
