#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// exists(<pattern>) is the pattern predicate <pattern>: true for a row when the pattern
// matches from it. KNOWS_WELL leaves Remy for Adam, Adam for Remy and Ghosts for Remy. Remy
// is interested in Ghosts and Eighties, both Exotic, and Luc in Animals, a SleepDisturber.
class ExistsFunctionTest : public WriteQueryTest {
};

TEST_F(ExistsFunctionTest, answersAsThePatternPredicate) {
    const Rows expected = {{"Remy", "true"},
                           {"Adam", "true"},
                           {"Maxime", "false"},
                           {"Luc", "false"},
                           {"Martina", "false"},
                           {"Suhas", "false"},
                           {"Cyrus", "false"},
                           {"Doruk", "false"}};

    expectRows("MATCH (p:Person) RETURN p.name, exists((p)-[:KNOWS_WELL]->())", expected);
    expectRows("MATCH (p:Person) RETURN p.name, (p)-[:KNOWS_WELL]->()", expected);
    expectRows("MATCH (p:Person) RETURN p.name, EXISTS { (p)-[:KNOWS_WELL]->() }", expected);
}

TEST_F(ExistsFunctionTest, filtersTheRows) {
    expectRows("MATCH (p:Person) WHERE exists((p)-[:INTERESTED_IN]->(:Exotic)) RETURN p.name", {{"Remy"}});
    expectRows("MATCH (p:Person) WHERE exists((p)<-[:KNOWS_WELL]-({name: 'Ghosts'})) RETURN p.name", {{"Remy"}});

    expectRows("MATCH (p:Person) WHERE NOT exists((p)-[:INTERESTED_IN]->()) RETURN p.name", {});

    expectRows("MATCH (p:Person) "
               "WHERE p.isFrench AND NOT exists((p)-[:KNOWS_WELL]-()) "
               "RETURN p.name",
               {{"Maxime"}, {"Luc"}});
}

TEST_F(ExistsFunctionTest, readsAPropertyMapInsideThePattern) {
    expectRows("MATCH (p:Person) WHERE exists((p)-[:INTERESTED_IN {duration: 20}]->()) RETURN p.name",
               {{"Remy"}, {"Luc"}});
}

TEST_F(ExistsFunctionTest, followsAVariableLengthPattern) {
    expectRows("MATCH (i:Interest) WHERE exists((i)-[:KNOWS_WELL*2]->({name: 'Adam'})) RETURN i.name", {{"Ghosts"}});
}

TEST_F(ExistsFunctionTest, readsAParenthesizedPattern) {
    expectRows("MATCH (p:Person) WHERE exists(((p)-[:INTERESTED_IN]->(:SleepDisturber))) RETURN p.name",
               {{"Remy"}, {"Luc"}});
}

TEST_F(ExistsFunctionTest, decidesAWhen) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN exists((p)-[:KNOWS_WELL]->()) THEN RETURN 'friend' AS k "
               "  ELSE RETURN 'loner' AS k "
               "}",
               {{"Remy", "[friend]"},
                {"Adam", "[friend]"},
                {"Maxime", "[loner]"},
                {"Luc", "[loner]"},
                {"Martina", "[loner]"},
                {"Suhas", "[loner]"},
                {"Cyrus", "[loner]"},
                {"Doruk", "[loner]"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN exists((p)-[:INTERESTED_IN]->(:SleepDisturber)) THEN RETURN 'restless' AS k } "
               "RETURN p.name, k",
               {{"Remy", "restless"}, {"Luc", "restless"}});
}

TEST_F(ExistsFunctionTest, isNotAName) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH {exists: exists((p)-[:KNOWS_WELL]->())} AS m RETURN m.exists",
               {{"true"}});
}

TEST_F(ExistsFunctionTest, rejectsAnArgumentThatIsNoPattern) {
    expectError("MATCH (p:Person) RETURN exists(p.name)", "exists() takes a pattern");
    expectError("MATCH (p:Person) RETURN exists(p)", "exists() takes a pattern");
    expectError("MATCH (p:Person) RETURN exists((p)-[:KNOWS_WELL]->() AND true)", "exists() takes a pattern");
    expectError("MATCH (p:Person) RETURN exists(EXISTS { (p)-[:KNOWS_WELL]->() })", "exists() takes a pattern");
}

TEST_F(ExistsFunctionTest, rejectsAPatternIntroducingAVariable) {
    expectError("MATCH (p:Person) WHERE exists((p)-[e:INTERESTED_IN WHERE e.duration < 20]->()) RETURN p.name",
                "A pattern predicate cannot introduce new variables: 'e'");
}
