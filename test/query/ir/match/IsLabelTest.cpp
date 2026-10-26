#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// IS is the GQL spelling of the colon that opens a label expression, in a pattern and in a
// label test. On simpledb the eight people carry Person, Remy and Adam are Founders, Doruk
// is in Sales, and Remy->Adam, Adam->Remy and Ghosts->Remy are the KNOWS_WELL edges.
class IsLabelTest : public WriteQueryTest {
};

TEST_F(IsLabelTest, aLabelTestKeepsTheNodesCarryingTheLabel) {
    expectRows("MATCH (n) WHERE n IS Person RETURN count(n)", {{"8"}});
}

TEST_F(IsLabelTest, aNodePatternKeepsTheNodesCarryingTheLabel) {
    expectRows("MATCH (n IS Person) RETURN count(n)", {{"8"}});
    expectRows("MATCH (IS Person) RETURN count(*)", {{"8"}});
}

TEST_F(IsLabelTest, aLabelTestTakesALabelExpression) {
    expectRows("MATCH (n) WHERE n IS Founder|Sales RETURN n.name", {{"Remy"}, {"Adam"}, {"Doruk"}});
    expectRows("MATCH (n) WHERE n IS (Founder|Sales)&!Bioinformatics RETURN n.name", {{"Remy"}, {"Doruk"}});
}

TEST_F(IsLabelTest, aNodePatternTakesALabelExpression) {
    expectRows("MATCH (n IS Person&!Founder) RETURN n.name",
               {{"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(IsLabelTest, anEdgeTypeTestKeepsTheEdgesOfTheType) {
    expectRows("MATCH (n)-[e]->(m) WHERE e IS KNOWS_WELL RETURN n.name, m.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Ghosts", "Remy"}});
}

TEST_F(IsLabelTest, aRelationshipPatternTakesATypeExpression) {
    expectRows("MATCH (n)-[e IS KNOWS_WELL]->(m) RETURN n.name, m.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Ghosts", "Remy"}});
    expectRows("MATCH (n IS Founder)-[IS !KNOWS_WELL]->(m) RETURN count(m)", {{"5"}});
    expectRows("MATCH (a IS Supernatural)-[IS !INTERESTED_IN*1..2]->(b) RETURN a.name, b.name",
               {{"Ghosts", "Remy"}, {"Ghosts", "Adam"}});
}

TEST_F(IsLabelTest, aPatternPredicateTakesIs) {
    expectRows("MATCH (a:Person) WHERE (a)-[IS KNOWS_WELL]->(IS Founder) RETURN a.name", {{"Remy"}, {"Adam"}});
}

TEST_F(IsLabelTest, aLabelTestProjectsAsABoolean) {
    expectRows("MATCH (n:Person) RETURN n.name, n IS Founder",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}});
}

TEST_F(IsLabelTest, aLabelTestStandsBesideANullTest) {
    expectRows("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(b) "
               "RETURN a.name, b IS NULL, b IS NOT NULL, b IS Founder",
               {{"Remy", "false", "true", "true"}, {"Adam", "false", "true", "true"},
                {"Maxime", "true", "false", "null"}, {"Luc", "true", "false", "null"},
                {"Martina", "true", "false", "null"}, {"Suhas", "true", "false", "null"},
                {"Cyrus", "true", "false", "null"}, {"Doruk", "true", "false", "null"}});
}

TEST_F(IsLabelTest, isBeforeNullOrANormalFormTestsTheValue) {
    expectRows("RETURN null IS NULL, 1 IS NOT NULL, 'a' IS NORMALIZED, 'a' IS NFC NORMALIZED, 'a' IS NOT NFKD NORMALIZED",
               {{"true", "true", "true", "true", "false"}});
    expectRows("MATCH (n:Person) WHERE n IS\n  NOT\tNULL RETURN count(n)", {{"8"}});
}

TEST_F(IsLabelTest, aLabelNamedLikeTheStartOfANullTestIsALabel) {
    expectWriteRows("CREATE (n IS Nullable&Nothing&NormalizedForm) RETURN n IS Nullable, n IS Nothing, n IS NormalizedForm",
                    {{"true", "true", "true"}});
}

TEST_F(IsLabelTest, aCreatedNodeTakesTheLabelsAfterIs) {
    expectWriteRows("CREATE (n IS Robot&Toy {name: 'Rob'}) RETURN n IS Robot, n IS Toy", {{"true", "true"}});
    expectRows("MATCH (n IS Robot&Toy) RETURN n.name", {{"Rob"}});
}

// In a comprehension a | after the label test starts the projection, as it does after a colon
TEST_F(IsLabelTest, aPipeAfterALabelTestInAComprehensionStartsTheProjection) {
    expectRows("MATCH (n:Founder) RETURN head([x IN [n] WHERE x IS Person | x.name])", {{"Remy"}, {"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
