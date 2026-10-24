#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

const Rows people = {
    {"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"},
};

const Rows interests = {
    {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Ghosts"},
    {"Padel"}, {"Animals"}, {"Gym"}, {"Travel"}, {"JiuJitsu"},
};

}

// A label expression combines labels with | (or), & (and), ! (not) and parentheses, on a
// node pattern and in a label test. On simpledb the eight people carry Person and the ten
// interests Interest; Remy and Adam are Founders, Doruk is in Sales, Adam, Maxime and
// Martina are in Bioinformatics, and Eighties and Ghosts are Exotic.
class LabelExpressionTest : public WriteQueryTest {
};

TEST_F(LabelExpressionTest, aNodePatternDisjunctionKeepsTheNodesCarryingEitherLabel) {
    expectRows("MATCH (n:Founder|Sales) RETURN n.name", {{"Remy"}, {"Adam"}, {"Doruk"}});
    expectRows("MATCH (n:Person|Interest) RETURN count(n)", {{"18"}});
}

TEST_F(LabelExpressionTest, aColonBeforeEachAlternativeIsTheSameDisjunction) {
    expectRows("MATCH (n:Founder|:Sales) RETURN n.name", {{"Remy"}, {"Adam"}, {"Doruk"}});
}

TEST_F(LabelExpressionTest, aNegatedNodePatternKeepsTheNodesWithoutTheLabel) {
    expectRows("MATCH (n:!Interest) RETURN n.name", people);
    expectRows("MATCH (n:!Person) RETURN n.name", interests);
}

TEST_F(LabelExpressionTest, aNodePatternConjunctionKeepsTheNodesCarryingBothLabels) {
    expectRows("MATCH (n:Person&Founder) RETURN n.name", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n:Person:Founder) RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(LabelExpressionTest, aConjunctionWithANegationDropsTheNodesCarryingTheNegatedLabel) {
    expectRows("MATCH (n:Person&!Founder) RETURN n.name",
               {{"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(LabelExpressionTest, parenthesesGroupALabelExpression) {
    expectRows("MATCH (n:(Founder|Sales)&!Bioinformatics) RETURN n.name", {{"Remy"}, {"Doruk"}});
    expectRows("MATCH (n:SoftwareEngineering&(Person|Interest)) RETURN n.name",
               {{"Remy"}, {"Computers"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

TEST_F(LabelExpressionTest, aNegatedGroupKeepsTheNodesCarryingNoneOfItsLabels) {
    expectRows("MATCH (n:!(Person|Exotic)) RETURN n.name",
               {{"Computers"}, {"Bio"}, {"Cooking"}, {"Padel"}, {"Animals"}, {"Gym"}, {"Travel"}, {"JiuJitsu"}});
}

TEST_F(LabelExpressionTest, aDoubleNegationIsTheLabelItself) {
    expectRows("MATCH (n:!!Founder) RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(LabelExpressionTest, aLabelNoNodeCarriesDropsOutOfADisjunction) {
    expectRows("MATCH (n:Founder|Sasquatch) RETURN n.name", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n:!Sasquatch) RETURN count(n)", {{"18"}});
}

TEST_F(LabelExpressionTest, bothEndsOfAHopTakeALabelExpression) {
    expectRows("MATCH (a:Founder|Sales)-[:INTERESTED_IN]->(b:!Exotic) RETURN a.name, b.name",
               {{"Remy", "Computers"}, {"Adam", "Bio"}, {"Adam", "Cooking"}, {"Doruk", "Gym"}});
}

TEST_F(LabelExpressionTest, anAnonymousNodeTakesALabelExpression) {
    expectRows("MATCH (a:Person)-[:INTERESTED_IN]->(:Exotic|Supernatural) RETURN a.name", {{"Remy"}, {"Remy"}});
}

TEST_F(LabelExpressionTest, theEndOfAVariableLengthPathTakesALabelExpression) {
    expectRows("MATCH (a:Supernatural)-[:KNOWS_WELL*1..2]->(b:Founder&!Bioinformatics) RETURN a.name, b.name",
               {{"Ghosts", "Remy"}});
}

TEST_F(LabelExpressionTest, aNodeOfAQuantifiedPatternTakesALabelExpression) {
    expectRows("MATCH (a:Founder)((x)-[:KNOWS_WELL]->(y:Person&!Bioinformatics)){1,2}(b) RETURN a.name, b.name",
               {{"Adam", "Remy"}});
}

TEST_F(LabelExpressionTest, anOptionalMatchKeepsItsLabelExpressionInsideIt) {
    expectRows("MATCH (a:Person) OPTIONAL MATCH (a)-[:INTERESTED_IN]->(b:Exotic|Supernatural) RETURN a.name, b.name",
               {{"Remy", "Ghosts"}, {"Remy", "Eighties"}, {"Adam", "null"}, {"Maxime", "null"}, {"Luc", "null"},
                {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(LabelExpressionTest, aPatternPredicateNodeTakesALabelExpression) {
    expectRows("MATCH (a:Person) WHERE (a)-[:INTERESTED_IN]->(:Exotic|Supernatural) RETURN a.name", {{"Remy"}});
    expectRows("MATCH (b:Interest) WHERE (:Founder|Sales)-[:INTERESTED_IN]->(b) RETURN b.name",
               {{"Ghosts"}, {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Gym"}});
}

TEST_F(LabelExpressionTest, aWhereDisjunctionKeepsTheNodesCarryingEitherLabel) {
    expectRows("MATCH (n) WHERE n:Founder|Sales RETURN n.name", {{"Remy"}, {"Adam"}, {"Doruk"}});
    expectRows("MATCH (n) WHERE n:Person|Interest RETURN count(n)", {{"18"}});
}

TEST_F(LabelExpressionTest, aWhereNegationKeepsTheNodesWithoutTheLabel) {
    expectRows("MATCH (n) WHERE n:!Interest RETURN n.name", people);
}

TEST_F(LabelExpressionTest, aWhereConjunctionKeepsTheNodesCarryingBothLabels) {
    expectRows("MATCH (n) WHERE n:Person&Founder RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(LabelExpressionTest, aWhereLabelExpressionKeepsWhatItsBooleanSpellingKeeps) {
    expectRows("MATCH (n) WHERE n:(Founder|Sales)&!Bioinformatics RETURN n.name", {{"Remy"}, {"Doruk"}});
    expectRows("MATCH (n) WHERE (n:Founder OR n:Sales) AND NOT n:Bioinformatics RETURN n.name", {{"Remy"}, {"Doruk"}});
}

TEST_F(LabelExpressionTest, aLabelExpressionProjectsAsABoolean) {
    expectRows("MATCH (n:Person) RETURN n.name, n:Founder|Sales, n:!Founder",
               {{"Remy", "true", "false"}, {"Adam", "true", "false"}, {"Maxime", "false", "true"},
                {"Luc", "false", "true"}, {"Martina", "false", "true"}, {"Suhas", "false", "true"},
                {"Cyrus", "false", "true"}, {"Doruk", "true", "true"}});
}

TEST_F(LabelExpressionTest, aLabelExpressionGroupsAndOrdersTheRows) {
    expectRowsInOrder("MATCH (n) RETURN n:Founder|Sales, count(n) ORDER BY n:Founder|Sales",
                      {{"false", "15"}, {"true", "3"}});
}

TEST_F(LabelExpressionTest, aLabelExpressionOverANullNodeIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:NOPE]->(n) "
               "RETURN p.name, n:Person|Interest, n:!Person, n:Person&Interest",
               {{"Remy", "null", "null", "null"}});
}

TEST_F(LabelExpressionTest, aWhereDisjunctionKeepsTheEdgesOfEitherType) {
    expectRows("MATCH (n)-[e]->(m) WHERE e:KNOWS_WELL|INTERESTED_IN RETURN count(e)", {{"18"}});
    expectRows("MATCH (n)-[e]-(m) WHERE e:KNOWS_WELL|INTERESTED_IN RETURN count(n)", {{"36"}});
}

TEST_F(LabelExpressionTest, aWhereNegationKeepsTheEdgesOfAnotherType) {
    expectRows("MATCH (n)-[e]->(m) WHERE e:!INTERESTED_IN RETURN n.name, m.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Ghosts", "Remy"}});
}

// An edge carries exactly one type, so it carries no two types at once
TEST_F(LabelExpressionTest, aConjunctionOfTwoEdgeTypesKeepsNoEdge) {
    expectRows("MATCH (n)-[e]->(m) WHERE e:KNOWS_WELL&INTERESTED_IN RETURN count(e)", {{"0"}});
}

TEST_F(LabelExpressionTest, anEdgeTypeExpressionOverANullEdgeIsNull) {
    expectRows("MATCH (a:Person) OPTIONAL MATCH (a)-[e:KNOWS_WELL]->(b) RETURN a.name, e:KNOWS_WELL|INTERESTED_IN, e:!KNOWS_WELL",
               {{"Remy", "true", "false"}, {"Adam", "true", "false"}, {"Maxime", "null", "null"},
                {"Luc", "null", "null"}, {"Martina", "null", "null"}, {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"}, {"Doruk", "null", "null"}});
}

// In a comprehension a | after the predicate starts the projection, as it did before label
// expressions, so a disjunction there is written in parentheses
TEST_F(LabelExpressionTest, aPipeAfterALabelTestInAComprehensionStartsTheProjection) {
    expectRows("MATCH (n:Founder) RETURN head([x IN [n] WHERE x:Person | x.name])", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n:Person) RETURN n.name, size([x IN [n] WHERE x:(Founder|Sales) | x.name])",
               {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"}, {"Martina", "0"},
                {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "1"}});
}

TEST_F(LabelExpressionTest, aCreatedNodeTakesTheLabelsOfAConjunction) {
    expectWriteRows("CREATE (n:Robot&Toy {name: 'Rob'}) RETURN n:Robot, n:Toy, n:Robot|Doll, n:!Toy",
                    {{"true", "true", "true", "false"}});
    expectRows("MATCH (n:Robot&Toy) RETURN n.name", {{"Rob"}});
}

TEST_F(LabelExpressionTest, aWritePatternTakesNoDisjunctionOrNegation) {
    expectError("CREATE (n:Robot|Toy)", "can only be joined by ':' or '&'");
    expectError("CREATE (n:!Robot)", "can only be joined by ':' or '&'");
    expectError("MERGE (n:Robot|Toy)", "can only be joined by ':' or '&'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
