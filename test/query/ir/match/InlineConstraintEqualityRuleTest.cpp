#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class InlineConstraintEqualityRuleTest : public WriteQueryTest {
};

TEST_F(InlineConstraintEqualityRuleTest, rejectsTwoDoublesOnANode) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.height = 1.8");

    expectError("MATCH (n {height: 1.8}) RETURN n.name",
                "Cannot evaluate node property: Equality of types 'Double' and 'Double' is not encouraged");
}

TEST_F(InlineConstraintEqualityRuleTest, rejectsTwoDoublesOnAnEdge) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() SET e.weight = 1.5");

    expectError("MATCH ()-[e {weight: 1.5}]->() RETURN e",
                "Cannot evaluate edge property: Equality of types 'Double' and 'Double' is not encouraged");
}

TEST_F(InlineConstraintEqualityRuleTest, comparesANumberByValue) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.height = 1.8");

    expectRows("MATCH (n {age: 32.0}) RETURN n.name", {{"Adam"}, {"Remy"}});
    expectRows("MATCH (n {age: 32.5}) RETURN n.name", {});
    expectRows("MATCH (n {height: 2}) RETURN n.name", {});
}

TEST_F(InlineConstraintEqualityRuleTest, matchesNoRowForAValueOfAnotherType) {
    expectRows("MATCH (n {name: 32}) RETURN n.name", {});
    expectRows("MATCH (n {age: 'thirty-two'}) RETURN n.name", {});
    expectRows("MATCH (n {isFrench: [true]}) RETURN n.name", {});
}
