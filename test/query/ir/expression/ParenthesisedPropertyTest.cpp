#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// openCypher's property expression is an atom followed by property lookups, and a
// parenthesised expression is an atom, so (p).age is a property expression wherever p.age is.
class ParenthesisedPropertyTest : public WriteQueryTest {
};

TEST_F(ParenthesisedPropertyTest, readsThePropertyOfAParenthesisedVariable) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN (p).age", {{"32"}});
}

TEST_F(ParenthesisedPropertyTest, removesThePropertyOfAParenthesisedVariable) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) REMOVE (p).age RETURN p.age", {{"null"}});
}

TEST_F(ParenthesisedPropertyTest, setsThePropertyOfAParenthesisedVariable) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET (p).age = 1 RETURN p.age", {{"1"}});
}

TEST_F(ParenthesisedPropertyTest, setsThePropertyOfAVariableInNestedParentheses) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET ((p)).age = 2 RETURN p.age", {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
