#include <gtest/gtest.h>

#include "CallV3Test.h"

using namespace turing::test;

class QuantifiedNestedBodyTest : public CallV3Test {
};

TEST_F(QuantifiedNestedBodyTest, aOneHopBodyCannotRepeatAQuantifiedPattern) {
    runQueryExpectingError("MATCH (n)((a)((x)-[]->(y)){2}(b)){1,3}(m) RETURN m",
                           "cannot repeat a pattern that is quantified itself");
}

TEST_F(QuantifiedNestedBodyTest, aSeveralHopBodyCannotRepeatAQuantifiedPattern) {
    runQueryExpectingError("MATCH (n)((a)((x)-[]->(y)){2}(b)-[]->(c)){1,3}(m) RETURN m",
                           "cannot repeat a pattern that is quantified itself");
}

TEST_F(QuantifiedNestedBodyTest, aBracketQuantifierCannotTakeASecondOne) {
    runQueryExpectingError("MATCH (n)((a)-[*1..2]->(b)){1,3}(m) RETURN m",
                           "takes one length quantifier");
}
