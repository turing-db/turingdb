#include <gtest/gtest.h>

#include "CallV3Test.h"

using namespace turing::test;

class QuantifiedWalkBoundNameTest : public CallV3Test {
};

TEST_F(QuantifiedWalkBoundNameTest, aBracketedWalkCannotTakeABoundRelationshipName) {
    runQueryExpectingError("MATCH (p)-[e:KNOWS_WELL]->(q) WITH e MATCH (n)-[e*1..3]->(m) RETURN e",
                           "already bound");
}

TEST_F(QuantifiedWalkBoundNameTest, aOneHopBodyCannotTakeABoundRelationshipName) {
    runQueryExpectingError("MATCH (p)-[e:KNOWS_WELL]->(q) MATCH (n)((a)-[e]->(b)){1,3}(m) RETURN m",
                           "already bound");
}

TEST_F(QuantifiedWalkBoundNameTest, aSeveralHopBodyCannotTakeABoundRelationshipName) {
    runQueryExpectingError("MATCH (p)-[e:KNOWS_WELL]->(q) MATCH (n)((a)-[e]->(b)-[f]->(c)){1,3}(m) RETURN m",
                           "already bound");
}
