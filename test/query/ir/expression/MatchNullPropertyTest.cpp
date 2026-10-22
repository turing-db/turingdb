#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class MatchNullPropertyTest : public WriteQueryTest {
};

TEST_F(MatchNullPropertyTest, matchesNoNodeOnANullLiteral) {
    expectRows("MATCH (m:Person {hasPhD: null}) RETURN m", {});
}

TEST_F(MatchNullPropertyTest, matchesNoNodeOnAComparisonWithNull) {
    expectRows("MATCH (m:Person {hasPhD: 1 = null}) RETURN m", {});
}

TEST_F(MatchNullPropertyTest, matchesNoNodeOnAnOrderingAcrossTypes) {
    expectRows("MATCH (n:Person {name: 'Remy'}) MATCH (m:Person {hasPhD: n.name < 5}) RETURN m", {});
}

TEST_F(MatchNullPropertyTest, matchesNoEdgeOnAnOrderingAcrossTypes) {
    expectRows("MATCH ()-[e:KNOWS_WELL {duration: 1 < 'a'}]->() RETURN e", {});
}
