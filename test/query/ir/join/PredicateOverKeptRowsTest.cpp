#include <gtest/gtest.h>

#include "HashJoinQueryTest.h"

using namespace turing::test;

// A WHERE after a WITH runs on the rows the earlier WHERE kept. range() raises on a step of
// 0, which size(a.name) - 4 is for Remy and Adam, the two people with an age.
class PredicateOverKeptRowsTest : public HashJoinQueryTest {
};

// Maxime, Martina, Suhas, Cyrus and Doruk build a non-empty list, Luc an empty one
TEST_F(PredicateOverKeptRowsTest, runsAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE size(range(1, 2, size(a.name) - 4)) > 0 RETURN count(*)",
                5);
}

TEST_F(PredicateOverKeptRowsTest, runsOnNoRowWhenTheOtherFactorIsEmpty) {
    expectCount("MATCH (a:Person), (b:Person {name: 'Nobody'}) "
                "WHERE size(range(1, 2, size(a.name) - 4)) > 0 RETURN count(*)",
                0);
}

// 10 / 2, 10 / 3 and 10 / 1 are positive, Luc's 10 / -1 is not
TEST_F(PredicateOverKeptRowsTest, dividesAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE 10 / (size(a.name) - 4) > 0 RETURN count(*)",
                5);
}

TEST_F(PredicateOverKeptRowsTest, dividesOnNoRowAnEdgeHopDrops) {
    expectCount("MATCH (a:Person)-->(b {name: 'Nobody'}) WHERE 10 / (a.age - 32) > 0 RETURN count(*)", 0);
}

TEST_F(PredicateOverKeptRowsTest, dividesOnNoRowAnUnwindDrops) {
    expectCount("MATCH (a:Person) UNWIND a.missing AS x WITH a, x WHERE 10 / (a.age - 32) > 0 RETURN count(*)", 0);
}

// 3e17 microseconds times 32 overflows a duration; the six people without an age give null
TEST_F(PredicateOverKeptRowsTest, multipliesADurationAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE duration(300000000000000000) * a.age IS NULL RETURN count(*)",
                6);
}
