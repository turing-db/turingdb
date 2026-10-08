#include <gtest/gtest.h>

#include "HashJoinQueryTest.h"

using namespace turing::test;

// An UNWIND after a WHERE builds its list from the rows the WHERE keeps. range() raises on a
// step of 0, which size(a.name) - 4 is for Remy and Adam, the two people with an age.
class UnwindListOverKeptRowsTest : public HashJoinQueryTest {
};

// Maxime, Luc, Martina, Suhas, Cyrus and Doruk unwind 1, 0, 1, 2, 2 and 2 elements
TEST_F(UnwindListOverKeptRowsTest, buildsTheListAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "UNWIND range(1, 2, size(a.name) - 4) AS i RETURN count(*)",
                8);
}

TEST_F(UnwindListOverKeptRowsTest, buildsTheListAfterAFilterOfItsOwnFactor) {
    expectCount("MATCH (a:Person), (b:Interest) WHERE a.age IS NULL "
                "UNWIND range(1, 2, size(a.name) - 4) AS i RETURN count(*)",
                80);
}

TEST_F(UnwindListOverKeptRowsTest, buildsNoListWhenTheOtherFactorIsEmpty) {
    expectCount("MATCH (a:Person), (b:Person {name: 'Nobody'}) "
                "UNWIND range(1, 2, size(a.name) - 4) AS i RETURN count(*)",
                0);
}

TEST_F(UnwindListOverKeptRowsTest, dividesForNoRowWhenTheOtherFactorIsEmpty) {
    expectCount("MATCH (a:Person), (b:Person {name: 'Nobody'}) "
                "UNWIND [10 / (size(a.name) - 4)] AS i RETURN count(*)",
                0);
}

TEST_F(UnwindListOverKeptRowsTest, buildsNoConstantListWhenTheOtherFactorIsEmpty) {
    expectCount("MATCH (a:Person), (b:Person {name: 'Nobody'}) UNWIND range(1, 2, 0) AS i RETURN count(*)", 0);
}
