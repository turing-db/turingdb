#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class CountAfterWriteTest : public WriteQueryTest {
};

TEST_F(CountAfterWriteTest, countsNoHolderOfThePropertyRemovedFromEveryScannedNode) {
    expectWriteRows("MATCH (p:Person) REMOVE p.age RETURN count(p.age)", {{"0"}});
}

// simpledb holds 8 Person nodes, 2 of them with an age
TEST_F(CountAfterWriteTest, countsEveryHolderOfThePropertySetOnEveryScannedNode) {
    expectWriteRows("MATCH (p:Person) SET p.age = 1 RETURN count(p.age)", {{"8"}});
}

TEST_F(CountAfterWriteTest, countsNoHolderOfThePropertySetToNullOnEveryScannedNode) {
    expectWriteRows("MATCH (p:Person) SET p.age = null RETURN count(p.age)", {{"0"}});
}

TEST_F(CountAfterWriteTest, countsNoHolderOfThePropertyRemovedFromEveryNodeOfAnUnlabelledScan) {
    expectWriteRows("MATCH (n) REMOVE n.name RETURN count(n.name)", {{"0"}});
}

TEST_F(CountAfterWriteTest, countsNoHolderOfThePropertyRemovedFromOneFactorOfAProduct) {
    expectWriteRows("MATCH (p:Person), (q:Interest) REMOVE p.age RETURN count(p.age)", {{"0"}});
}

TEST_F(CountAfterWriteTest, countsNoHolderOfThePropertyRemovedBehindAWith) {
    expectWriteRows("MATCH (p:Person) REMOVE p.age WITH p RETURN count(p.age)", {{"0"}});
}

TEST_F(CountAfterWriteTest, countsNoHolderOfThePropertyRemovedInTheWithThatCounts) {
    expectWriteRows("MATCH (p:Person) REMOVE p.age WITH count(p.age) AS c RETURN c", {{"0"}});
}

TEST_F(CountAfterWriteTest, countsTheNodeAnEarlierPartCreated) {
    expectWriteRows("CREATE (:Person {name: 'z'}) WITH 1 AS x MATCH (n:Person) RETURN count(n)", {{"9"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
