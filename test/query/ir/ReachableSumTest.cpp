#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Only Remy and Adam have an age, 32 each. Within 3 hops Remy reaches Adam, Ghosts,
// Computers, Eighties, itself, Bio and Cooking, over 14 walks: 2 end at Adam, 2 at Remy.
class ReachableSumTest : public WriteQueryTest {
};

TEST_F(ReachableSumTest, sumsOnePropertyPerWalk) {
    expectRows("MATCH (s {name:'Remy'})-[*1..3]->(n) RETURN count(*), sum(n.age)", {{"14", "128"}});
}

TEST_F(ReachableSumTest, sumsOnePropertyPerReachableNode) {
    expectRows("MATCH (s {name:'Remy'})-[*1..3]->(n) WITH DISTINCT n RETURN count(*), sum(n.age)", {{"7", "64"}});
}

TEST_F(ReachableSumTest, sumsFromASeedThatReachesItselfFirst) {
    expectRows("MATCH (s {name:'Ghosts'})-[*1..3]->(n) RETURN count(*), sum(n.age)", {{"8", "96"}});
    expectRows("MATCH (s {name:'Ghosts'})-[*1..3]->(n) WITH DISTINCT n RETURN count(*), sum(n.age)", {{"7", "64"}});
}

TEST_F(ReachableSumTest, sumsToZeroWhenNoReachableNodeHasTheProperty) {
    expectRows("MATCH (s {name:'Luc'})-[*1..3]->(n) WITH DISTINCT n RETURN count(*), sum(n.age)", {{"2", "0"}});
}

TEST_F(ReachableSumTest, sumsPerSeed) {
    expectRows("MATCH (s:Person)-[*1..3]->(n) WITH DISTINCT s, n RETURN s.name, sum(n.age)",
               {{"Remy", "64"},
                {"Adam", "64"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}
