#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A variable-length walk in a WHEN branch whose end is filtered on a property: the
// fuse_explore_end_set pass binds that end to a set scanned ahead of the function, which
// every row of the branch shares. KNOWS_WELL leaves Remy for Adam, Adam for Remy and Ghosts
// for Remy, so Ghosts and Adam reach Adam in two hops and Remy in one.
class ConditionalExploreEndSetTest : public WriteQueryTest {
};

TEST_F(ConditionalExploreEndSetTest, walksToTheSetInAStandaloneWhen) {
    expectRows("WHEN true THEN MATCH (i:Interest)-[:KNOWS_WELL*2]->(a) WHERE a.name = 'Adam' RETURN i.name AS i",
               {{"Ghosts"}});

    expectRows("WHEN false THEN RETURN 'none' AS i "
               "ELSE MATCH (i)-[:KNOWS_WELL*2]->(a) WHERE a.name = 'Adam' RETURN i.name AS i",
               {{"Ghosts"}, {"Adam"}});
}

TEST_F(ConditionalExploreEndSetTest, walksToTheSetInACallBranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL*2]->(a) WHERE a.name = 'Adam' RETURN a.name AS a "
               "  ELSE MATCH (p)-[:INTERESTED_IN]->(a) WHERE a.name = 'Gym' RETURN a.name AS a "
               "} "
               "RETURN p.name, a",
               {{"Adam", "Adam"}, {"Suhas", "Gym"}, {"Cyrus", "Gym"}, {"Doruk", "Gym"}});
}

TEST_F(ConditionalExploreEndSetTest, walksToTheSetInACountBranch) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL*1..2]->(a) WHERE a.name = 'Adam' }",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(ConditionalExploreEndSetTest, walksToTheNodesBeforeTheSet) {
    expectRows("WHEN true THEN "
               "  MATCH (i:Interest)-[:KNOWS_WELL*1]->(b)-[:KNOWS_WELL]->(c) WHERE c.name = 'Adam' "
               "  RETURN i.name AS i, b.name AS b",
               {{"Ghosts", "Remy"}});
}

TEST_F(ConditionalExploreEndSetTest, walksToTheSetInANestedWhen) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN p.isFrench THEN { "
               "    WHEN p.hasPhD THEN MATCH (p)-[:KNOWS_WELL*2]->(a) WHERE a.name = 'Remy' RETURN a.name AS a "
               "  } "
               "} "
               "RETURN p.name, a",
               {{"Remy", "Remy"}});
}
