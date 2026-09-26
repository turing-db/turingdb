#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// What a MERGE bound - some rows written by it, the others matched - passes a WITH like any
// other variable. Remy is 32 in simpledb.
class MergeThroughWithTest : public WriteQueryTest {
};

TEST_F(MergeThroughWithTest, readsANodeTheMergeCreatedPastAWith) {
    expectWriteRows("MERGE (t:Thing {name: 'T4'}) WITH t RETURN t.name", {{"T4"}});
}

TEST_F(MergeThroughWithTest, readsANodeTheMergeMatchedPastAWith) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) WITH p RETURN p.age", {{"32"}});
}

TEST_F(MergeThroughWithTest, readsCreatedAndMatchedNodesPastAWith) {
    expectWriteRows("UNWIND ['Remy', 'Zoe'] AS n MERGE (p:Person {name: n}) WITH p RETURN p.name, p.age",
                    {{"Remy", "32"}, {"Zoe", "null"}});
}

TEST_F(MergeThroughWithTest, setsCreatedAndMatchedNodesPastAWith) {
    applyWrite("UNWIND ['Remy', 'Zoe'] AS n MERGE (p:Person {name: n}) WITH p SET p.visits = 1");

    expectRows("MATCH (p:Person) WHERE p.visits IS NOT NULL RETURN p.name, p.visits", {{"Remy", "1"}, {"Zoe", "1"}});
}

TEST_F(MergeThroughWithTest, readsAMergedNodePastAnAliasingWith) {
    expectWriteRows("MERGE (p:Person {name: 'Zoe'}) WITH p AS q RETURN q.name", {{"Zoe"}});
}

TEST_F(MergeThroughWithTest, readsAMergedNodePastAnOrderingWith) {
    expectWriteRows("UNWIND ['Zoe', 'Remy'] AS n MERGE (p:Person {name: n}) WITH p ORDER BY p.name RETURN p.name, p.age",
                    {{"Remy", "32"}, {"Zoe", "null"}});
}

TEST_F(MergeThroughWithTest, readsAMergedNodePastADistinctWith) {
    expectWriteRows("UNWIND ['Zoe', 'Remy', 'Zoe'] AS n MERGE (p:Person {name: n}) WITH DISTINCT p RETURN p.name, p.age",
                    {{"Remy", "32"}, {"Zoe", "null"}});
}

// The KNOWS_WELL edge from Remy to Adam lasts 20
TEST_F(MergeThroughWithTest, readsAMergedEdgePastAWith) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "MERGE (a)-[e:KNOWS_WELL]->(b) "
                    "WITH e RETURN e.duration",
                    {{"20"}});
}

TEST_F(MergeThroughWithTest, groupsOnAMergedNodePastAWith) {
    expectWriteRows("UNWIND ['Remy', 'Zoe', 'Zoe'] AS n MERGE (p:Person {name: n}) WITH p, count(*) AS c RETURN p.name, c",
                    {{"Remy", "1"}, {"Zoe", "2"}});
}

TEST_F(MergeThroughWithTest, readsAMergedNodePastTheGroupsItKeys) {
    expectWriteRows("UNWIND ['Zoe', 'Remy', 'Zoe'] AS n MERGE (p:Person {name: n}) WITH p, count(*) AS c ORDER BY p.name RETURN p.name, p.age, c",
                    {{"Remy", "32", "1"}, {"Zoe", "null", "2"}});
}

TEST_F(MergeThroughWithTest, mergesAnEdgeFromAMergedNodePastAWithAndAMatch) {
    expectWriteRows("UNWIND ['Remy', 'Zed'] AS name "
                    "MERGE (p:Person {name: name}) "
                    "WITH p MATCH (g:Interest {name: 'Ghosts'}) "
                    "MERGE (p)-[:LIKES]->(g) "
                    "RETURN p.name, g.name",
                    {{"Remy", "Ghosts"}, {"Zed", "Ghosts"}});

    expectRows("MATCH (p:Person)-[:LIKES]->(g:Interest) RETURN p.name, g.name", {{"Remy", "Ghosts"}, {"Zed", "Ghosts"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
