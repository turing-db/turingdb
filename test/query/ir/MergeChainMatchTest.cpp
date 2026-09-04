#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The matches a merge chain enumerates: one row per path the graph holds, whichever way
// several of them run through the same node or the same edge.
class MergeChainMatchTest : public WriteQueryTest {
};

// Two paths meet at the middle Tag, so the second hop walks that node's edges once
// however many partial matches reached it: one row per path, not one per (match, edge)
// pair of a node reached twice
TEST_F(MergeChainMatchTest, bindsOneRowPerPathThroughASharedMiddleNode) {
    // A merge writes the whole pattern it does not find, fresh ends and all, so the two
    // paths only meet at one middle node if that node is bound rather than described
    expectWriteRowCount("MERGE (m:Tag {name: 'm'})-[:LINKS]->(z:Tag {name: 'z'})", 0);
    expectWriteRowCount("MATCH (m:Tag {name: 'm'}) MERGE (a:Tag {name: 'a1'})-[:LINKS]->(m)", 0);
    expectWriteRowCount("MATCH (m:Tag {name: 'm'}) MERGE (a:Tag {name: 'a2'})-[:LINKS]->(m)", 0);

    expectWriteRows("MERGE (a:Tag)-[:LINKS]->(b:Tag)-[:LINKS]->(c:Tag) RETURN a.name, c.name",
                    {{"a1", "z"}, {"a2", "z"}});

    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN count(*)", {{"3"}});
}

// An undirected hop takes the edge either way round, and a self-loop is an out-edge and
// an in-edge of the one node: it is one edge, so it binds one row
TEST_F(MergeChainMatchTest, bindsASelfLoopOnceForAnUndirectedHop) {
    expectWriteRowCount("MATCH (a:Person {name: 'Remy'}) MERGE (a)-[:LOOPS]->(a)", 0);

    expectWriteRows("MATCH (a:Person {name: 'Remy'}) MERGE (a)-[e:LOOPS]-(a) RETURN a.name",
                    {{"Remy"}});

    expectRows("MATCH (a:Person {name: 'Remy'})-[e:LOOPS]->(b) RETURN count(e)", {{"1"}});
}

// The pending sibling of the case above: the loop the same query wrote is in no graph
// the match reads, so the undirected hop finds it in the write log - once, from the one
// side the outgoing pass already covers
TEST_F(MergeChainMatchTest, bindsAPendingSelfLoopOnceForAnUndirectedHop) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}) "
                    "MERGE (a)-[:LOOPS]->(a) "
                    "MERGE (a)-[:LOOPS]-(a) "
                    "RETURN a.name",
                    {{"Remy"}});

    expectRows("MATCH (a:Person {name: 'Remy'})-[e:LOOPS]->(b) RETURN count(e)", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
