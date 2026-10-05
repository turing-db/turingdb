#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE binding a node a CREATE in the same query wrote: every row of it is pending, and
// the merge matches against what this change wrote rather than the committed graph
class MergeFromCreatedNodeTest : public WriteQueryTest {
};

TEST_F(MergeFromCreatedNodeTest, mergesAnEdgeFromACreatedNode) {
    expectWriteRows("CREATE (n:Person {name: 'Kai'}) MERGE (n)-[:OWNS]->(c:Car {owner: 'Kai'}) RETURN n.name, c.owner",
                    {{"Kai", "Kai"}});

    expectRows("MATCH (p:Person)-[:OWNS]->(c:Car) RETURN p.name, c.owner", {{"Kai", "Kai"}});
}

TEST_F(MergeFromCreatedNodeTest, mergesAnEdgeFromEachCreatedNode) {
    expectWriteRows("UNWIND ['Kai', 'Nia'] AS name CREATE (n:Person {name: name}) "
                    "MERGE (n)-[:OWNS]->(:Car {owner: name}) RETURN name",
                    {{"Kai"}, {"Nia"}});

    expectRows("MATCH (p:Person)-[:OWNS]->(c:Car) RETURN p.name, c.owner", {{"Kai", "Kai"}, {"Nia", "Nia"}});
}

// Remy KNOWS_WELL Adam in the graph: Kai does not, whatever ID the change gave Kai
TEST_F(MergeFromCreatedNodeTest, matchesNoCommittedEdgeFromACreatedNode) {
    expectWriteRows("CREATE (n:Person {name: 'Kai'}) MERGE (n)-[:KNOWS_WELL]->(a:Person {name: 'Adam'}) RETURN a.name",
                    {{"Adam"}});

    expectRows("MATCH (p:Person {name: 'Kai'})-[:KNOWS_WELL]->(a:Person) RETURN a.name", {{"Adam"}});
    expectRows("MATCH (a:Person {name: 'Adam'}) RETURN count(a)", {{"2"}});
}

TEST_F(MergeFromCreatedNodeTest, matchesTheEdgeAnEarlierMergeWroteFromACreatedNode) {
    expectWriteRows("CREATE (n:Person {name: 'Kai'}) "
                    "MERGE (n)-[:OWNS]->(:Car {owner: 'Kai'}) "
                    "MERGE (n)-[:OWNS]->(:Car {owner: 'Kai'}) "
                    "RETURN n.name",
                    {{"Kai"}});

    expectRows("MATCH (c:Car) RETURN count(c)", {{"1"}});
}

TEST_F(MergeFromCreatedNodeTest, mergesAHopToANewNodeFromACreatedNode) {
    expectWriteRows("CREATE (a:Tag {name: 'x'}) MERGE (a)-[:LINKS]->(b:Tag {name: 'y'}) RETURN a.name, b.name",
                    {{"x", "y"}});

    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN a.name, b.name", {{"x", "y"}});
}
