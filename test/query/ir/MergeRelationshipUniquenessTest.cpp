#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Each hop of a merge chain binds an edge of its own - Cypher's relationship isomorphism -
// which is what stops an undirected chain from walking back along the edge it arrived on.
class MergeRelationshipUniquenessTest : public WriteQueryTest {
};

// The graph holds one LINKS edge, and both hops are undirected: the second would walk
// back along the first's edge, which is not a match, so the whole chain is written
TEST_F(MergeRelationshipUniquenessTest, writesAChainRatherThanWalkingOneEdgeTwice) {
    expectWriteRowCount("MERGE (a:Tag {name: 'a'})-[:LINKS]->(b:Tag {name: 'b'})", 0);

    expectWriteRowCount("MERGE (x:Tag)-[:LINKS]-(y:Tag)-[:LINKS]-(z:Tag)", 0);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"5"}});
    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN count(*)", {{"3"}});
}

// The rule excludes a repeated edge and nothing else: a chain over two distinct ones is
// still found, so nothing is written
TEST_F(MergeRelationshipUniquenessTest, bindsAChainOverTwoDistinctEdges) {
    expectWriteRowCount("MERGE (a:Tag {name: 'a'})-[:LINKS]->(b:Tag {name: 'b'})"
                        "-[:LINKS]->(c:Tag {name: 'c'})",
                        0);

    expectWriteRows("MERGE (x:Tag {name: 'a'})-[:LINKS]-(y:Tag {name: 'b'})"
                    "-[:LINKS]-(z:Tag {name: 'c'}) "
                    "RETURN z.name",
                    {{"c"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
