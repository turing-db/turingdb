#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The hops of a CREATE chain past the first: each is written between the two nodes it
// joins, whichever of them the change has already committed.
class CreateChainTest : public WriteQueryTest {
};

// The chain starts at a node a merge bound - committed, so no offset - and runs on into
// two nodes the CREATE writes: the second hop joins those two, not the one the chain
// started at
TEST_F(CreateChainTest, writesTheSecondHopBetweenTheNodesItJoins) {
    expectWriteRowCount("CREATE (t:Tag {name: 'x'})", 0);

    expectWriteRowCount("MERGE (m:Tag {name: 'x'}) "
                        "CREATE (m)-[:LINKS]->(b:Tag {name: 'b'})-[:LINKS]->(c:Tag {name: 'c'})",
                        0);

    expectRows("MATCH (m:Tag {name: 'x'})-[:LINKS]->(t:Tag) RETURN t.name", {{"b"}});
    expectRows("MATCH (b:Tag {name: 'b'})-[:LINKS]->(t:Tag) RETURN t.name", {{"c"}});
    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN count(*)", {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
