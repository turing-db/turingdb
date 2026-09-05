#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// What a projection behind a SET reads: the value the statement just wrote, not the one
// the graph held before it.
class SetReadbackTest : public WriteQueryTest {
};

TEST_F(SetReadbackTest, readsThePropertyAPlainSetWroteOnAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 99 RETURN p.age", {{"99"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"99"}});
}

// A merge's ON MATCH writes over the rows it bound, which are entities the graph holds
TEST_F(SetReadbackTest, readsThePropertyOnMatchSetOnARowItBound) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) ON MATCH SET p.age = 1 RETURN p.age", {{"1"}});
}

// A property the node did not carry reads as the value the SET gave it, not as null
TEST_F(SetReadbackTest, readsThePropertyASetAddedToANodeWithoutIt) {
    expectWriteRowCount("CREATE (t:Tag {name: 'x'})", 0);

    expectWriteRows("MATCH (t:Tag) SET t.age = 5 RETURN t.age", {{"5"}});
}

// A string is read back as the value the SET wrote, whose bytes the change owns
TEST_F(SetReadbackTest, readsTheStringAPlainSetWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.name = 'Remi' RETURN p.name", {{"Remi"}});
}

// The edge sibling of the first case
TEST_F(SetReadbackTest, readsThePropertyAPlainSetWroteOnAMatchedEdge) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
                    "SET e.duration = 3 "
                    "RETURN e.duration",
                    {{"3"}});
}

// The value the SET computes is read off the graph, so the expression sees what the
// property held before the statement wrote to it
TEST_F(SetReadbackTest, readsTheValueTheSetComputedFromTheOldOne) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = p.age + 1 RETURN p.age", {{"33"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
