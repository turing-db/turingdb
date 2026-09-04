#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A projection reading what a CREATE bound: the rows a CREATE ... RETURN emits, over the
// nodes and edges the pattern wrote rather than over the ones a MATCH found.
class CreateReturnTest : public WriteQueryTest {
};

TEST_F(CreateReturnTest, returnsAPropertyOfTheNodeItCreated) {
    expectWriteRows("CREATE (n:Tag {name: 'x'}) RETURN n.name", {{"x"}});
}

TEST_F(CreateReturnTest, returnsTheNodeItCreated) {
    expectWriteRows("CREATE (n:Tag) RETURN n", {{"18"}});
}

TEST_F(CreateReturnTest, multiCreate) {
    expectWriteRows("CREATE (n:Tag) CREATE (m:Tag) RETURN n, m", {{"18", "19"}});
    expectWriteRows("CREATE (n:Tag) CREATE (m:Tag) RETURN n, m", {{"20", "21"}});
    expectWriteRows("CREATE (s:Src)-[e:E]->(t:Tgt) RETURN s, e, t", {{"22", "18", "23"}});
    expectWriteRows("CREATE (a:A) CREATE (a)-[e:E]->(b:B) RETURN a, e, b", {{"24", "19", "25"}});
    expectWriteRows("MATCH (n:Src), (m:Tgt) CREATE (n)<-[e:New]-(m) RETURN e", {{"20"}});

    expectWriteRows("commit", {});
    expectWriteRows("MATCH (a:A)-[e:E]->(b:B) RETURN *", {{"24", "19", "25"}});
}

// The CREATE wrote no age, and a property it did not write is null rather than whatever
// the graph holds for the ID the provisional one collides with
TEST_F(CreateReturnTest, returnsNullForAPropertyTheCreateDidNotWrite) {
    expectWriteRows("CREATE (n:Tag {name: 'x'}) RETURN n.age", {{"null"}});
}

// One Tag per Person, each carrying that Person's name
TEST_F(CreateReturnTest, returnsANodePerMatchedRow) {
    expectWriteRows("MATCH (p:Person) CREATE (t:Tag {name: p.name}) RETURN t.name",
                    {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"},
                     {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

// The rows a barrier published drive the write, and the projection reads the nodes it wrote
TEST_F(CreateReturnTest, returnsANodeCreatedOverTheRowsABarrierPublished) {
    expectWriteRows("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 "
                    "CREATE (t:Tag {name: name}) "
                    "RETURN t.name",
                    {{"Adam"}, {"Cyrus"}});
}

TEST_F(CreateReturnTest, returnsBothEndsOfThePatternItCreated) {
    expectWriteRows("CREATE (a:Tag {name: 'a'})-[:LINK]->(b:Tag {name: 'b'}) "
                    "RETURN a.name, b.name",
                    {{"a", "b"}});
}

TEST_F(CreateReturnTest, returnsAPropertyOfTheEdgeItCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                    "CREATE (a)-[e:KNOWS_WELL {name: 'Remy -> Luc'}]->(b) "
                    "RETURN e.name",
                    {{"Remy -> Luc"}});
}

// The node the projection returned is the node the change wrote
TEST_F(CreateReturnTest, returnsTheNodeTheChangeKept) {
    expectWriteRows("CREATE (n:Tag {name: 'kept'}) RETURN n.name", {{"kept"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"kept"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv,
                                        [] { testing::GTEST_FLAG(repeat) = 5; });
}
