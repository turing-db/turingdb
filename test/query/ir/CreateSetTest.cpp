#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET in the part whose CREATE wrote the entity, with no WITH between the two clauses
class CreateSetTest : public WriteQueryTest {
};

TEST_F(CreateSetTest, setsAPropertyOnACreatedNode) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' RETURN t.name", {{"x"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
}

TEST_F(CreateSetTest, overwritesAPropertyTheCreateWrote) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.name = 'y' RETURN t.name", {{"y"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"y"}});
}

TEST_F(CreateSetTest, setsAPropertyNoEntityCarries) {
    expectWriteRows("CREATE (t:Tag) SET t.colour = 'red' RETURN t.colour", {{"red"}});

    expectRows("MATCH (t:Tag) RETURN t.colour", {{"red"}});
}

TEST_F(CreateSetTest, setsEveryPropertyOfEveryClause) {
    expectWriteRows("CREATE (p:Person) SET p.name = 'Ava', p.age = 1 SET p.hasPhD = false "
                    "RETURN p.name, p.age, p.hasPhD",
                    {{"Ava", "1", "false"}});

    expectRows("MATCH (p:Person {name: 'Ava'}) RETURN p.age, p.hasPhD", {{"1", "false"}});
}

TEST_F(CreateSetTest, setsAPropertyOnACreatedEdge) {
    expectWriteRows("CREATE (a:Tag {name: 'a'})-[e:TAGS]->(b:Tag {name: 'b'}) SET e.duration = 3 "
                    "RETURN e.duration",
                    {{"3"}});

    expectRows("MATCH (a:Tag)-[e:TAGS]->(b:Tag) RETURN a.name, b.name, e.duration", {{"a", "b", "3"}});
}

// One Tag per Person, each set from the row that created it. Remy and Adam are the two
// Person nodes carrying an age, both 32
TEST_F(CreateSetTest, setsEachCreatedNodeFromItsOwnRow) {
    applyWrite("MATCH (p:Person) CREATE (t:Tag {name: p.name}) SET t.age = p.age");

    expectRows("MATCH (t:Tag) WHERE t.age IS NOT NULL RETURN t.name, t.age", {{"Remy", "32"}, {"Adam", "32"}});
    expectCounts("MATCH (t:Tag) RETURN count(t)", {8});
}

TEST_F(CreateSetTest, setsAPropertyFromOneTheCreateWrote) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.alias = t.name RETURN t.alias", {{"x"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.alias", {{"x", "x"}});
}

TEST_F(CreateSetTest, setsAPropertyTheCreateWroteToNull) {
    expectWriteRows("CREATE (t:Tag {name: 'x', dob: '01/01'}) SET t.name = null RETURN t.name, t.dob",
                    {{"null", "01/01"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.dob", {{"null", "01/01"}});
}

TEST_F(CreateSetTest, setsAMatchedNodeBehindACreate) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CREATE (t:Tag {name: 'x'}) SET p.age = 33 RETURN p.age",
                    {{"33"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"33"}});
}

TEST_F(CreateSetTest, createsAnEdgeFromAPropertyTheSetWrote) {
    applyWrite("CREATE (a:Tag) SET a.name = 'a' CREATE (a)-[:TAGS]->(b:Tag {name: a.name})");

    expectRows("MATCH (a:Tag)-[:TAGS]->(b:Tag) RETURN a.name, b.name", {{"a", "a"}});
}

TEST_F(CreateSetTest, filtersOnTheSetPropertyBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t WHERE t.name = 'x' RETURN t.name", {{"x"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
