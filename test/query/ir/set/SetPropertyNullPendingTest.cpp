#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET of a null on an entity the same query created. The null lands on the entity the
// write buffer holds rather than as an update, and the change submits the entity without
// a value for the property.
class SetPropertyNullPendingTest : public WriteQueryTest {
};

TEST_F(SetPropertyNullPendingTest, submitsThePendingNodeWithoutThePropertySetToNull) {
    applyWrite("CREATE (t:Tag {name: 'x', v: 1}) WITH t SET t.v = null");

    expectRows("MATCH (t:Tag) RETURN t.name, t.v", {{"x", "null"}});
    expectRows("MATCH (t:Tag) RETURN count(t.v)", {{"0"}});
}

// Remy and Adam are the only nodes of simpledb carrying an age
TEST_F(SetPropertyNullPendingTest, setsToNullAPropertyThePendingNodeWasNotCreatedWith) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH t SET t.age = null RETURN t.age", {{"null"}});

    expectRows("MATCH (n) WHERE n.age IS NOT NULL RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(SetPropertyNullPendingTest, setsThePropertyToNullOnThePendingRowACaseLeavesNull) {
    expectWriteRows("UNWIND [1, 2, 3] AS x CREATE (t:Tag {v: x}) WITH t, x "
                    "SET t.v = CASE WHEN x = 2 THEN null ELSE x END "
                    "RETURN x, t.v",
                    {{"1", "1"}, {"2", "null"}, {"3", "3"}});

    expectRows("MATCH (t:Tag) RETURN t.v", {{"1"}, {"null"}, {"3"}});
}

TEST_F(SetPropertyNullPendingTest, writesTheValueSetAfterTheNullOnAPendingNode) {
    expectWriteRows("CREATE (t:Tag {name: 'x', v: 1}) WITH t SET t.v = null, t.v = 2 RETURN t.v", {{"2"}});

    expectRows("MATCH (t:Tag) RETURN t.v", {{"2"}});
}

TEST_F(SetPropertyNullPendingTest, removesTheValueSetBeforeTheNullOnAPendingNode) {
    expectWriteRows("CREATE (t:Tag {name: 'x', v: 1}) WITH t SET t.v = 2, t.v = null RETURN t.v", {{"null"}});

    expectRows("MATCH (t:Tag) RETURN t.v", {{"null"}});
}

TEST_F(SetPropertyNullPendingTest, submitsThePendingEdgeWithoutThePropertySetToNull) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "CREATE (a)-[e:TAGGED {name: 't', w: 1}]->(b) WITH e "
                    "SET e.w = null "
                    "RETURN e.w",
                    {{"null"}});

    expectRows("MATCH ()-[e:TAGGED]->() RETURN e.name, e.w", {{"t", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
