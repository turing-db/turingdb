#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET of a null over a list property. A list is one property value, so the null takes the
// whole list off the entity rather than emptying it - and the two values it is easily
// confused with, the empty list and a list holding a null, stay values the property holds.
class SetListPropertyNullTest : public WriteQueryTest {
};

TEST_F(SetListPropertyNullTest, setsAnIntegerListToNull) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");

    expectWriteRows("MATCH (n:Tagged) SET n.tags = null RETURN n.tags", {{"null"}});

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"null"}});
}

TEST_F(SetListPropertyNullTest, setsAStringListToNull) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: ['x', 'yy']})");
    applyWrite("MATCH (n:Tagged) SET n.tags = null");

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"null"}});
}

TEST_F(SetListPropertyNullTest, setsANestedListToNull) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: [[1, 2], [3]]})");
    applyWrite("MATCH (n:Tagged) SET n.tags = null");

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"null"}});
}

TEST_F(SetListPropertyNullTest, setsAnEmptyListToNull) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: []})");
    applyWrite("MATCH (n:Tagged) SET n.tags = null");

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"null"}});
}

// The empty list is a value the property holds; the null is the absence of one
TEST_F(SetListPropertyNullTest, tellsTheEmptyListApartFromTheNull) {
    applyWrite("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    applyWrite("CREATE (b:Tagged {name: 'b', tags: []})");
    applyWrite("MATCH (n:Tagged {name: 'a'}) SET n.tags = null");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.tags", {{"a", "null"}, {"b", "[]"}});

    expectRows("MATCH (n:Tagged) WHERE n.tags IS NULL RETURN n.name", {{"a"}});
}

// A list holding a null is a list, so the property keeps a value
TEST_F(SetListPropertyNullTest, storesAListHoldingANullRatherThanRemovingTheProperty) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: [1, 2]})");
    applyWrite("MATCH (n:Tagged) SET n.tags = [1, null, 3]");

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[1, null, 3]"}});

    expectRows("MATCH (n:Tagged) WHERE n.tags IS NULL RETURN n.name", {});
}

// The one-element list holding a null is the closest a list gets to the null itself
TEST_F(SetListPropertyNullTest, storesASingletonListHoldingANullRatherThanRemovingTheProperty) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: [1, 2]})");
    applyWrite("MATCH (n:Tagged) SET n.tags = [null]");

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[null]"}});
}

TEST_F(SetListPropertyNullTest, setsOneNodesListToNullAndLeavesTheOther) {
    applyWrite("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    applyWrite("CREATE (b:Tagged {name: 'b', tags: ['x', 'y']})");
    applyWrite("MATCH (n:Tagged {name: 'a'}) SET n.tags = null");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.tags", {{"a", "null"}, {"b", "[x, y]"}});
}

TEST_F(SetListPropertyNullTest, theListSetToNullNoLongerMatchesItsOldValue) {
    applyWrite("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    applyWrite("CREATE (b:Tagged {name: 'b', tags: [1, 2]})");
    applyWrite("MATCH (n:Tagged {name: 'a'}) SET n.tags = null");

    expectRows("MATCH (n:Tagged {tags: [1, 2]}) RETURN n.name", {{"b"}});
}

TEST_F(SetListPropertyNullTest, setsAListToNullOnAnEdge) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "CREATE (a)-[e:TAGGED {tags: [1, 2]}]->(b)");

    expectWriteRows("MATCH ()-[e:TAGGED]->() SET e.tags = null RETURN e.tags", {{"null"}});

    expectRows("MATCH ()-[e:TAGGED]->() RETURN e.tags", {{"null"}});
}

// The property type survives the removal, so a later list lands in the same column
TEST_F(SetListPropertyNullTest, storesAListAgainAfterTheNull) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: [1, 2]})");
    applyWrite("MATCH (n:Tagged) SET n.tags = null");
    applyWrite("MATCH (n:Tagged) SET n.tags = [9]");

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[9]"}});
}

// A list-valued name no entity carries: the clause writes nothing and interns no type
TEST_F(SetListPropertyNullTest, setsToNullAListPropertyNoEntityCarries) {
    expectWriteRowCount("MATCH (p:Person) SET p.tags = null RETURN p.name", 8);

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.tags", {{"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
