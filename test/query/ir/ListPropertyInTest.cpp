#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// IN over a list a node or an edge holds as a property: its right operand is the nullable
// list column a property fetch produces, rather than the plain one a literal spells out.
class ListPropertyInTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("CREATE (a:Tagged {name: 'a', tags: [1, 2, 3]})");
        applyWrite("CREATE (b:Tagged {name: 'b', tags: ['x', 'y']})");
        applyWrite("CREATE (c:Tagged {name: 'c'})");
    }
};

TEST_F(ListPropertyInTest, findsAnElementOfAStoredList) {
    expectRows("MATCH (n:Tagged) RETURN n.name, 2 IN n.tags",
               {{"a", "true"}, {"b", "false"}, {"c", "null"}});
}

TEST_F(ListPropertyInTest, filtersOnAnElementOfAStoredList) {
    expectRows("MATCH (n:Tagged) WHERE 2 IN n.tags RETURN n.name", {{"a"}});
}

TEST_F(ListPropertyInTest, findsAStringElementOfAStoredList) {
    expectRows("MATCH (n:Tagged) RETURN n.name, 'x' IN n.tags",
               {{"a", "false"}, {"b", "true"}, {"c", "null"}});
}

TEST_F(ListPropertyInTest, answersNullWhereTheNodeHoldsNoList) {
    expectRows("MATCH (n:Tagged {name: 'c'}) RETURN 2 IN n.tags", {{"null"}});
}

TEST_F(ListPropertyInTest, negatesTheMembership) {
    expectRows("MATCH (n:Tagged) WHERE NOT 2 IN n.tags RETURN n.name", {{"b"}});
}

TEST_F(ListPropertyInTest, findsNoElementOfAnEmptyStoredList) {
    applyWrite("CREATE (d:Tagged {name: 'd', tags: []})");

    expectRows("MATCH (n:Tagged {name: 'd'}) RETURN 2 IN n.tags", {{"false"}});
}

TEST_F(ListPropertyInTest, answersNullWhereTheStoredListHoldsANull) {
    applyWrite("CREATE (d:Tagged {name: 'd', tags: [1, null, 3]})");

    expectRows("MATCH (n:Tagged {name: 'd'}) RETURN 3 IN n.tags, 2 IN n.tags",
               {{"true", "null"}});
}

TEST_F(ListPropertyInTest, findsNoScalarInAStoredListOfLists) {
    applyWrite("CREATE (d:Tagged {name: 'd', tags: [[1, 2], [3]]})");

    expectRows("MATCH (n:Tagged {name: 'd'}) RETURN 1 IN n.tags", {{"false"}});
}

TEST_F(ListPropertyInTest, answersNullForANullLiteralAgainstAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN null IN n.tags", {{"null"}});
}

TEST_F(ListPropertyInTest, answersNullForAnAbsentPropertyAgainstAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN n.age IN n.tags", {{"null"}});
}

// The OR-fold of no elements is false whatever the left operand is, so an empty stored
// list answers false even against a value that is itself null
TEST_F(ListPropertyInTest, answersFalseForAnAbsentPropertyAgainstAnEmptyStoredList) {
    applyWrite("CREATE (d:Tagged {name: 'd', tags: []})");

    expectRows("MATCH (n:Tagged {name: 'd'}) RETURN n.age IN n.tags", {{"false"}});
}

TEST_F(ListPropertyInTest, findsAnElementOfAListStoredOnAnEdge) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "CREATE (a)-[e:TAGGED {tags: [1, 2]}]->(b)");

    expectRows("MATCH (:Person)-[e:TAGGED]->(:Person) RETURN 2 IN e.tags, 7 IN e.tags",
               {{"true", "false"}});
}

TEST_F(ListPropertyInTest, findsAnElementThroughAWithAlias) {
    expectRows("MATCH (n:Tagged) WITH n.tags AS t RETURN 2 IN t",
               {{"true"}, {"false"}, {"null"}});
}

TEST_F(ListPropertyInTest, findsAnUnwoundElementOfAStoredList) {
    expectRows("MATCH (n:Tagged) UNWIND n.tags AS t RETURN t IN [2, 'y']",
               {{"false"}, {"true"}, {"false"}, {"false"}, {"true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
