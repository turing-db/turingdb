#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Indexing and concatenating a list a node or an edge holds as a property: both read the
// nullable list column a property fetch produces, and answer null on a row holding no
// list.
class ListPropertyIndexConcatTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("CREATE (a:Tagged {name: 'a', tags: [1, 2, 3]})");
        applyWrite("CREATE (b:Tagged {name: 'b', tags: ['x', 'y']})");
        applyWrite("CREATE (c:Tagged {name: 'c'})");
    }
};

TEST_F(ListPropertyIndexConcatTest, readsAnElementOfAStoredList) {
    expectRows("MATCH (n:Tagged) RETURN n.name, n.tags[0]",
               {{"a", "1"}, {"b", "x"}, {"c", "null"}});
}

TEST_F(ListPropertyIndexConcatTest, countsAPositionFromTheEndOfAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN n.tags[-1]", {{"3"}});
}

TEST_F(ListPropertyIndexConcatTest, readsNullPastTheEndOfAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN n.tags[7]", {{"null"}});
}

TEST_F(ListPropertyIndexConcatTest, readsNullWhereThePositionIsNull) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN n.tags[null]", {{"null"}});
}

TEST_F(ListPropertyIndexConcatTest, filtersOnAnElementOfAStoredList) {
    expectRows("MATCH (n:Tagged) WHERE n.tags[0] = 1 RETURN n.name", {{"a"}});
}

TEST_F(ListPropertyIndexConcatTest, readsANestedStoredListAsAnElement) {
    applyWrite("CREATE (d:Tagged {name: 'd', tags: [[1, 2], [3]]})");

    expectRows("MATCH (n:Tagged {name: 'd'}) RETURN n.tags[0]", {{"[1, 2]"}});
}

TEST_F(ListPropertyIndexConcatTest, readsAnElementOfAListStoredOnAnEdge) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "CREATE (a)-[e:TAGGED {tags: [1, 2]}]->(b)");

    expectRows("MATCH (:Person)-[e:TAGGED]->(:Person) RETURN e.tags[1]", {{"2"}});
}

TEST_F(ListPropertyIndexConcatTest, concatenatesAListOntoAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN n.tags + [4]", {{"[1, 2, 3, 4]"}});
}

TEST_F(ListPropertyIndexConcatTest, concatenatesAStoredListOntoAList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN [0] + n.tags", {{"[0, 1, 2, 3]"}});
}

TEST_F(ListPropertyIndexConcatTest, concatenatesAStoredListOntoItself) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN n.tags + n.tags", {{"[1, 2, 3, 1, 2, 3]"}});
}

TEST_F(ListPropertyIndexConcatTest, concatenatesTwoStoredListsOfDifferentTypes) {
    expectRows("MATCH (a:Tagged {name: 'a'}), (b:Tagged {name: 'b'}) RETURN a.tags + b.tags",
               {{"[1, 2, 3, x, y]"}});
}

TEST_F(ListPropertyIndexConcatTest, concatenatesNullWhereTheNodeHoldsNoList) {
    expectRows("MATCH (n:Tagged) RETURN n.name, n.tags + [4]",
               {{"a", "[1, 2, 3, 4]"}, {"b", "[x, y, 4]"}, {"c", "null"}});
}

TEST_F(ListPropertyIndexConcatTest, readsAnElementOfAConcatenatedStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN (n.tags + [4])[3]", {{"4"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
