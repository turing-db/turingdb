#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The items of one SET clause apply row by row: a row runs every item before the next row
// starts, so an item reads what another item wrote for an earlier row. Remy is 32 in
// simpledb, and his KNOWS_WELL edge to Adam lasts 20.
class SetItemsRowByRowTest : public WriteQueryTest {
};

TEST_F(SetItemsRowByRowTest, readsWhatALaterItemWroteForAnEarlierRow) {
    applyWrite("UNWIND [1, 2, 3] AS i MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "SET b.age = a.age, a.age = b.age + 1");

    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Adam' RETURN n.name, n.age",
               {{"Remy", "35"}, {"Adam", "34"}});
}

TEST_F(SetItemsRowByRowTest, copiesWhatALaterItemWroteForAnEarlierRow) {
    applyWrite("UNWIND [1, 2] AS i MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "SET b += a, a.age = a.age + 1");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"33"}, {"34"}});
}

TEST_F(SetItemsRowByRowTest, readsWhatAnEarlierItemWroteForThisRow) {
    applyWrite("UNWIND [1, 2, 3] AS i MATCH (a:Person {name: 'Remy'}) CREATE (b:X {i: i}) "
               "SET a.age = a.age + 1, b.age = a.age");

    expectRows("MATCH (b:X) RETURN b.i, b.age", {{"1", "33"}, {"2", "34"}, {"3", "35"}});
}

TEST_F(SetItemsRowByRowTest, runsTheItemsInOrderOnOneRow) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n.name = 'Xy', n.age = size(n.name) RETURN n.age",
                    {{"2"}});
}

TEST_F(SetItemsRowByRowTest, keepsTheRowsInOrder) {
    Rows rows;
    writeRows("UNWIND [1, 2, 3] AS i MATCH (a:Person {name: 'Remy'}) SET a.age = i, a.previous = a.age RETURN i, a.age",
              rows);

    const Rows expected {{"1", "3"}, {"2", "3"}, {"3", "3"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(SetItemsRowByRowTest, appliesRowByRowOverChunks) {
    applyWrite("UNWIND range(1, 70000) AS i MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "SET b.age = a.age, a.age = b.age + 1");

    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Adam' RETURN n.name, n.age",
               {{"Remy", "70032"}, {"Adam", "70031"}});
}

TEST_F(SetItemsRowByRowTest, writesDistinctEntitiesTogether) {
    applyWrite("UNWIND range(1, 70000) AS i CREATE (:X {i: i})");
    applyWrite("MATCH (n:X) SET n.j = n.i, n.i = n.j + 1");

    expectRows("MATCH (n:X) WHERE n.i = n.j + 1 RETURN count(n)", {{"70000"}});
}

TEST_F(SetItemsRowByRowTest, readsAnEdgeALaterItemWrote) {
    applyWrite("UNWIND [1, 2] AS i MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
               "SET b.duration = e.duration, e.duration = e.duration + 1");

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) RETURN b.duration, e.duration",
               {{"21", "22"}});
}

TEST_F(SetItemsRowByRowTest, mergeActionsApplyRowByRow) {
    applyWrite("UNWIND [1, 2] AS i MERGE (n:Person {name: 'Remy'}) ON MATCH SET n.previous = n.age, n.age = n.previous + 1");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.previous, n.age", {{"33", "34"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
