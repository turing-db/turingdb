#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A SET applies row by row: a row reaching an entity an earlier row wrote reads what that
// row wrote. Remy is 32 in simpledb.
class SetRepeatedEntityTest : public WriteQueryTest {
};

TEST_F(SetRepeatedEntityTest, incrementsTheNodeOncePerRow) {
    applyWrite("UNWIND [1, 2, 3] AS x MATCH (n:Person {name: 'Remy'}) SET n.age = n.age + 1");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"35"}});
}

// Cyrus, Suhas and Doruk are interested in Gym
TEST_F(SetRepeatedEntityTest, countsTheRowsReachingTheSameNode) {
    applyWrite("MATCH (:Person)-[:INTERESTED_IN]->(i:Interest {name: 'Gym'}) SET i.visits = coalesce(i.visits, 0) + 1");

    expectRows("MATCH (i:Interest {name: 'Gym'}) RETURN i.visits", {{"3"}});
}

TEST_F(SetRepeatedEntityTest, mergesOnMatchOncePerRow) {
    applyWrite("UNWIND ['x', 'x', 'y', 'x'] AS k "
               "MERGE (n:Tally {key: k}) "
               "ON CREATE SET n.count = 1 "
               "ON MATCH SET n.count = n.count + 1");

    expectRows("MATCH (n:Tally) RETURN n.key, n.count", {{"x", "3"}, {"y", "1"}});
}

TEST_F(SetRepeatedEntityTest, readsWhatAnEarlierRowWroteThroughAnotherVariable) {
    applyWrite("UNWIND [1, 2] AS i "
               "MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Remy'}) "
               "SET a.age = b.age + 1");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"34"}});
}

// The KNOWS_WELL edge from Remy to Adam lasts 20
TEST_F(SetRepeatedEntityTest, incrementsTheEdgeOncePerRow) {
    applyWrite("UNWIND [1, 2, 3] AS x "
               "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "SET e.duration = e.duration + 1");

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.duration", {{"23"}});
}

TEST_F(SetRepeatedEntityTest, incrementsTheNodeTheQueryCreatedOncePerRow) {
    applyWrite("CREATE (c:Counter {n: 0}) WITH c UNWIND [1, 2, 3] AS i SET c.n = c.n + 1");

    expectRows("MATCH (c:Counter) RETURN c.n", {{"3"}});
}

// 70000 rows span two chunks, and every row of a chunk reaches the same node
TEST_F(SetRepeatedEntityTest, incrementsTheNodeOncePerRowOfManyChunks) {
    applyWrite("UNWIND range(1, 70000) AS x MATCH (n:Person {name: 'Remy'}) SET n.age = n.age + 1");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"70032"}});
}

TEST_F(SetRepeatedEntityTest, readsWhatEachOfManyRowsWroteThroughAnotherVariable) {
    applyWrite("UNWIND range(1, 70000) AS i "
               "MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Remy'}) "
               "SET a.age = b.age + 1");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"70032"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
