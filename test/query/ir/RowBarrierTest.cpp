#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// 70000 unwound rows span two chunks of 65536, so the MATCH or MERGE of the second chunk
// runs after the write of the first. Remy and Adam are the only nodes with an age, both 32.
class RowBarrierTest : public WriteQueryTest {
};

TEST_F(RowBarrierTest, matchesTheNodeInEveryChunkBeforeTheSetRenamesIt) {
    expectWriteRows("UNWIND range(1, 70000) AS i MATCH (n:Person {name: 'Remy'}) SET n.name = 'X' RETURN count(*)",
                    {{"70000"}});

    expectRows("MATCH (n:Person {name: 'X'}) RETURN count(n)", {{"1"}});
}

TEST_F(RowBarrierTest, filtersEveryChunkOnTheValueBeforeTheSet) {
    expectWriteRows("UNWIND range(1, 70000) AS i MATCH (n:Person) WHERE n.age = 32 SET n.age = 33 RETURN count(*)",
                    {{"140000"}});
}

TEST_F(RowBarrierTest, incrementsRowByRowPastTheBarrier) {
    applyWrite("UNWIND [1, 2, 3] AS i MATCH (n:Person) WHERE n.age = 32 SET n.age = n.age + 1");

    expectRows("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name, n.age", {{"Adam", "35"}, {"Remy", "35"}});
}

TEST_F(RowBarrierTest, mergesTheNodeInEveryChunkBeforeTheSetRenamesIt) {
    applyWrite("UNWIND range(1, 70000) AS i MERGE (n:Person {name: 'Remy'}) SET n.name = 'X'");

    expectRows("MATCH (n:Person {name: 'X'}) RETURN count(n)", {{"1"}});
    expectRows("MATCH (n:Person) RETURN count(n)", {{"8"}});
}

TEST_F(RowBarrierTest, matchesTheNodeInEveryChunkBeforeTheDeleteRemovesIt) {
    expectWriteRows("UNWIND range(1, 70000) AS i WITH i MATCH (n:Person {name: 'Remy'}) DETACH DELETE n RETURN count(*)",
                    {{"70000"}});

    expectRows("MATCH (n:Person) RETURN count(n)", {{"7"}});
}

TEST_F(RowBarrierTest, mergesInEveryChunkBeforeTheCreateAddsWhatItWouldFind) {
    expectWriteRows("UNWIND range(1, 70000) AS i MERGE (t:Tally {k: 1}) CREATE (:Tally {k: 1}) RETURN count(*)",
                    {{"70000"}});

    expectRows("MATCH (t:Tally) RETURN count(t)", {{"70001"}});
}

TEST_F(RowBarrierTest, createsFromWhatTheSetWroteForEveryRow) {
    applyWrite("UNWIND range(1, 65537) AS i MATCH (a:Person {name: 'Remy'}) SET a.x = coalesce(a.x, 0) + 1 CREATE (:Log {v: a.x})");

    expectRows("MATCH (l:Log) RETURN l.v, count(l)", {{"65537", "65537"}});
}

TEST_F(RowBarrierTest, setsFromWhatAnEarlierSetWroteForEveryRow) {
    applyWrite("UNWIND range(1, 70000) AS i MATCH (a:Person {name: 'Remy'}) "
               "SET a.x = coalesce(a.x, 0) + 1 "
               "SET a.y = coalesce(a.y, 0) + a.x");

    expectRows("MATCH (a:Person {name: 'Remy'}) RETURN a.x, a.y", {{"70000", "4900000000"}});
}

TEST_F(RowBarrierTest, mergesOnWhatTheSetWroteForEveryRow) {
    applyWrite("UNWIND range(1, 70000) AS i MATCH (a:Person {name: 'Remy'}) SET a.x = coalesce(a.x, 0) + 1 MERGE (:Log {v: a.x})");

    expectRows("MATCH (l:Log) RETURN l.v, count(l)", {{"70000", "1"}});
}

TEST_F(RowBarrierTest, filtersOnWhatTheSetWroteForEveryRow) {
    expectWriteRows("UNWIND range(1, 70000) AS i MATCH (a:Person {name: 'Remy'}) "
                    "SET a.x = coalesce(a.x, 0) + 1 "
                    "WITH a WHERE a.x = 70000 RETURN count(*)",
                    {{"70000"}});
}

TEST_F(RowBarrierTest, matchesEveryNodeACallCreatedForEveryRow) {
    expectWriteRows("UNWIND range(1, 70000) AS x CALL () { CREATE (:Y) } WITH x WHERE x = 1 MATCH (y:Y) RETURN count(y)",
                    {{"70000"}});
}

TEST_F(RowBarrierTest, matchesEveryNodeACreateWroteForEveryRow) {
    expectWriteRows("UNWIND range(1, 70000) AS x CREATE (:Y) WITH x WHERE x = 1 MATCH (y:Y) RETURN count(y)",
                    {{"70000"}});
}

TEST_F(RowBarrierTest, walksEveryEdgeACallCreatedForEveryRow) {
    expectWriteRows("UNWIND range(1, 70000) AS x MATCH (r:Person {name: 'Remy'}) "
                    "CALL (r) { CREATE (r)-[:T]->(:W) } "
                    "WITH r, x WHERE x = 1 MATCH (r)-[:T]->(w) RETURN count(w)",
                    {{"70000"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
