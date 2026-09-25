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

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
