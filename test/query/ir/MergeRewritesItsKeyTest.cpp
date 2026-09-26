#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE applies row by row: a row looks its pattern up among what the ON CREATE and ON
// MATCH of the rows before it left
class MergeRewritesItsKeyTest : public WriteQueryTest {
};

TEST_F(MergeRewritesItsKeyTest, createsAgainWhatTheOnCreateOfAnEarlierRowRenamed) {
    expectWriteRows("UNWIND [1, 2] AS x MERGE (n:Person {name: 'Zoe'}) ON CREATE SET n.name = 'Yan' RETURN x, n.name",
                    {{"1", "Yan"}, {"2", "Yan"}});

    expectRows("MATCH (n:Person) WHERE n.name IN ['Zoe', 'Yan'] RETURN n.name, count(n)", {{"Yan", "2"}});
}

TEST_F(MergeRewritesItsKeyTest, createsWhatTheOnMatchOfAnEarlierRowRenamed) {
    applyWrite("UNWIND [1, 2] AS x MERGE (n:Person {name: 'Remy'}) ON MATCH SET n.name = 'X'");

    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'X'] RETURN n.name, count(n)", {{"Remy", "1"}, {"X", "1"}});
}

TEST_F(MergeRewritesItsKeyTest, createsAgainTheEdgeTheOnCreateOfAnEarlierRowRekeyed) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "UNWIND [1, 2] AS x "
               "MERGE (a)-[e:T {k: 1}]->(b) ON CREATE SET e.k = 2");

    expectRows("MATCH (:Person {name: 'Remy'})-[e:T]->(:Person {name: 'Adam'}) RETURN e.k, count(e)", {{"2", "2"}});
}

TEST_F(MergeRewritesItsKeyTest, matchesWhatTheOnCreateLeftUnderItsKey) {
    applyWrite("UNWIND [1, 2, 3] AS x MERGE (n:Tally {key: 'k'}) ON CREATE SET n.key = 'k', n.count = 1 ON MATCH SET n.count = n.count + 1");

    expectRows("MATCH (n:Tally) RETURN n.key, n.count", {{"k", "3"}});
}

// The clauses after a merge read its rows once every row has merged
TEST_F(MergeRewritesItsKeyTest, returnsWhatEveryRowLeftOnTheMergedNode) {
    expectWriteRows("UNWIND [1, 1, 1] AS x MERGE (n:K {k: x}) ON CREATE SET n.c = 1 ON MATCH SET n.c = n.c + 1, n.k = x RETURN n.c",
                    {{"3"}, {"3"}, {"3"}});
}

TEST_F(MergeRewritesItsKeyTest, filtersOnTheKeyEveryRowLeft) {
    expectWriteRows("UNWIND [1, 1] AS x MERGE (n:W {k: x}) ON MATCH SET n.k = 2 WITH n WHERE n.k = 1 RETURN count(*)",
                    {{"0"}});
}

// Every matched row writes the key back as it was, 70000 times over 7 nodes
TEST_F(MergeRewritesItsKeyTest, rewritesTheKeyOfFewNodesOverManyRows) {
    expectWriteRows("UNWIND range(1, 70000) AS i MERGE (n:T {k: i % 7}) ON MATCH SET n.k = i % 7 RETURN count(*)",
                    {{"70000"}});

    expectRows("MATCH (n:T) RETURN count(n), sum(n.k)", {{"7", "21"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
