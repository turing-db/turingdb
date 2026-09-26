#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WITH of aggregates alone makes one row, and a CREATE behind it runs once for that row,
// whichever aggregate the row is driven by
class CreateAfterGlobalAggregateTest : public WriteQueryTest {
};

TEST_F(CreateAfterGlobalAggregateTest, createsFromACollectAfterACount) {
    applyWrite("UNWIND [1, 2] AS i WITH count(i) AS c, collect(i) AS l CREATE (:T {l: l})");

    expectRows("MATCH (n:T) RETURN size(n.l)", {{"2"}});
}

TEST_F(CreateAfterGlobalAggregateTest, readsBothAggregates) {
    expectWriteRows("MATCH (p:Person) WITH count(p) AS c, collect(p.name) AS names CREATE (x:B {names: names, c: c}) "
                    "RETURN size(x.names), x.c",
                    {{"8", "8"}});
}

TEST_F(CreateAfterGlobalAggregateTest, setsTheCollectOnTheCreatedNode) {
    applyWrite("UNWIND [1, 2] AS i WITH count(i) AS c, collect(i) AS l CREATE (n:T) SET n.l = l");

    expectRows("MATCH (n:T) RETURN size(n.l)", {{"2"}});
}

TEST_F(CreateAfterGlobalAggregateTest, createsAnEdgeFromTheCollect) {
    applyWrite("UNWIND [1, 2] AS i WITH count(i) AS c, collect(i) AS l CREATE (:U)-[:R {l: l}]->(:U)");

    expectRows("MATCH ()-[r:R]->() RETURN size(r.l)", {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
