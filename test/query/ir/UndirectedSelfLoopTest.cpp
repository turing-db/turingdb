#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A relationship pattern without a direction binds a self-loop once: it leaves the node
// and arrives at it, and is still one edge
class UndirectedSelfLoopTest : public WriteQueryTest {
};

TEST_F(UndirectedSelfLoopTest, matchesACommittedSelfLoopOnce) {
    applyWrite("MATCH (a:Person {name: 'Luc'}) CREATE (a)-[:LOOP {x: 5}]->(a)");

    expectRows("MATCH ()-[r:LOOP]-() RETURN count(r)", {{"1"}});
    expectRows("MATCH (a:Person {name: 'Luc'})-[r:LOOP]-(b) RETURN b.name", {{"Luc"}});
    expectRows("MATCH (a)-[r]-(a) RETURN count(r)", {{"1"}});
}

TEST_F(UndirectedSelfLoopTest, setsASelfLoopOnce) {
    applyWrite("MATCH (a:Person {name: 'Luc'}) CREATE (a)-[:LOOP {x: 5}]->(a)");
    applyWrite("MATCH ()-[r:LOOP]-() SET r.x = r.x + 1");

    expectRows("MATCH ()-[r:LOOP]->() RETURN r.x", {{"6"}});
}

TEST_F(UndirectedSelfLoopTest, matchesASelfLoopTheQueryCreatedOnce) {
    expectWriteRows("CREATE (a:X)-[:L]->(a) WITH a MATCH (a)-[r:L]-() RETURN count(r)", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
