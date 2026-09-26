#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A variable a MERGE pattern names twice is one node: the match holds both places at it, and
// a write writes it once
class MergeRepeatedNodeTest : public WriteQueryTest {
};

TEST_F(MergeRepeatedNodeTest, writesASelfLoop) {
    applyWrite("MERGE (a:X {k: 1})-[:R]->(a)");

    expectRows("MATCH (a:X)-[:R]->(a) RETURN a.k", {{"1"}});
    expectRows("MATCH (n:X) RETURN count(n)", {{"1"}});
}

TEST_F(MergeRepeatedNodeTest, findsTheSelfLoopItWrote) {
    applyWrite("MERGE (a:X {k: 1})-[:R]->(a)");
    applyWrite("MERGE (a:X {k: 1})-[:R]->(a)");

    expectRows("MATCH (n:X) RETURN count(n)", {{"1"}});
    expectRows("MATCH ()-[r:R]->() RETURN count(r)", {{"1"}});
}

TEST_F(MergeRepeatedNodeTest, findsWhatAnEarlierRowWrote) {
    expectWriteRows("UNWIND [1, 2] AS i MERGE (a:X {k: 1})-[:R]->(a) RETURN i, a.k", {{"1", "1"}, {"2", "1"}});

    expectRows("MATCH (n:X) RETURN count(n)", {{"1"}});
    expectRows("MATCH ()-[r:R]->() RETURN count(r)", {{"1"}});
}

TEST_F(MergeRepeatedNodeTest, writesACycle) {
    applyWrite("MERGE (a:X {k: 1})-[:R1]->(b:X {k: 2})-[:R2]->(a)");
    applyWrite("MERGE (a:X {k: 1})-[:R1]->(b:X {k: 2})-[:R2]->(a)");

    expectRows("MATCH (n:X) RETURN count(n)", {{"2"}});
    expectRows("MATCH (a:X {k: 1})-[:R1]->(b:X {k: 2})-[:R2]->(c:X {k: 1}) RETURN count(*)", {{"1"}});
}

TEST_F(MergeRepeatedNodeTest, writesThePatternWhenTheNodeHasNoLoop) {
    applyWrite("CREATE (a:X {k: 1})-[:R]->(:X {k: 1})");
    applyWrite("MERGE (a:X {k: 1})-[:R]->(a)");

    expectRows("MATCH (n:X) RETURN count(n)", {{"3"}});
    expectRows("MATCH (a:X)-[:R]->(a) RETURN count(a)", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
