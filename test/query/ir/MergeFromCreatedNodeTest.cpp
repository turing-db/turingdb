#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class MergeFromCreatedNodeTest : public WriteQueryTest {
};

TEST_F(MergeFromCreatedNodeTest, createsThePathOffANodeACreateWrote) {
    expectWriteRows("CREATE (a:W {k: 1}) MERGE (a)-[:T]->(b:W {k: 2}) RETURN a.k, b.k", {{"1", "2"}});

    expectRows("MATCH (a:W)-[:T]->(b:W) RETURN a.k, b.k", {{"1", "2"}});
}

TEST_F(MergeFromCreatedNodeTest, mergesAnEdgeToANodeACreateWrote) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}) CREATE (b:W {k: 3}) MERGE (a)-[:T]->(b) RETURN b.k", {{"3"}});

    expectRows("MATCH (:Person {name: 'Remy'})-[:T]->(b) RETURN b.k", {{"3"}});
}

TEST_F(MergeFromCreatedNodeTest, createsOnePathPerCreatedNode) {
    expectWriteRows("UNWIND [1, 2] AS x CREATE (a:W {k: x}) MERGE (a)-[:T]->(b:W {k: 10}) RETURN a.k, b.k",
                    {{"1", "10"}, {"2", "10"}});

    expectRows("MATCH (a:W)-[:T]->(b:W) RETURN a.k, b.k", {{"1", "10"}, {"2", "10"}});
}

TEST_F(MergeFromCreatedNodeTest, matchesThePathAnEarlierRowMergedOffTheCreatedNode) {
    expectWriteRows("CREATE (a:W {k: 1}) WITH a UNWIND [1, 2] AS x MERGE (a)-[:T]->(b:W {k: 2}) RETURN x, b.k",
                    {{"1", "2"}, {"2", "2"}});

    expectRows("MATCH (a:W)-[:T]->(b:W) RETURN a.k, b.k", {{"1", "2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
