#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// simpledb holds nodes 0 to 17, so 18, 999 and -1 name none
class CallGraphSAGEUnknownSeedTest : public CallV3Test {};

TEST_F(CallGraphSAGEUnknownSeedTest, rejectsASeedPastTheLastNode) {
    runQueryExpectingError("CALL gnn.graphSAGE([999], [2, 2, 2], 42) YIELD dst_nodes0 "
                           "RETURN dst_nodes0, labels(dst_nodes0)",
                           "Invalid node ID: 999");
}

TEST_F(CallGraphSAGEUnknownSeedTest, rejectsTheFirstIDPastTheLastNode) {
    runQueryExpectingError("CALL gnn.graphSAGE([18], [2, 2, 2], 42) YIELD dst_nodes0 RETURN dst_nodes0",
                           "Invalid node ID: 18");
}

TEST_F(CallGraphSAGEUnknownSeedTest, rejectsANegativeSeed) {
    runQueryExpectingError("CALL gnn.graphSAGE([-1], [2, 2, 2], 42) YIELD dst_nodes0 RETURN dst_nodes0",
                           "Invalid node ID: -1");
}

TEST_F(CallGraphSAGEUnknownSeedTest, rejectsAnUnknownSeedBesideKnownSeeds) {
    runQueryExpectingError("CALL gnn.graphSAGE([0, 999, 8], [2, 2, 2], 42) YIELD * RETURN *",
                           "Invalid node ID: 999");
}

TEST_F(CallGraphSAGEUnknownSeedTest, rejectsAnUnknownSeedOfAnyRow) {
    runQueryExpectingError("UNWIND [0, 8, 999] AS id "
                           "CALL gnn.graphSAGE([id], [0, 0, 0], 42) YIELD dst_nodes0 "
                           "RETURN id, dst_nodes0",
                           "Invalid node ID: 999");
}

// Doruk is node 17
TEST_F(CallGraphSAGEUnknownSeedTest, rejectsADeletedNode) {
    runWrite("MATCH (n {name: 'Doruk'}) DETACH DELETE n");

    runQueryExpectingError("CALL gnn.graphSAGE([0, 17], [0, 0, 0], 42) YIELD dst_nodes0 RETURN dst_nodes0",
                           "Invalid node ID: 17");
}

TEST_F(CallGraphSAGEUnknownSeedTest, rejectsANodeTheQueryDeleted) {
    runWriteExpectingError("MATCH (n {name: 'Doruk'}) DETACH DELETE n WITH 1 AS one "
                           "CALL gnn.graphSAGE([0, 17], [0, 0, 0], 42) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "Invalid node ID: 17");
}

TEST_F(CallGraphSAGEUnknownSeedTest, aNodeTheQueryCreatedIsASeed) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Zoe'}) WITH id(n) AS id "
             "CALL gnn.graphSAGE([id], [0, 0, 0], 42) YIELD dst_nodes0 "
             "RETURN dst_nodes0.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Zoe"}};
    EXPECT_EQ(sink.getRows(), expected);
}
