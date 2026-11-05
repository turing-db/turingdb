#include <gtest/gtest.h>

#include "CallV3Test.h"

using namespace turing::test;

class CallGraphSAGENullSeedTest : public CallV3Test {};

TEST_F(CallGraphSAGENullSeedTest, rejectsANullBesideANodeID) {
    runQueryExpectingError("CALL gnn.graphSAGE([null, 0], [0, 0, 0], 42) YIELD dst_nodes0 RETURN dst_nodes0",
                           "graphSAGE() seeds must be a list of nodes or ints");
}

TEST_F(CallGraphSAGENullSeedTest, rejectsAListOfNulls) {
    runQueryExpectingError("CALL gnn.graphSAGE([null, null], [2, 2, 2], 42) YIELD dst_nodes0 RETURN dst_nodes0",
                           "graphSAGE() seeds must be a list of nodes or ints");
}

TEST_F(CallGraphSAGENullSeedTest, rejectsTheNullNodeOfAnOptionalMatch) {
    runQueryExpectingError("OPTIONAL MATCH (n:Nope) "
                           "CALL gnn.graphSAGE([n], [2, 2, 2], 42) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "graphSAGE() seeds must be a list of nodes or ints");
}
