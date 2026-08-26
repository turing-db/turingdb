#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallOrderByTest : public CallV3Test {
};

TEST_F(CallOrderByTest, ordersByAYieldedNode) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([2, 0, 1]) YIELD id RETURN id ORDER BY id DESC", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}, {"1"}, {"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallOrderByTest, ordersByAYieldedEdge) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([2, 0, 1]) YIELD id RETURN id ORDER BY id DESC", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}, {"1"}, {"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Remy's four out-edges: one KNOWS_WELL, three INTERESTED_IN.
TEST_F(CallOrderByTest, ordersByAYieldedEdgeType) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([0, 1, 2, 3]) YIELD edgeTypeID RETURN edgeTypeID ORDER BY edgeTypeID", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}, {"1"}, {"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallOrderByTest, ordersByAYieldedUnsigned) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD inEdgeCount RETURN inEdgeCount ORDER BY inEdgeCount DESC", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}, {"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallOrderByTest, ordersByACountOverYields) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([0, 1, 2, 3]) YIELD src, tgt RETURN src, count(tgt) ORDER BY count(tgt) DESC", sink);

    const std::vector<StringRowSink::Row> expected {{"0", "4"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Every Person is crossed with the two edge types, so the two smallest names are Adam twice.
TEST_F(CallOrderByTest, ordersByAMatchedPropertyBesideACall) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) CALL db.edgeTypes() YIELD edgeType RETURN n.name ORDER BY n.name LIMIT 2", sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Adam"}};
    EXPECT_EQ(sink.getRows(), expected);
}
