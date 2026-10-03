#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallComponentsTest : public CallV3Test {
};

// Two leading calls bind both ends of one pattern; Remy does point at Adam.
TEST_F(CallComponentsTest, twoLeadingCallsFeedOnePattern) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0]) YIELD id AS a CALL db.getNodes([1]) YIELD id AS b MATCH (a)-->(b) RETURN a, b", sink);

    const std::vector<StringRowSink::Row> expected {{"0", "1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallComponentsTest, yieldedEndpointsFormAPattern) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([0]) YIELD src, tgt MATCH (src)-->(tgt) RETURN src, tgt", sink);

    const std::vector<StringRowSink::Row> expected {{"0", "1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The yielded node drives the first component, and the second one is crossed with it.
TEST_F(CallComponentsTest, secondComponentAfterALeadingCall) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0]) YIELD id AS a MATCH (a)-->(m), (x {name: 'Adam'}) RETURN a, m, x", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "1", "1"}, {"0", "2", "1"}, {"0", "3", "1"}, {"0", "6", "1"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallComponentsTest, callAfterTwoMatchComponents) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) MATCH (m {name: 'Adam'}) CALL db.edgeTypes() YIELD edgeType RETURN n, m, edgeType", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "1", "INTERESTED_IN"}, {"0", "1", "KNOWS_WELL"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallComponentsTest, callAfterACommaPattern) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}), (m {name: 'Adam'}) CALL db.edgeTypes() YIELD edgeType RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}
