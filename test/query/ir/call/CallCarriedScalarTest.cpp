#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallCarriedScalarTest : public CallV3Test {
};

// The yielded count rides through the traversal the yielded node drives, and the MATCH's
// WHERE reads it: only Remy has more than one incoming edge.
TEST_F(CallCarriedScalarTest, matchWhereReadsAYieldedScalarThroughTheDrive) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD id AS a, inEdgeCount MATCH (a)-->(m) WHERE inEdgeCount > 1 RETURN a, m", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "1"}, {"0", "2"}, {"0", "3"}, {"0", "6"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallCarriedScalarTest, setReadsAYieldedScalar) {
    runWrite("CALL db.getNodes([0, 1]) YIELD id AS a, inEdgeCount MATCH (a) SET a.degree = inEdgeCount");

    StringRowSink sink;
    runQuery("MATCH (n) WHERE n.degree > 0 RETURN n.name, n.degree", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Adam", "1"}, {"Remy", "2"}};
    EXPECT_EQ(rows, expected);
}

// A constant argument may be an expression, as long as it varies with no row.
TEST_F(CallCarriedScalarTest, constantExpressionArgument) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) CALL gnn.neighbourhoodSample(n, 1 + 1, 42) YIELD tgt RETURN count(tgt)", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallCarriedScalarTest, twoCallsReadTheSameMatchedVariable) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) CALL gnn.neighbourhoodSample(n, 1, 42) YIELD tgt "
             "CALL gnn.neighbourhoodSample(n, 1, 7) YIELD tgt AS t2 RETURN count(*)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallCarriedScalarTest, crossesAnEmptyMatchWithACall) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n.age > 100 CALL db.labels() YIELD label RETURN label", sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(CallCarriedScalarTest, crossesAnEmptyLeadingCallWithAMatch) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label WHERE label = 'Nope' MATCH (n) RETURN n", sink);

    EXPECT_TRUE(sink.getRows().empty());
}
