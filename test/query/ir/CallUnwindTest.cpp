#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallUnwindTest : public CallV3Test {
};

namespace {

const std::vector<StringRowSink::Row> unwoundEdgeTypes {{"1", "INTERESTED_IN"},
                                                        {"1", "KNOWS_WELL"},
                                                        {"2", "INTERESTED_IN"},
                                                        {"2", "KNOWS_WELL"}};

}

TEST_F(CallUnwindTest, unwindBeforeACall) {
    StringRowSink sink;
    runQuery("UNWIND [1, 2] AS x CALL db.edgeTypes() YIELD edgeType RETURN x, edgeType", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, unwoundEdgeTypes);
}

TEST_F(CallUnwindTest, unwindAfterACall) {
    StringRowSink sink;
    runQuery("CALL db.edgeTypes() YIELD edgeType UNWIND [1, 2] AS x RETURN x, edgeType", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, unwoundEdgeTypes);
}

// The yielded column is no list, so each of its rows unwinds to the single value it holds.
TEST_F(CallUnwindTest, unwindsAYieldedValue) {
    StringRowSink sink;
    runQuery("CALL db.edgeTypes() YIELD edgeType UNWIND edgeType AS unwoundType RETURN unwoundType", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"INTERESTED_IN"}, {"KNOWS_WELL"}};
    EXPECT_EQ(rows, expected);
}

// One node per row of the unwound list crossed with the yields: two cells, two edge types.
TEST_F(CallUnwindTest, unwindThenCallThenCreate) {
    runWrite("UNWIND [1, 2] AS x CALL db.edgeTypes() YIELD edgeType CREATE (m:Combo)");

    StringRowSink sink;
    runQuery("MATCH (m:Combo) RETURN count(m)", sink);

    const std::vector<StringRowSink::Row> expected {{"4"}};
    EXPECT_EQ(sink.getRows(), expected);
}
