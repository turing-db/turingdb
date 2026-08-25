#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallDistinctTest : public CallV3Test {
};

// Every node is crossed with every label, and DISTINCT folds the 162 rows back to the nine
// labels.
TEST_F(CallDistinctTest, distinctOnACrossedYield) {
    StringRowSink sink;
    runQuery("MATCH (n) CALL db.labels() YIELD label RETURN DISTINCT label", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Bioinformatics"},
                                                    {"Exotic"},
                                                    {"Founder"},
                                                    {"Interest"},
                                                    {"Person"},
                                                    {"Sales"},
                                                    {"SleepDisturber"},
                                                    {"SoftwareEngineering"},
                                                    {"Supernatural"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallDistinctTest, distinctOnAYieldedNode) {
    StringRowSink sink;
    runQuery("UNWIND [0, 1] AS x CALL db.getNodes([0, 1]) YIELD id RETURN DISTINCT id", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallDistinctTest, distinctOnACarriedAndAYieldedColumn) {
    StringRowSink sink;
    runQuery("UNWIND [1, 1, 2] AS x CALL db.getNodes([0]) YIELD id RETURN DISTINCT x, id", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"1", "0"}, {"2", "0"}};
    EXPECT_EQ(rows, expected);
}
