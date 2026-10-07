#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// Cypher orders a NaN after every number and before null, so ORDER BY puts it last, min
// skips it and max returns it, whatever row it arrives on
class NaNOrderingTest : public CallV3Test {
};

TEST_F(NaNOrderingTest, ordersNaNAfterEveryNumber) {
    StringRowSink sink;
    runQuery("UNWIND [4.0, -1.0, 1.0, 9.0, 0.25] AS x RETURN sqrt(x) AS r ORDER BY r", sink);

    const Rows expected {{"0.5"}, {"1"}, {"2"}, {"3"}, {"NaN"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(NaNOrderingTest, ordersNaNFirstDescending) {
    StringRowSink sink;
    runQuery("UNWIND [4.0, -1.0, 1.0, 9.0, 0.25] AS x RETURN sqrt(x) AS r ORDER BY r DESC", sink);

    const Rows expected {{"NaN"}, {"3"}, {"2"}, {"1"}, {"0.5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(NaNOrderingTest, ordersNaNBeforeNull) {
    StringRowSink sink;
    runQuery("UNWIND [2.0, null, -1.0, 1.0] AS x RETURN sqrt(x) AS r ORDER BY r", sink);

    const Rows expected {{"1"}, {"1.4142135623730951"}, {"NaN"}, {"null"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(NaNOrderingTest, keepsTheSmallestRowsPastNaN) {
    StringRowSink sink;
    runQuery("UNWIND [4.0, -1.0, 1.0, 9.0, 0.25] AS x RETURN sqrt(x) AS r ORDER BY r LIMIT 2", sink);

    const Rows expected {{"0.5"}, {"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(NaNOrderingTest, minSkipsNaNAndMaxReturnsIt) {
    const Rows expected {{"1", "NaN"}};

    StringRowSink nanFirst;
    runQuery("UNWIND [-1.0, 1.0] AS x RETURN min(sqrt(x)), max(sqrt(x))", nanFirst);
    EXPECT_EQ(nanFirst.getRows(), expected);

    StringRowSink nanLast;
    runQuery("UNWIND [1.0, -1.0] AS x RETURN min(sqrt(x)), max(sqrt(x))", nanLast);
    EXPECT_EQ(nanLast.getRows(), expected);
}

TEST_F(NaNOrderingTest, groupedMinSkipsNaNAndMaxReturnsIt) {
    StringRowSink sink;
    runQuery("UNWIND [-1.0, 1.0, 4.0, -4.0, 9.0] AS x RETURN abs(x) > 2 AS k, min(sqrt(x)), max(sqrt(x))", sink);

    Rows rows;
    sink.sortedRows(rows);

    const Rows expected {{"false", "1", "NaN"}, {"true", "2", "NaN"}};
    EXPECT_EQ(rows, expected);
}
