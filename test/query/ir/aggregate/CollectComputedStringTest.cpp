#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CollectComputedStringTest : public CallV3Test {
};

TEST_F(CollectComputedStringTest, collectsToStringResults) {
    StringRowSink sink;
    runQuery("UNWIND [1, 2] AS x RETURN collect(toString(x))", sink);

    const std::vector<StringRowSink::Row> expected {{"1, 2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CollectComputedStringTest, indexesTheCollectedList) {
    StringRowSink sink;
    runQuery("UNWIND [1, 2] AS x WITH collect(toString(x)) AS texts RETURN texts[0]", sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CollectComputedStringTest, sizesTheCollectedList) {
    StringRowSink sink;
    runQuery("UNWIND [1, 2] AS x RETURN size(collect(toString(x)))", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CollectComputedStringTest, collectsDistinctToStringResults) {
    StringRowSink sink;
    runQuery("UNWIND [1, 1, 2] AS x RETURN collect(DISTINCT toString(x))", sink);

    const std::vector<StringRowSink::Row> expected {{"1, 2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CollectComputedStringTest, unwindsTheCollectedList) {
    StringRowSink sink;
    runQuery("UNWIND [1, 2] AS x WITH collect(toString(x)) AS texts UNWIND texts AS text RETURN text", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"1"}, {"2"}};
    EXPECT_EQ(rows, expected);
}
