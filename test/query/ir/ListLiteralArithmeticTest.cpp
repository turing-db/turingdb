#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class ListLiteralArithmeticTest : public CallV3Test {
};

TEST_F(ListLiteralArithmeticTest, addsTwoLiterals) {
    StringRowSink sink;
    runQuery("RETURN [1 + 1]", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListLiteralArithmeticTest, addsToAnUnwoundVariable) {
    StringRowSink sink;
    runQuery("UNWIND [1, 2] AS x RETURN [x, x + 1]", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"1, 2"}, {"2, 3"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(ListLiteralArithmeticTest, multipliesAProperty) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) RETURN [n.age * 2]", sink);

    const std::vector<StringRowSink::Row> expected {{"64"}};
    EXPECT_EQ(sink.getRows(), expected);
}
