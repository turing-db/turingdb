#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallBooleanPredicatesTest : public CallV3Test {
};

TEST_F(CallBooleanPredicatesTest, notInAYieldWhere) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label WHERE NOT label = 'Person' RETURN count(label)", sink);

    const std::vector<StringRowSink::Row> expected {{"8"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallBooleanPredicatesTest, orInAYieldWhere) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label WHERE label = 'Person' OR label = 'Interest' RETURN label", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Interest"}, {"Person"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallBooleanPredicatesTest, andInAYieldWhere) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label WHERE label = 'Person' AND NOT label = 'Interest' RETURN label", sink);

    const std::vector<StringRowSink::Row> expected {{"Person"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallBooleanPredicatesTest, xorInAYieldWhere) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label WHERE label = 'Person' XOR label = 'Interest' RETURN label", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Interest"}, {"Person"}};
    EXPECT_EQ(rows, expected);
}
