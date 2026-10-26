#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An item without an alias names its column after the text it is written with, line breaks
// and comments included. The expected names and rows are Neo4j 5.26's
class MultiLineItemNameTest : public CallV3Test {
protected:
    void expectColumn(std::string_view query, std::string_view name, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getNames(), std::vector<std::string> {std::string {name}}) << query;

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(MultiLineItemNameTest, namesAnIsTestAcrossALineBreak) {
    expectColumn("RETURN 1 IS\nNOT NULL", "1 IS\nNOT NULL", {{"true"}});
}

TEST_F(MultiLineItemNameTest, namesAnAdditionAcrossALineBreak) {
    expectColumn("RETURN 1 +\n2", "1 +\n2", {{"3"}});
}

TEST_F(MultiLineItemNameTest, namesACaseOverSeveralLines) {
    expectColumn("RETURN CASE\n  WHEN 1 = 1 THEN 'a'\n  ELSE 'b'\nEND", "CASE\n  WHEN 1 = 1 THEN 'a'\n  ELSE 'b'\nEND", {{"a"}});
}

TEST_F(MultiLineItemNameTest, namesAMapOverTwoLines) {
    expectColumn("RETURN {a: 1,\n b: 2}", "{a: 1,\n b: 2}", {{"{a: 1, b: 2}"}});
}

TEST_F(MultiLineItemNameTest, namesAListOverThreeLines) {
    expectColumn("RETURN [1,\n 2,\n 3]", "[1,\n 2,\n 3]", {{"1, 2, 3"}});
}

TEST_F(MultiLineItemNameTest, namesACallWithItsArgumentOnALineOfItsOwn) {
    expectColumn("RETURN toUpper(\n  'a'\n)", "toUpper(\n  'a'\n)", {{"A"}});
}

TEST_F(MultiLineItemNameTest, namesAnItemHoldingALineComment) {
    expectColumn("RETURN 1 + // one\n2", "1 + // one\n2", {{"3"}});
}

TEST_F(MultiLineItemNameTest, namesAnItemHoldingABlockComment) {
    expectColumn("RETURN 1 /* comment */ + 1", "1 /* comment */ + 1", {{"2"}});
}

TEST_F(MultiLineItemNameTest, namesAnItemHoldingABlockCommentOverTwoLines) {
    expectColumn("RETURN 1 /* a\nb */ + 1", "1 /* a\nb */ + 1", {{"2"}});
}

TEST_F(MultiLineItemNameTest, namesAStringHoldingALineBreak) {
    expectColumn("RETURN 'a\nb'", "'a\nb'", {{"a\nb"}});
}

TEST_F(MultiLineItemNameTest, namesAnItemAcrossACarriageReturnAndALineBreak) {
    expectColumn("RETURN 1 +\r\n2", "1 +\r\n2", {{"3"}});
}

TEST_F(MultiLineItemNameTest, namesAReturnItemAfterAWith) {
    expectColumn("WITH 1 AS a RETURN a +\n1", "a +\n1", {{"2"}});
}

TEST_F(MultiLineItemNameTest, ordersByAnItemWrittenOverTwoLines) {
    expectColumn("WITH 1 AS a RETURN a +\n1 ORDER BY a +\n1", "a +\n1", {{"2"}});
}

TEST_F(MultiLineItemNameTest, readsAMultiLineWithItemThroughItsAlias) {
    expectColumn("WITH 1 +\n2 AS x RETURN x", "x", {{"3"}});
}

// A union that ends a NEXT chain returns its columns by name, so the multi-line name is read
// back as well
TEST_F(MultiLineItemNameTest, namesTheColumnsOfAUnionEndingANextChain) {
    expectColumn("RETURN 1 AS a NEXT RETURN a +\n1 UNION ALL RETURN a +\n1", "a +\n1", {{"2"}, {"2"}});
}

TEST_F(MultiLineItemNameTest, underlinesAnOperationWrittenOverTwoLines) {
    runQueryExpectingError("RETURN 1 -\n'a'",
                           "       |        ⌄⌄⌄\n"
                           "     1 | RETURN 1 -\n"
                           "     2 | 'a'\n"
                           "       | ^^^\n");
}
