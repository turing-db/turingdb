#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// Cypher takes any whitespace, a line break or a comment, `//` or `/* */`, between two tokens,
// the words of IS NOT and the parts of a relationship arrow. The expected rows are Neo4j 5.26's
class TokenWhitespaceTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(TokenWhitespaceTest, testsIsNotAcrossTwoSpaces) {
    expectRows("RETURN 1 IS  NOT NULL", {{"true"}});
}

TEST_F(TokenWhitespaceTest, testsIsNotAcrossATab) {
    expectRows("RETURN 1 IS\tNOT NULL", {{"true"}});
}

TEST_F(TokenWhitespaceTest, testsIsNotAcrossALineBreak) {
    expectRows("RETURN 1 IS\nNOT NULL AS x", {{"true"}});
}

TEST_F(TokenWhitespaceTest, testsIsNotAcrossAComment) {
    expectRows("RETURN 1 IS // not what it seems\nNOT NULL AS x", {{"true"}});
}

TEST_F(TokenWhitespaceTest, testsANormalFormAcrossALineBreak) {
    expectRows("RETURN 'a' IS\nNOT NFD NORMALIZED AS x", {{"false"}});
}

TEST_F(TokenWhitespaceTest, switchesOnIsNotAcrossALineBreak) {
    expectRows("RETURN CASE null WHEN IS\nNOT NULL THEN 'set' ELSE 'unset' END AS x", {{"unset"}});
}

TEST_F(TokenWhitespaceTest, followsAnOutgoingArrowAcrossALineBreak) {
    expectRows("MATCH (a:Person {name: 'Remy'})-\n->(b) RETURN count(b)", {{"4"}});
}

TEST_F(TokenWhitespaceTest, followsAnIncomingArrowAcrossALineBreak) {
    expectRows("MATCH (a:Person {name: 'Remy'})<-\n-(b) RETURN count(b)", {{"2"}});
}

TEST_F(TokenWhitespaceTest, opensAnOutgoingRelationshipAcrossALineBreak) {
    expectRows("MATCH (a:Person {name: 'Remy'})-\n[r]->(b) RETURN count(b)", {{"4"}});
}

TEST_F(TokenWhitespaceTest, opensAnIncomingRelationshipAcrossALineBreak) {
    expectRows("MATCH (a:Person {name: 'Remy'})<-\n[r]-(b) RETURN count(b)", {{"2"}});
}

TEST_F(TokenWhitespaceTest, followsAnArrowAcrossAComment) {
    expectRows("MATCH (a:Person {name: 'Remy'})- // out\n->(b) RETURN count(b)", {{"4"}});
}

TEST_F(TokenWhitespaceTest, skipsABlockComment) {
    expectRows("RETURN 1 /* comment */ + 1 AS x", {{"2"}});
}

TEST_F(TokenWhitespaceTest, closesABlockCommentAtItsFirstEnd) {
    expectRows("RETURN 1 /* /* */ + 1 AS x", {{"2"}});
}

TEST_F(TokenWhitespaceTest, readsTheStarsAroundABlockComment) {
    expectRows("RETURN 4 /*2*/ * 2 AS x", {{"8"}});
}

TEST_F(TokenWhitespaceTest, dividesRightAfterABlockComment) {
    expectRows("RETURN 1 /* c *// 1 AS x", {{"1"}});
}

TEST_F(TokenWhitespaceTest, testsIsNotAcrossABlockComment) {
    expectRows("RETURN 1 IS /* c */ NOT NULL AS x", {{"true"}});
}

TEST_F(TokenWhitespaceTest, testsIsNotAcrossABlockCommentOverTwoLines) {
    expectRows("RETURN 1 IS /* a\nb */ NOT NULL AS x", {{"true"}});
}

TEST_F(TokenWhitespaceTest, followsAnArrowAcrossABlockComment) {
    expectRows("MATCH (a:Person {name: 'Remy'})-/* c */->(b) RETURN count(b)", {{"4"}});
}

TEST_F(TokenWhitespaceTest, followsAnArrowAcrossABlockCommentOverTwoLines) {
    expectRows("MATCH (a:Person {name: 'Remy'})<-/* a\nb */-(b) RETURN count(b)", {{"2"}});
}

TEST_F(TokenWhitespaceTest, keepsABlockCommentInAString) {
    expectRows("RETURN '/* not a comment */' AS x", {{"/* not a comment */"}});
}

TEST_F(TokenWhitespaceTest, keepsABlockCommentInABacktickedName) {
    StringRowSink sink;
    runQuery("RETURN 1 AS `/* x */`", sink);

    EXPECT_EQ(sink.getNames(), std::vector<std::string> {"/* x */"});
}
