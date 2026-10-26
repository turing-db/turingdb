#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "CypherAST.h"
#include "CypherParser.h"
#include "ParserException.h"

using namespace db;

class ParseErrorMessageTest : public ::testing::Test {
protected:
    void expectMessage(std::string_view query, std::string_view expected) {
        const std::string text {query};
        CypherAST ast(nullptr, text);
        CypherParser parser(&ast);

        try {
            parser.parse(text);
        } catch (const ParserException& error) {
            const std::string_view message = error.what();
            const size_t lastLine = message.rfind("-------* ");
            ASSERT_NE(lastLine, std::string_view::npos) << message;

            EXPECT_EQ(message.substr(lastLine + 9), expected) << query;
            return;
        }

        ADD_FAILURE() << "parsed: " << query;
    }

    void expectReport(std::string_view query, std::string_view expected) {
        const std::string text {query};
        CypherAST ast(nullptr, text);
        CypherParser parser(&ast);

        try {
            parser.parse(text);
        } catch (const ParserException& error) {
            EXPECT_EQ(std::string_view {error.what()}, expected) << query;
            return;
        }

        ADD_FAILURE() << "parsed: " << query;
    }
};

// GRAPH and INDEX can also be the name of a path, as in `CREATE graph = (a)-->(b)`
TEST_F(ParseErrorMessageTest, createGraphWithoutAName) {
    expectMessage("CREATE GRAPH", "syntax error, unexpected PROG_END, expecting ASSIGN or a name");
}

TEST_F(ParseErrorMessageTest, createGraphNamedByANumber) {
    expectMessage("CREATE GRAPH 1", "syntax error, unexpected DIGIT, expecting ASSIGN or a name");
}

TEST_F(ParseErrorMessageTest, createIndexWithoutAName) {
    expectMessage("CREATE INDEX", "syntax error, unexpected PROG_END, expecting ASSIGN or a name");
}

TEST_F(ParseErrorMessageTest, loadGraphWithoutAName) {
    expectMessage("LOAD GRAPH", "syntax error, unexpected PROG_END, expecting a name");
}

TEST_F(ParseErrorMessageTest, dropIndexWithoutAName) {
    expectMessage("DROP INDEX", "syntax error, unexpected PROG_END, expecting a name");
}

TEST_F(ParseErrorMessageTest, installWithoutAName) {
    expectMessage("INSTALL", "syntax error, unexpected PROG_END, expecting a name");
}

TEST_F(ParseErrorMessageTest, explainAnUnknownOptionBeforeAClause) {
    expectMessage("EXPLAIN (bogus MATCH (n) RETURN n",
                  "Unknown EXPLAIN option 'bogus'. Expected one of ast, vdg, codegen, passes, db, nl, all, "
                  "or before, after, around followed by a pass name");
}

TEST_F(ParseErrorMessageTest, constraintRequiringANotNullProperty) {
    expectMessage("CREATE CONSTRAINT c FOR (n:Person) REQUIRE n.name IS NOT NULL", "Not implemented: CREATE CONSTRAINT");
}

TEST_F(ParseErrorMessageTest, createVectorWithoutIndex) {
    expectMessage("CREATE VECTOR", "syntax error, unexpected PROG_END, expecting ASSIGN or INDEX");
}

TEST_F(ParseErrorMessageTest, isNotFollowedByAValue) {
    expectMessage("RETURN 1 IS NOT 5", "The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");
}

TEST_F(ParseErrorMessageTest, isNotFollowedByAFormWithoutNormalized) {
    expectMessage("RETURN 'a' IS NOT NFC 5", "The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");
}

TEST_F(ParseErrorMessageTest, isNotFollowedByAParenthesizedNull) {
    expectMessage("RETURN 1 IS NOT (NULL)", "The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");
}

TEST_F(ParseErrorMessageTest, pointsPastAnArrowAcrossALineBreak) {
    expectReport("MATCH (a)-\n->(b)\nRETURN 1 +",
                 "-------* Cypher parser\n"
                 "     2 | ->(b)\n"
                 "     3 | RETURN 1 +\n"
                 "       |          ^\n"
                 "-------* syntax error, unexpected PROG_END");
}

TEST_F(ParseErrorMessageTest, pointsAtAnUnterminatedBlockComment) {
    expectReport("RETURN 1 /* unterminated",
                 "-------* Cypher parser\n"
                 "     1 | RETURN 1 /* unterminated\n"
                 "       |          ^^\n"
                 "-------* A comment that opens with /* must close with */");
}

TEST_F(ParseErrorMessageTest, endsABlockCommentAtItsFirstClose) {
    expectReport("RETURN 1 /* /* */ */ + 1",
                 "-------* Cypher parser\n"
                 "     1 | RETURN 1 /* /* */ */ + 1\n"
                 "       |                    ^\n"
                 "-------* syntax error, unexpected DIV");
}

TEST_F(ParseErrorMessageTest, pointsPastABlockCommentOverTwoLines) {
    expectReport("RETURN 1 /* a\nb */ +",
                 "-------* Cypher parser\n"
                 "     1 | RETURN 1 /* a\n"
                 "     2 | b */ +\n"
                 "       |      ^\n"
                 "-------* syntax error, unexpected PROG_END");
}
