#include <gtest/gtest.h>

#include <memory>
#include <string_view>

#include "CypherAST.h"
#include "CypherParser.h"
#include "ProcedureManager.h"
#include "TuringException.h"
#include "expr/Expr.h"

using namespace db;

class ParseExpressionTest : public ::testing::Test {
protected:
    void SetUp() override {
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

    bool parses(std::string_view text) {
        size_t endOffset = 0;
        return parses(text, endOffset);
    }

    bool parses(std::string_view text, size_t& endOffset) {
        CypherAST ast(_procedures.get(), text);
        CypherParser parser(&ast);

        try {
            return parser.parseExpression(text, endOffset) != nullptr;
        } catch (const TuringException&) {
            return false;
        }
    }

    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(ParseExpressionTest, aWholeExpressionParses) {
    EXPECT_TRUE(parses("1 + 2"));
    EXPECT_TRUE(parses("datetime('2026-11-05T14:05:00Z') + duration({hours: 1})"));
    EXPECT_TRUE(parses("[1, 'a', null]"));
    EXPECT_TRUE(parses("{key: $p}"));
    EXPECT_TRUE(parses("1 // a comment to the end of the line"));
}

TEST_F(ParseExpressionTest, textPastTheExpressionIsASyntaxError) {
    EXPECT_FALSE(parses("1 2"));
    EXPECT_FALSE(parses("1) AS a, (2"));
    EXPECT_FALSE(parses("1 UNION RETURN 2"));
    EXPECT_FALSE(parses("1; 2"));
}

TEST_F(ParseExpressionTest, semicolonsAndCommentsMayFollowTheExpression) {
    size_t endOffset = 0;

    EXPECT_TRUE(parses("1 + 2 ; ; // the answer", endOffset));
    EXPECT_EQ(endOffset, 5u);

    EXPECT_TRUE(parses("[1, 2];", endOffset));
    EXPECT_EQ(endOffset, 6u);
}

TEST_F(ParseExpressionTest, noExpressionIsASyntaxError) {
    EXPECT_FALSE(parses(""));
    EXPECT_FALSE(parses("   "));
    EXPECT_FALSE(parses("RETURN 1"));
}
