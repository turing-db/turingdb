#include <gtest/gtest.h>

#include <stddef.h>
#include <string>

#include "CypherParser.h"
#include "ParseOnStack.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr Nesting PARENTHESES {"RETURN ", "(", "1", ")"};
constexpr Nesting LISTS {"RETURN ", "[", "1", "]"};
constexpr Nesting MAPS {"RETURN ", "{a: ", "1", "}"};
constexpr Nesting CASES {"RETURN ", "CASE WHEN true THEN ", "1", " END", 0, 4};
constexpr Nesting CALL_SUBQUERIES {"", "CALL { ", "RETURN 1", " }", 5};
constexpr Nesting LABELS {"RETURN n:", "(", "A", ")"};
constexpr Nesting EXISTS_SUBQUERIES {"RETURN ", "EXISTS { RETURN ", "true", " }", 7};
constexpr Nesting COMPREHENSIONS {"RETURN ", "[x IN ", "xs", " | x]"};
constexpr Nesting REDUCES {"RETURN ", "reduce(s = 0, x IN ", "xs", " | s)", 6};
constexpr Nesting LABEL_BARS {"RETURN ", "[x IN xs WHERE x:A | B + ", "1", "]"};

}

// The parser recurses once per level of nesting, so every level a query may reach must fit
// the 512 KB a macOS thread starts with
class ParserNestingTest : public ::testing::Test {
protected:
    static constexpr size_t STACK_BYTES = 512 * 1024;
    static constexpr size_t MAX_DEPTH = CypherParser::MAX_NESTING_DEPTH;

    // At the deepest level allowed, a query parses, or fails only the expression depth check
    // that follows the parse
    static void expectNoNestingError(const Nesting& nesting) {
        std::string query;
        buildNestedQuery(nesting, MAX_DEPTH, query);

        std::string error;
        parseOnStack(query, STACK_BYTES, error);

        EXPECT_EQ(error.find("Query nested deeper than"), std::string::npos) << error;
        if (!error.empty()) {
            EXPECT_NE(error.find("Expression nested deeper than"), std::string::npos) << error;
        }
    }

    static void expectNestingErrorAtTheLevelPast(const Nesting& nesting) {
        std::string query;
        buildNestedQuery(nesting, MAX_DEPTH + 1, query);

        std::string error;
        parseOnStack(query, STACK_BYTES, error);

        const size_t opening = nesting._prefix.size() + MAX_DEPTH * nesting._open.size() + nesting._openingOffset;

        std::string expected = "-------* Cypher parser\n     1 | " + query + "\n       | ";
        expected += std::string(opening, ' ') + std::string(nesting._openingLength, '^') + "\n";
        expected += "-------* Query nested deeper than 256 levels";

        EXPECT_EQ(error, expected);
    }
};

TEST_F(ParserNestingTest, parsesParenthesesAtTheMaximumDepth) {
    expectNoNestingError(PARENTHESES);
}

TEST_F(ParserNestingTest, rejectsParenthesesOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(PARENTHESES);
}

TEST_F(ParserNestingTest, parsesListsAtTheMaximumDepth) {
    expectNoNestingError(LISTS);
}

TEST_F(ParserNestingTest, rejectsListsOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(LISTS);
}

TEST_F(ParserNestingTest, parsesMapsAtTheMaximumDepth) {
    expectNoNestingError(MAPS);
}

TEST_F(ParserNestingTest, rejectsMapsOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(MAPS);
}

TEST_F(ParserNestingTest, parsesCasesAtTheMaximumDepth) {
    expectNoNestingError(CASES);
}

TEST_F(ParserNestingTest, rejectsCasesOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(CASES);
}

TEST_F(ParserNestingTest, parsesCallSubqueriesAtTheMaximumDepth) {
    expectNoNestingError(CALL_SUBQUERIES);
}

TEST_F(ParserNestingTest, rejectsCallSubqueriesOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(CALL_SUBQUERIES);
}

TEST_F(ParserNestingTest, parsesLabelExpressionsAtTheMaximumDepth) {
    expectNoNestingError(LABELS);
}

TEST_F(ParserNestingTest, rejectsLabelExpressionsOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(LABELS);
}

TEST_F(ParserNestingTest, parsesExistsSubqueriesAtTheMaximumDepth) {
    expectNoNestingError(EXISTS_SUBQUERIES);
}

TEST_F(ParserNestingTest, rejectsExistsSubqueriesOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(EXISTS_SUBQUERIES);
}

TEST_F(ParserNestingTest, parsesComprehensionsAtTheMaximumDepth) {
    expectNoNestingError(COMPREHENSIONS);
}

TEST_F(ParserNestingTest, rejectsComprehensionsOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(COMPREHENSIONS);
}

TEST_F(ParserNestingTest, parsesReducesAtTheMaximumDepth) {
    expectNoNestingError(REDUCES);
}

TEST_F(ParserNestingTest, rejectsReducesOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(REDUCES);
}

TEST_F(ParserNestingTest, parsesLabelBarsAtTheMaximumDepth) {
    expectNoNestingError(LABEL_BARS);
}

TEST_F(ParserNestingTest, rejectsLabelBarsOneLevelDeeper) {
    expectNestingErrorAtTheLevelPast(LABEL_BARS);
}
