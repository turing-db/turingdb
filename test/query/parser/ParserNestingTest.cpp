#include <gtest/gtest.h>

#include <pthread.h>
#include <stddef.h>
#include <exception>
#include <string>
#include <string_view>

#include "CypherAST.h"
#include "CypherParser.h"

using namespace db;

namespace {

struct Nesting {
    std::string_view _prefix;
    std::string_view _open;
    std::string_view _core;
    std::string_view _close;
    size_t _openingOffset {0};
    size_t _openingLength {1};
};

constexpr Nesting PARENTHESES {"RETURN ", "(", "1", ")"};
constexpr Nesting LISTS {"RETURN ", "[", "1", "]"};
constexpr Nesting MAPS {"RETURN ", "{a: ", "1", "}"};
constexpr Nesting CASES {"RETURN ", "CASE WHEN true THEN ", "1", " END", 0, 4};
constexpr Nesting CALL_SUBQUERIES {"", "CALL { ", "RETURN 1", " }", 5};
constexpr Nesting LABELS {"RETURN n:", "(", "A", ")"};
constexpr Nesting EXISTS_SUBQUERIES {"RETURN ", "EXISTS { RETURN ", "true", " }", 7};

}

// The parser recurses once per level of nesting, so every level a query may reach must fit
// the 512 KB a macOS thread starts with
class ParserNestingTest : public ::testing::Test {
protected:
    static constexpr size_t STACK_BYTES = 512 * 1024;
    static constexpr size_t MAX_DEPTH = CypherParser::MAX_NESTING_DEPTH;

    static void buildQuery(const Nesting& nesting, size_t levels, std::string& query) {
        query = nesting._prefix;
        for (size_t level = 0; level < levels; level++) {
            query += nesting._open;
        }

        query += nesting._core;

        for (size_t level = 0; level < levels; level++) {
            query += nesting._close;
        }
    }

    static void parseOnSmallStack(const std::string& query, std::string& error) {
        struct ParseRun {
            const std::string* _query {nullptr};
            std::string* _error {nullptr};
        };

        ParseRun run {&query, &error};

        const auto parseOnThread = [](void* argument) -> void* {
            ParseRun* const run = static_cast<ParseRun*>(argument);

            CypherAST ast(nullptr, *run->_query);
            CypherParser parser(&ast);

            try {
                parser.parse(*run->_query);
            } catch (const std::exception& exception) {
                *run->_error = exception.what();
            }

            return nullptr;
        };

        pthread_attr_t attributes;
        ASSERT_EQ(pthread_attr_init(&attributes), 0);
        ASSERT_EQ(pthread_attr_setstacksize(&attributes, STACK_BYTES), 0);

        pthread_t thread;
        ASSERT_EQ(pthread_create(&thread, &attributes, parseOnThread, &run), 0);
        ASSERT_EQ(pthread_join(thread, nullptr), 0);
        pthread_attr_destroy(&attributes);
    }

    // At the deepest level allowed, a query parses, or fails only the expression depth check
    // that follows the parse
    static void expectNoNestingError(const Nesting& nesting) {
        std::string query;
        buildQuery(nesting, MAX_DEPTH, query);

        std::string error;
        parseOnSmallStack(query, error);

        EXPECT_EQ(error.find("Query nested deeper than"), std::string::npos) << error;
        if (!error.empty()) {
            EXPECT_NE(error.find("Expression nested deeper than"), std::string::npos) << error;
        }
    }

    static void expectNestingErrorAtTheLevelPast(const Nesting& nesting) {
        std::string query;
        buildQuery(nesting, MAX_DEPTH + 1, query);

        std::string error;
        parseOnSmallStack(query, error);

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
