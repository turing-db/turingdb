#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "CypherParser.h"
#include "StringRowSink.h"

using namespace turing::test;

class DeepExpressionTest : public CallV3Test {
protected:
    static constexpr size_t maxDepth = db::CypherParser::MAX_EXPRESSION_DEPTH;

    void buildSum(size_t additions, std::string& query) {
        query = "RETURN 1";
        for (size_t addition = 0; addition < additions; addition++) {
            query += " + 1";
        }
    }

    void buildNesting(std::string_view open, std::string_view close, size_t levels, std::string& query) {
        query = "RETURN ";
        for (size_t level = 0; level < levels; level++) {
            query += open;
        }

        query += "1";

        for (size_t level = 0; level < levels; level++) {
            query += close;
        }
    }
};

TEST_F(DeepExpressionTest, sumsAChainAtTheMaximumDepth) {
    std::string query;
    buildSum(maxDepth - 1, query);

    StringRowSink sink;
    runQuery(query, sink);

    const std::vector<StringRowSink::Row> expected {{std::to_string(maxDepth)}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepExpressionTest, rejectsAChainOneLevelDeeper) {
    std::string query;
    buildSum(maxDepth, query);

    runQueryExpectingError(query, "nested deeper than");
}

TEST_F(DeepExpressionTest, rejectsAHundredThousandTerms) {
    std::string query;
    buildSum(100000, query);

    runQueryExpectingError(query, "nested deeper than");
}

TEST_F(DeepExpressionTest, rejectsAHundredThousandDisjuncts) {
    std::string query = "MATCH (n) WHERE n.age = 0";
    for (size_t age = 1; age < 100000; age++) {
        query += " OR n.age = " + std::to_string(age);
    }
    query += " RETURN n.name";

    runQueryExpectingError(query, "nested deeper than");
}

TEST_F(DeepExpressionTest, rejectsNestedFunctionCalls) {
    std::string query;
    buildNesting("toInteger(", ")", maxDepth, query);

    runQueryExpectingError(query, "nested deeper than");
}

TEST_F(DeepExpressionTest, rejectsNestedLists) {
    std::string query;
    buildNesting("[", "]", maxDepth, query);

    runQueryExpectingError(query, "nested deeper than");
}

TEST_F(DeepExpressionTest, rejectsNestedMaps) {
    std::string query;
    buildNesting("{a: ", "}", maxDepth, query);

    runQueryExpectingError(query, "nested deeper than");
}
