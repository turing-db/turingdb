#include <gtest/gtest.h>

#include <stddef.h>
#include <string>

#include "CypherParser.h"
#include "ParseOnStack.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr Nesting UNION_MERGE {"RETURN ",
                               "COUNT { RETURN 1 AS a NEXT RETURN 1 AS b UNION MERGE (n) "
                               "ON MATCH SET n.x = 1 ON CREATE SET n.x = NOT n.y IS NULL AND ",
                               "true RETURN 1",
                               " }"};

constexpr Nesting WHEN_MERGE {"RETURN ",
                              "COUNT { RETURN 1 AS a NEXT WHEN true THEN MERGE (n) "
                              "ON MATCH SET n.x = 1 ON CREATE SET n.x = NOT n.y IS NULL AND ",
                              "true RETURN 1",
                              " }"};

}

class SubqueryNestingStackTest : public ::testing::Test {
protected:
    static constexpr size_t MACOS_THREAD_STACK = 512 * 1024;

    static void expectParseWithinTheStackBudget(const Nesting& nesting) {
        std::string query;
        buildNestedQuery(nesting, CypherParser::MAX_NESTING_DEPTH - 1, query);

        std::string error;
        parseOnStack(query, MACOS_THREAD_STACK, error);

        EXPECT_TRUE(error.empty() || error.ends_with("-------* Query nested too deeply")) << error;
    }
};

TEST_F(SubqueryNestingStackTest, staysWithinAMacOSThreadThroughUnionAndMerge) {
    expectParseWithinTheStackBudget(UNION_MERGE);
}

TEST_F(SubqueryNestingStackTest, staysWithinAMacOSThreadThroughWhenAndMerge) {
    expectParseWithinTheStackBudget(WHEN_MERGE);
}
