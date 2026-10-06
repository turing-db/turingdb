#include <gtest/gtest.h>

#include <pthread.h>
#include <stddef.h>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// Each query builds a use-def chain of CHAIN_LENGTH ops, one per list element or clause. A walk
// recursing once per op needs megabytes of stack for it, and the query runs on a thread given
// STACK_BYTES, so such a walk fails here whatever the compiler's frame sizes.
class DeepChainQueryTest : public CallV3Test {
protected:
    static constexpr size_t CHAIN_LENGTH = 10000;
    static constexpr size_t STACK_BYTES = 512 * 1024;

    static std::string repeat(const std::string& clause) {
        std::string text;
        for (size_t index = 0; index < CHAIN_LENGTH; index++) {
            text += clause;
        }

        return text;
    }

    static std::string namesListedAmongAbsentOnes() {
        std::string list = "['Remy', 'Adam'";
        for (size_t index = 0; index < CHAIN_LENGTH; index++) {
            list += ", 'absent" + std::to_string(index) + "'";
        }

        return list + "]";
    }

    void runOnSmallStack(const std::string& query, StringRowSink& sink) {
        struct QueryRun {
            DeepChainQueryTest* _test {nullptr};
            const std::string* _query {nullptr};
            StringRowSink* _sink {nullptr};
        };

        QueryRun run {this, &query, &sink};

        const auto runQueryOnThread = [](void* argument) -> void* {
            QueryRun* const run = static_cast<QueryRun*>(argument);
            run->_test->runQuery(*run->_query, *run->_sink);

            return nullptr;
        };

        pthread_attr_t attributes;
        ASSERT_EQ(pthread_attr_init(&attributes), 0);
        ASSERT_EQ(pthread_attr_setstacksize(&attributes, STACK_BYTES), 0);

        pthread_t thread;
        ASSERT_EQ(pthread_create(&thread, &attributes, runQueryOnThread, &run), 0);
        ASSERT_EQ(pthread_join(thread, nullptr), 0);
        pthread_attr_destroy(&attributes);
    }
};

TEST_F(DeepChainQueryTest, hopsFromNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n)-->(m) WHERE n.name = x RETURN count(m)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, limitsNodesMatchedByALongDisjunction) {
    const std::string query = "UNWIND " + namesListedAmongAbsentOnes() + " AS x MATCH (n) WHERE n.name = x WITH n SKIP 1 LIMIT 1 RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, limitsNodesBelowALongChainOfFilters) {
    const std::string query = "MATCH (n) WITH n" + repeat(" WITH n WHERE n.name <> 'x'") + " WITH n LIMIT 3 RETURN count(n)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, returnsALongChainComputedOverACount) {
    const std::string query = "MATCH (n) WITH count(n) AS c" + repeat(" WITH c + 1 AS c") + " RETURN c";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{std::to_string(18 + CHAIN_LENGTH)}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, filtersOnALongChainComputedOverAProperty) {
    const std::string query = "MATCH (n) WITH n, n.age AS v" + repeat(" WITH n, v + 1 AS v") + " WITH n, v WHERE v > 0 RETURN v";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::string age = std::to_string(32 + CHAIN_LENGTH);
    const std::vector<StringRowSink::Row> expected {{age}, {age}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsAtASeedCarriedThroughALongChainOfFilters) {
    const std::string query = "MATCH (a)-[e]->(b) UNWIND [0, 2, 7] AS y WITH a, b, y" + repeat(" WITH a, b, y WHERE y >= 0") + " WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsBelowAFilterOnALongChainOverTheSeed) {
    const std::string query = "MATCH (a)-[e]->(b) UNWIND [0, 2, 7] AS y WITH a, b, y, y AS z" + repeat(" WITH a, b, y, z + 0 AS z") + " WITH a, b, y WHERE z >= 0 WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsAtIDsComputedThroughALongChain) {
    const std::string query = "MATCH (a)-[e]->(b) UNWIND [0, 2, 7] AS y WITH a, b, y" + repeat(" WITH a, b, y + 0 AS y") + " WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(DeepChainQueryTest, rerootsAPatternFilteredOnALongChain) {
    const std::string query = "MATCH (a)-[e]->(b) WITH a, b, a.age AS v" + repeat(" WITH a, b, v + 0 AS v") + " WITH a, b WHERE v > 0 UNWIND [0, 2, 7] AS y WITH a, b, y WHERE b = y RETURN count(a)";

    StringRowSink sink;
    runOnSmallStack(query, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}
