#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// One MATCH chain of many hops gives the edge pair proofs a check per hop against every hop
// before it, n(n-1)/2 pairs in all; proving them has to stay well inside a query's time
class ProveDistinctEdgesLongChainTest : public CallV3Test {
protected:
    static constexpr size_t HOP_COUNT = 200;

    static void chainQuery(std::string& query) {
        query = "MATCH (n0)";
        for (size_t hop = 1; hop <= HOP_COUNT; hop++) {
            query += "-[e" + std::to_string(hop) + "]->(n" + std::to_string(hop) + ")";
        }
        query += " RETURN count(*)";
    }

    std::chrono::seconds _deadline {10};
};

// simpledb holds no trail of 200 edges, so the count is 0 once the pairs are settled
TEST_F(ProveDistinctEdgesLongChainTest, provesTheChainsPairsInTime) {
    std::string query;
    chainQuery(query);

    const auto start = std::chrono::steady_clock::now();

    StringRowSink sink;
    runQuery(query, sink);

    const auto elapsed = std::chrono::steady_clock::now() - start;
    const auto elapsedMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);

    ASSERT_EQ(sink.getRows().size(), 1u);
    EXPECT_EQ(sink.getRows().front().front(), "0");
    EXPECT_LT(elapsed, _deadline) << "elapsed: " << elapsedMilliseconds.count() << " ms";
}
