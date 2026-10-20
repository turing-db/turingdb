#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

}

// Remy and Adam are 32, so s.age - 40 is -8 for them and null for the six other people.
// Each query's other side has no rows, so the product is empty and counts 0.
class FetchNodesOverAnEmptyProductTest : public CallV3Test {
protected:
    void expectFetchedCount(std::string_view query, std::string_view expected) {
        StringRowSink plan;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), plan);
        ASSERT_EQ(plan.getRows().size(), 1u);

        const std::string& program = plan.getRows().front().back();
        EXPECT_TRUE(contains(program, "db.fetch_nodes")) << program;

        StringRowSink sink;
        runQuery(query, sink);

        ASSERT_EQ(sink.getRows().size(), 1u) << query;
        EXPECT_EQ(sink.getRows().front().front(), expected) << query;
    }
};

TEST_F(FetchNodesOverAnEmptyProductTest, reroots) {
    expectFetchedCount("MATCH (s:Person), (a)-->(b) WHERE a.name = 'Nobody' AND b = s.age - 40 RETURN count(*)", "0");
}

TEST_F(FetchNodesOverAnEmptyProductTest, rerootsAcrossTwoMatches) {
    expectFetchedCount("MATCH (s:Person) MATCH (a:Nobody)-->(b) WHERE b = s.age - 40 RETURN count(*)", "0");
}

TEST_F(FetchNodesOverAnEmptyProductTest, fusesTheFetch) {
    expectFetchedCount("MATCH (s:Person), (b:Nobody) WHERE b = s.age - 40 RETURN count(*)", "0");
}

TEST_F(FetchNodesOverAnEmptyProductTest, fetchesBesideAnEmptyUnwind) {
    expectFetchedCount("MATCH (s:Person) UNWIND [] AS x WITH s, x MATCH (b) WHERE b = s.age - 40 RETURN count(*)", "0");
}

TEST_F(FetchNodesOverAnEmptyProductTest, rerootsOverAProductWithRows) {
    expectFetchedCount("MATCH (s:Person), (a)-->(b) WHERE b = s.age - 40 RETURN count(*)", "0");
}
