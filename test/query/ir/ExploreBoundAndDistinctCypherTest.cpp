#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

Rows deduplicated(Rows rows) {
    std::sort(rows.begin(), rows.end());
    rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
    return rows;
}

}

// A bound end folded into the exploration by fuse_explore_end_nodes and a DISTINCT read out
// of it by fuse_explore_distinct_ends: the rows are read off the engine's own unconstrained
// walks rather than hand-listed
class ExploreBoundAndDistinctCypherTest : public CallV3Test {
protected:
    std::string_view dumpOf(const StringRowSink& sink, std::string_view stage) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                return row.back();
            }
        }

        return {};
    }

    // Whether the optimised program of the query carries a distinct exploration
    bool exploresDistinctly(std::string_view query) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (after fuse_explore_distinct_ends) ") + std::string(query), sink);

        const std::string_view after = dumpOf(sink, "after fuse_explore_distinct_ends");
        EXPECT_TRUE(contains(after, "db.explore_paths")) << after;

        return contains(after, " distinct");
    }

    // The DISTINCT query must emit the distinct rows of the same query without DISTINCT
    void expectDeduplicatedRows(std::string_view distinctQuery, std::string_view plainQuery) {
        StringRowSink plain;
        runQuery(plainQuery, plain);

        StringRowSink distinct;
        runQuery(distinctQuery, distinct);

        Rows rows;
        distinct.sortedRows(rows);

        const Rows expected = deduplicated(plain.getRows());
        EXPECT_FALSE(expected.empty()) << distinctQuery;
        EXPECT_EQ(rows, expected) << distinctQuery;
    }
};

TEST_F(ExploreBoundAndDistinctCypherTest, explainShowsTheBoundEnd) {
    StringRowSink sink;
    runQuery("EXPLAIN (around fuse_explore_end_nodes) MATCH (a:Person)-[e:KNOWS_WELL]->+(b), (a)-[:KNOWS_WELL]->(b) RETURN a.name, e, b.name", sink);

    const std::string_view before = dumpOf(sink, "before fuse_explore_end_nodes");
    EXPECT_TRUE(contains(before, "db.eq")) << before;
    EXPECT_TRUE(contains(before, "db.filter")) << before;
    EXPECT_FALSE(contains(before, "end_column")) << before;

    const std::string_view after = dumpOf(sink, "after fuse_explore_end_nodes");
    EXPECT_TRUE(contains(after, "end_column")) << after;
    EXPECT_FALSE(contains(after, "db.eq")) << after;
    EXPECT_FALSE(contains(after, "db.filter")) << after;
}

TEST_F(ExploreBoundAndDistinctCypherTest, boundEndKeepsThePathsLandingOnTheJoinedNode) {
    // The pairs a direct edge joins, then every path between them, both from the engine
    StringRowSink joined;
    runQuery("MATCH (a:Person)-->(b) RETURN a.name, b.name", joined);

    std::set<StringRowSink::Row> pairs(joined.getRows().begin(), joined.getRows().end());
    ASSERT_FALSE(pairs.empty());

    StringRowSink unconstrained;
    runQuery("MATCH (a:Person)-[e]->{1,3}(b) RETURN a.name, e, b.name", unconstrained);

    Rows expected;
    for (const StringRowSink::Row& row : unconstrained.getRows()) {
        if (pairs.contains(StringRowSink::Row {row[0], row[2]})) {
            expected.push_back(row);
        }
    }
    std::sort(expected.begin(), expected.end());

    StringRowSink sink;
    runQuery("MATCH (a:Person)-[e]->{1,3}(b), (a)-->(b) RETURN a.name, e, b.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_FALSE(expected.empty());
    EXPECT_EQ(rows, expected);
}

TEST_F(ExploreBoundAndDistinctCypherTest, explainShowsTheDistinctExploration) {
    EXPECT_TRUE(exploresDistinctly("MATCH (n)-[e]->*(m) RETURN DISTINCT n.name, m.name"));
    EXPECT_TRUE(exploresDistinctly("MATCH (n)-[e]->*(m) RETURN count(DISTINCT m.name)"));
    EXPECT_TRUE(exploresDistinctly("MATCH (n:Person)-[e]-{0,3}(m) WHERE m.age > 30 RETURN DISTINCT m.name"));
    EXPECT_TRUE(exploresDistinctly("MATCH (n)-[e]->*(m) RETURN n.name, count(DISTINCT m.name)"));

    // The path is read, the rows are counted, a two-hop minimum leaves walks with no trail,
    // and undirected the one-edge backtrack does the same past a minimum of zero
    EXPECT_FALSE(exploresDistinctly("MATCH (n)-[e]->*(m) RETURN DISTINCT n.name, e"));
    EXPECT_FALSE(exploresDistinctly("MATCH (n)-[e]->*(m) RETURN count(*)"));
    EXPECT_FALSE(exploresDistinctly("MATCH (n)-[e]->*(m) RETURN n.name, m.name"));
    EXPECT_FALSE(exploresDistinctly("MATCH (n)-[e]->{2,3}(m) RETURN DISTINCT n.name, m.name"));
    EXPECT_FALSE(exploresDistinctly("MATCH (n:Person)-[e]-{1,3}(m) RETURN DISTINCT m.name"));
}

TEST_F(ExploreBoundAndDistinctCypherTest, distinctExplorationEmitsTheDeduplicatedRows) {
    expectDeduplicatedRows("MATCH (n)-[e]->*(m) RETURN DISTINCT n.name, m.name",
                           "MATCH (n)-[e]->*(m) RETURN n.name, m.name");

    expectDeduplicatedRows("MATCH (n:Person)-[e]-{1,3}(m) RETURN DISTINCT m.name",
                           "MATCH (n:Person)-[e]-{1,3}(m) RETURN m.name");

    expectDeduplicatedRows("MATCH (n:Person)-[e]->*(m:Person) WHERE m.age > 30 RETURN DISTINCT n.name, m.name",
                           "MATCH (n:Person)-[e]->*(m:Person) WHERE m.age > 30 RETURN n.name, m.name");

    expectDeduplicatedRows("MATCH (n)<-[e]-+(m)-->(p) RETURN DISTINCT n.name, p.name",
                           "MATCH (n)<-[e]-+(m)-->(p) RETURN n.name, p.name");
}

TEST_F(ExploreBoundAndDistinctCypherTest, distinctAggregatesCountTheDeduplicatedEnds) {
    StringRowSink plain;
    runQuery("MATCH (n)-[e]->*(m) RETURN m.name", plain);
    const size_t distinctEnds = deduplicated(plain.getRows()).size();

    StringRowSink counted;
    runQuery("MATCH (n)-[e]->*(m) RETURN count(DISTINCT m.name)", counted);
    ASSERT_EQ(counted.getRows().size(), 1u);
    EXPECT_EQ(counted.getRows().front().front(), std::to_string(distinctEnds));

    StringRowSink plainPairs;
    runQuery("MATCH (n:Person)-[e]->*(m:Person) RETURN n.name, m.name", plainPairs);
    const size_t distinctPairs = deduplicated(plainPairs.getRows()).size();

    StringRowSink withDistinct;
    runQuery("MATCH (n:Person)-[e]->*(m:Person) WITH DISTINCT n, m RETURN count(*)", withDistinct);
    ASSERT_EQ(withDistinct.getRows().size(), 1u);
    EXPECT_EQ(withDistinct.getRows().front().front(), std::to_string(distinctPairs));
}
