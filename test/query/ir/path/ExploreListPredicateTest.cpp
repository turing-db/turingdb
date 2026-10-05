#include <gtest/gtest.h>

#include <algorithm>
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

}

// A WHERE all(...) or none(...) over the relationships or nodes of a variable-length path,
// moved by the fuse_explore_list_predicate pass into the exploration's hop predicate, so a
// failing hop is cut during the walk instead of every path being built and filtered after it
class ExploreListPredicateTest : public CallV3Test {
protected:
    std::string_view dumpOf(const StringRowSink& sink, std::string_view stage) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                return row.back();
            }
        }

        return {};
    }

    void explainAround(std::string_view query, std::string& before, std::string& after) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (around fuse_explore_list_predicate) ") + std::string(query), sink);

        before = dumpOf(sink, "before fuse_explore_list_predicate");
        after = dumpOf(sink, "after fuse_explore_list_predicate");
        ASSERT_FALSE(before.empty()) << query;
        ASSERT_FALSE(after.empty()) << query;
    }

    void expectFused(std::string_view query) {
        std::string before;
        std::string after;
        explainAround(query, before, after);

        EXPECT_TRUE(contains(before, "db.list_predicate")) << before;
        EXPECT_FALSE(contains(after, "db.list_predicate")) << after;
        EXPECT_TRUE(contains(after, "db.yield")) << after;
    }

    void expectNotFused(std::string_view query) {
        std::string before;
        std::string after;
        explainAround(query, before, after);

        EXPECT_TRUE(contains(after, "db.list_predicate")) << after;
        EXPECT_EQ(before, after);
    }

    // The filtered query must emit the rows of the reference whose last column is 'kept',
    // that column dropped. The reference reads the predicate through a CASE, which no pass
    // folds into the walk.
    void expectKeptRows(std::string_view filtered, std::string_view reference) {
        StringRowSink all;
        runQuery(reference, all);

        Rows expected;
        size_t droppedCount = 0;
        for (const StringRowSink::Row& row : all.getRows()) {
            if (row.back() == "kept") {
                expected.emplace_back(row.begin(), row.end() - 1);
            } else {
                droppedCount++;
            }
        }
        std::sort(expected.begin(), expected.end());

        StringRowSink sink;
        runQuery(filtered, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_FALSE(expected.empty()) << reference;
        EXPECT_NE(droppedCount, 0u) << reference;
        EXPECT_EQ(rows, expected) << filtered;
    }

    void expectCount(std::string_view query, std::string_view expected) {
        StringRowSink sink;
        runQuery(query, sink);

        ASSERT_EQ(sink.getRows().size(), 1u) << query;
        EXPECT_EQ(sink.getRows().front().front(), expected) << query;
    }
};

TEST_F(ExploreListPredicateTest, movesAPredicateOnEveryRelationshipIntoTheWalk) {
    std::string before;
    std::string after;
    explainAround("MATCH p = (a:Person)-[:KNOWS_WELL*1..4]->(a) "
                  "WHERE all(r IN relationships(p) WHERE r.duration > 10) RETURN count(a) AS c",
                  before,
                  after);

    EXPECT_TRUE(contains(before, "db.list_predicate")) << before;
    EXPECT_TRUE(contains(before, "db.filter")) << before;

    EXPECT_FALSE(contains(after, "db.list_predicate")) << after;
    EXPECT_FALSE(contains(after, "db.filter")) << after;
    EXPECT_FALSE(contains(after, "db.expand_path")) << after;
    EXPECT_TRUE(contains(after, "db.get_edge_properties(%arg1, \"duration\")")) << after;
}

TEST_F(ExploreListPredicateTest, countsTheCyclesWhoseEveryRelationshipHolds) {
    // Remy -> Adam -> Remy and Adam -> Remy -> Adam, both KNOWS_WELL edges of duration 20
    expectCount("MATCH p = (a:Person)-[:KNOWS_WELL*1..4]->(a) "
                "WHERE all(r IN relationships(p) WHERE r.duration > 10) RETURN count(a) AS c",
                "2");

    expectCount("MATCH p = (a:Person)-[:KNOWS_WELL*1..4]->(a) "
                "WHERE all(r IN relationships(p) WHERE r.duration > 20) RETURN count(a) AS c",
                "0");
}

TEST_F(ExploreListPredicateTest, keepsThePathsWhoseEveryRelationshipHolds) {
    const std::string_view filtered = "MATCH p = (a)-[*1..3]->(b) WHERE all(r IN relationships(p) WHERE r.duration >= 20) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[*1..3]->(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(r IN relationships(p) WHERE r.duration >= 20) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, keepsThePathsNoRelationshipOfWhichHolds) {
    const std::string_view filtered = "MATCH p = (a)-[*1..3]->(b) WHERE none(r IN relationships(p) WHERE r.duration > 100) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[*1..3]->(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN none(r IN relationships(p) WHERE r.duration > 100) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, keepsThePathsWhoseEveryNodeHolds) {
    const std::string_view filtered = "MATCH p = (a)-[*0..3]-(b) WHERE all(n IN nodes(p) WHERE n.age > 30) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[*0..3]-(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(n IN nodes(p) WHERE n.age > 30) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, keepsThePathsWhoseEveryNodeCarriesAFlag) {
    const std::string_view filtered = "MATCH p = (a)-[*1..3]-(b) WHERE all(n IN nodes(p) WHERE n.isFrench) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[*1..3]-(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(n IN nodes(p) WHERE n.isFrench) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, readsAFlagInsideTheHop) {
    const std::string_view filtered = "MATCH p = (a)((x)-[e]-(y) WHERE y.isFrench){1,3}(b) RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[*1..3]-(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(n IN tail(nodes(p)) WHERE n.isFrench) THEN 'kept' ELSE 'dropped' END";

    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, keepsTheZeroLengthPaths) {
    const std::string_view filtered = "MATCH p = (a:Person)-[*0..2]->(b) WHERE all(r IN relationships(p) WHERE r.duration = 20) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a:Person)-[*0..2]->(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(r IN relationships(p) WHERE r.duration = 20) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, readsTheSeedOfEachPath) {
    const std::string_view filtered = "MATCH p = (a)-[*1..3]->(b) WHERE all(r IN relationships(p) WHERE r.duration < a.age) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[*1..3]->(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(r IN relationships(p) WHERE r.duration < a.age) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, movesAPredicateBehindAnotherFilter) {
    const std::string_view filtered = "MATCH p = (a)-[*1..3]->(b) "
                                      "WHERE b.name <> 'Adam' AND all(r IN relationships(p) WHERE r.duration >= 20) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[*1..3]->(b) WHERE b.name <> 'Adam' RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(r IN relationships(p) WHERE r.duration >= 20) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, joinsTheHopPredicateAlreadyThere) {
    const std::string_view filtered = "MATCH p = (a)-[e WHERE e.duration >= 15]->{1,3}(b) WHERE all(n IN nodes(p) WHERE n.age > 30) "
                                      "RETURN a.name, b.name, relationships(p)";
    const std::string_view reference = "MATCH p = (a)-[e WHERE e.duration >= 15]->{1,3}(b) RETURN a.name, b.name, relationships(p), "
                                       "CASE WHEN all(n IN nodes(p) WHERE n.age > 30) THEN 'kept' ELSE 'dropped' END";

    expectFused(filtered);
    expectKeptRows(filtered, reference);
}

TEST_F(ExploreListPredicateTest, leavesAPredicateThatHoldsForSomeElements) {
    expectNotFused("MATCH p = (a)-[*1..3]->(b) WHERE any(r IN relationships(p) WHERE r.duration > 100) "
                   "RETURN a.name, b.name");
}

TEST_F(ExploreListPredicateTest, leavesAPredicateTheQueryAlsoReturns) {
    expectNotFused("MATCH p = (a)-[*1..3]->(b) WITH a, b, all(r IN relationships(p) WHERE r.duration > 10) AS ok "
                   "WHERE ok RETURN a.name, b.name, ok");
}
