#include <gtest/gtest.h>

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

// TUR-268: a walk whose end is pinned only through a node some hops past it ends on the nodes
// those hops reach the pinned one from, whichever end of the pattern the query starts from
class ExploreEndSetPastHopsCypherTest : public CallV3Test {
protected:
    Rows run(std::string_view query) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);
        return rows;
    }

    void explainAround(std::string_view query, std::string& before, std::string& after) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (around fuse_explore_end_set) ") + std::string(query), sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "before fuse_explore_end_set") {
                before = row.back();
            } else if (row.front() == "after fuse_explore_end_set") {
                after = row.back();
            }
        }

        ASSERT_FALSE(before.empty()) << query;
        ASSERT_FALSE(after.empty()) << query;
    }

    void expectEndSet(std::string_view query) {
        std::string before;
        std::string after;
        explainAround(query, before, after);

        EXPECT_FALSE(contains(before, "end_nodes")) << before;
        EXPECT_TRUE(contains(after, "end_nodes")) << after;
    }
};

TEST_F(ExploreEndSetPastHopsCypherTest, endsOnTheNodesOneHopFromThePinnedOne) {
    const std::string_view walkFirst = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i {name:'Cooking'}) RETURN a, b, i";
    const std::string_view pinnedFirst = "MATCH (i {name:'Cooking'})<-[:INTERESTED_IN]-(b)<-[:KNOWS_WELL*1..3]-(a {name:'Remy'}) RETURN a, b, i";

    expectEndSet(walkFirst);
    EXPECT_EQ(run(walkFirst), (Rows {{"0", "1", "5"}}));
    EXPECT_EQ(run(pinnedFirst), run(walkFirst));
}

TEST_F(ExploreEndSetPastHopsCypherTest, quantifiedPathPatternEndsOnTheSet) {
    const std::string_view query = "MATCH (a:Person {name:'Remy'})-[:KNOWS_WELL]->{1,3}(b:Person)-[:INTERESTED_IN]->(i:Interest {name:'Cooking'}) RETURN count(*)";

    expectEndSet(query);
    EXPECT_EQ(run(query), (Rows {{"1"}}));
}

TEST_F(ExploreEndSetPastHopsCypherTest, endsOnTheSetWhereverThePredicateIsWritten) {
    const Rows expected {{"0", "1", "5"}};

    const std::string_view whereClause = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i) WHERE i.name = 'Cooking' RETURN a, b, i";
    expectEndSet(whereClause);
    EXPECT_EQ(run(whereClause), expected);

    const std::string_view twoParts = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b), (b)-[:INTERESTED_IN]->(i {name:'Cooking'}) RETURN a, b, i";
    expectEndSet(twoParts);
    EXPECT_EQ(run(twoParts), expected);
}

TEST_F(ExploreEndSetPastHopsCypherTest, walksBackEveryHopToTheWalk) {
    const std::string_view query = "MATCH (a {name:'Ghosts'})-[:KNOWS_WELL*1..2]->(b)-[:KNOWS_WELL]->(c)-[:INTERESTED_IN]->(i {name:'Bio'}) RETURN a, b, c, i";

    expectEndSet(query);
    EXPECT_EQ(run(query), (Rows {{"6", "0", "1", "4"}}));
}

TEST_F(ExploreEndSetPastHopsCypherTest, pinsTheNodePastTheWalkByItsID) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[*1..2]->(b)<-[:INTERESTED_IN]-(p:Person) WHERE id(p) = 9 RETURN a, b, p";

    expectEndSet(query);
    EXPECT_EQ(run(query), (Rows {{"0", "2", "9"}}));
}

TEST_F(ExploreEndSetPastHopsCypherTest, emitsNothingWhenNoNodeHoldsTheValue) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i {name:'Nothing'}) RETURN a, b, i";

    EXPECT_EQ(run(query), Rows {});
}
