#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class ListPredicateOverGroupTest : public CallV3Test {
protected:
    void expectRows(const std::string& query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectSortedRows(const std::string& query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows = sink.getRows();
        std::sort(rows.begin(), rows.end());

        EXPECT_EQ(rows, expected) << query;
    }

    void expectFused(const std::string& query) {
        StringRowSink sink;
        runQuery("EXPLAIN (around fuse_explore_list_predicate) " + query, sink);

        std::string before;
        std::string after;
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "before fuse_explore_list_predicate") {
                before = row.back();
            } else if (row.front() == "after fuse_explore_list_predicate") {
                after = row.back();
            }
        }

        EXPECT_NE(before.find("db.list_predicate"), std::string::npos) << before;
        EXPECT_EQ(after.find("db.list_predicate"), std::string::npos) << after;
    }
};

TEST_F(ListPredicateOverGroupTest, keepsTheWalksWhoseEveryEndHolds) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE all(z IN b WHERE z.age > 3) RETURN count(*)", {{"6"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheWalksWhoseEverySourceHolds) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE all(z IN a WHERE z.age > 3) RETURN count(*)", {{"14"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheWalksSomeEndOfWhichHolds) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE any(z IN b WHERE z.name = 'Ghosts') RETURN count(*)", {{"4"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheWalksWhoseEveryGroupedRelationshipHolds) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE all(z IN r WHERE z.duration >= 20) RETURN count(*)", {{"14"}});
}

TEST_F(ListPredicateOverGroupTest, projectsAPropertyOfEachEnd) {
    expectSortedRows("MATCH (x {name: 'Ghosts'})((a)-[r]->(b)){2}(y) RETURN [z IN b | z.name]",
                     {{"Remy, Adam"}, {"Remy, Computers"}, {"Remy, Eighties"}, {"Remy, Ghosts"}});
}

TEST_F(ListPredicateOverGroupTest, projectsAPropertyOfEachSource) {
    expectSortedRows("MATCH (x {name: 'Ghosts'})((a)-[r]->(b)){2}(y) RETURN [z IN a | z.name]",
                     {{"Ghosts, Remy"}, {"Ghosts, Remy"}, {"Ghosts, Remy"}, {"Ghosts, Remy"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheEndsAWhereHolds) {
    expectRows("MATCH (x {name: 'Ghosts'})((a)-[r]->(b)){2}(y) RETURN size([z IN b WHERE z.age > 3]) AS s, count(*) ORDER BY s",
               {{"1", "3"}, {"2", "1"}});
}

TEST_F(ListPredicateOverGroupTest, readsTheGroupCarriedThroughAWith) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WITH b WHERE all(z IN b WHERE z.age > 3) RETURN count(*)", {{"6"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheWalksFromAPersonWhoseEveryEndHolds) {
    expectRows("MATCH (n:Person)((x)-[e]->(y)){1,3}(m) WHERE all(z IN y WHERE z.age > 3) RETURN count(m)", {{"4"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheCyclesWhoseEveryRelationshipHolds) {
    expectRows("MATCH (a:Person)-[t:KNOWS_WELL*1..4]->(a) WHERE all(r IN t WHERE r.duration > 10) RETURN count(a)", {{"2"}});
}

TEST_F(ListPredicateOverGroupTest, movesAPredicateOnEveryEndIntoTheWalk) {
    expectFused("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE all(z IN b WHERE z.age > 3) RETURN count(*)");
}

TEST_F(ListPredicateOverGroupTest, movesAPredicateOnEverySourceIntoTheWalk) {
    expectFused("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE none(z IN a WHERE z.name = 'Ghosts') RETURN count(*)");
}

TEST_F(ListPredicateOverGroupTest, keepsTheWalksNoSourceOfWhichHolds) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE none(z IN a WHERE z.name = 'Ghosts') RETURN count(*)", {{"24"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheWalksBackFromABoundEndWhoseEverySourceHolds) {
    expectRows("MATCH (s {name: 'Adam'}) MATCH (x)((a)-[r]->(b)){1,2}(s) WHERE all(z IN a WHERE z.age > 3) RETURN count(*)", {{"2"}});
}

TEST_F(ListPredicateOverGroupTest, keepsTheWalksBackFromABoundEndWhoseEveryEndHolds) {
    expectRows("MATCH (s {name: 'Adam'}) MATCH (x)((a)-[r]->(b)){1,2}(s) WHERE all(z IN b WHERE z.age > 3) RETURN count(*)", {{"3"}});
}
