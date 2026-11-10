#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An all() or none() over a walk, reading a constant a WITH binds after the MATCH.
// simpledb has 25 walks (p:Person)-[*1..2]->(q). Five of its edges last 20, Ghosts -> Remy
// 200, Luc -> Computers 15 and Martina -> Cooking 10; Remy and Adam, both 32, are the only
// people with an age.
class ExploreListPredicateLaterConstantTest : public CallV3Test {
protected:
    void expectCount(std::string_view query, std::string_view expected) {
        StringRowSink sink;
        runQuery(query, sink);

        ASSERT_EQ(sink.getRows().size(), 1u) << query;
        EXPECT_EQ(sink.getRows().front().front(), expected) << query;
    }

    void expectFused(std::string_view query) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (around fuse_explore_list_predicate) ") + std::string(query), sink);

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

TEST_F(ExploreListPredicateLaterConstantTest, allOverTheRelationshipsReadsTheConstant) {
    const std::string_view query = "MATCH (p:Person)-[e*1..2]->(q) WITH e, 1 AS t WHERE all(r IN e WHERE t = 1) RETURN count(*)";

    expectCount(query, "25");
    expectFused(query);

    expectCount("MATCH (p:Person)-[e*1..2]->(q) WITH e, 1 AS t WHERE all(r IN e WHERE t = 2) RETURN count(*)", "0");
}

TEST_F(ExploreListPredicateLaterConstantTest, noneOverTheRelationshipsReadsTheConstant) {
    const std::string_view query = "MATCH (p:Person)-[e*1..2]->(q) WITH e, 1 AS t WHERE none(r IN e WHERE t = 2) RETURN count(*)";

    expectCount(query, "25");
    expectFused(query);

    expectCount("MATCH (p:Person)-[e*1..2]->(q) WITH e, 1 AS t WHERE none(r IN e WHERE t = 1) RETURN count(*)", "0");
}

TEST_F(ExploreListPredicateLaterConstantTest, comparesEachRelationshipToTheConstant) {
    // Remy -> Adam, Remy -> Ghosts, Remy -> Eighties, Adam -> Remy, Luc -> Animals, and
    // Remy -> Adam -> Remy, Adam -> Remy -> Adam, Adam -> Remy -> Ghosts, Adam -> Remy -> Eighties
    expectCount("MATCH (p:Person)-[e*1..2]->(q) WITH e, 20 AS t WHERE all(r IN e WHERE r.duration = t) RETURN count(*)", "9");

    // Luc -> Computers and Martina -> Cooking: every other walk has an edge lasting 20 or
    // one with no duration
    expectCount("MATCH (p:Person)-[e*1..2]->(q) WITH e, 20 AS t WHERE none(r IN e WHERE r.duration = t) RETURN count(*)", "2");
}

TEST_F(ExploreListPredicateLaterConstantTest, comparesEachRelationshipToAComputedConstant) {
    expectCount("MATCH (p:Person)-[e*1..2]->(q) WITH e, 10 + 10 AS t WHERE all(r IN e WHERE r.duration = t) RETURN count(*)", "9");
}

TEST_F(ExploreListPredicateLaterConstantTest, comparesEachRelationshipToAConstantList) {
    // The 9 walks of edges lasting 20, and Martina -> Cooking
    expectCount("MATCH (p:Person)-[e*1..2]->(q) WITH e, [10, 20] AS ts WHERE all(r IN e WHERE r.duration IN ts) RETURN count(*)", "10");
    expectCount("MATCH (p:Person)-[e*1..2]->(q) WITH e, [x IN [10, 20] | x] AS ts WHERE all(r IN e WHERE r.duration IN ts) RETURN count(*)",
                "10");
}

TEST_F(ExploreListPredicateLaterConstantTest, joinsTheTestOfAWalkWithAHopPredicate) {
    expectCount("MATCH (p:Person)-[e*1..2 WHERE e.duration = 20]->(q) WITH e, 20 AS t WHERE all(r IN e WHERE r.duration = t) RETURN count(*)",
                "9");
}

TEST_F(ExploreListPredicateLaterConstantTest, allOverTheNodesReadsTheConstant) {
    const std::string_view query = "MATCH path = (p:Person)-[*1..2]->(q) WITH nodes(path) AS ns, 1 AS t WHERE all(n IN ns WHERE t = 1) RETURN count(*)";

    expectCount(query, "25");
    expectFused(query);

    // Remy -> Adam, Adam -> Remy, Remy -> Adam -> Remy and Adam -> Remy -> Adam
    expectCount("MATCH path = (p:Person)-[*1..2]->(q) WITH nodes(path) AS ns, 32 AS t WHERE all(n IN ns WHERE n.age = t) RETURN count(*)",
                "4");
}

TEST_F(ExploreListPredicateLaterConstantTest, allOverAGroupVariableReadsTheConstant) {
    expectCount("MATCH (p:Person)((a)-[r]->(b)){1,2}(q) WITH r, 1 AS t WHERE all(x IN r WHERE t = 1) RETURN count(*)", "25");
    expectCount("MATCH (p:Person)((a)-[r]->(b)){1,2}(q) WITH r, 20 AS t WHERE all(x IN r WHERE x.duration = t) RETURN count(*)", "9");
    expectCount("MATCH (p:Person)((a)-[r]->(b)){1,2}(q) WITH b, 32 AS t WHERE all(x IN b WHERE x.age = t) RETURN count(*)", "4");
}
