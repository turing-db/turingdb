#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// simpledb's KNOWS_WELL edges are Remy (0) -> Adam (1), edge 0, and Adam -> Remy, edge 4.
// Ghosts -> Remy leaves no Person. A list of nodes orders by its node IDs.
class MatchOrderByGroupTest : public CallV3Test {
protected:
    void expectRows(const std::string& query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(MatchOrderByGroupTest, ordersByAGroup) {
    expectRows("MATCH (n:Person) ((x)-[:KNOWS_WELL]->(y)){1,1} (c) ORDER BY y "
               "RETURN [v IN x | v.name], [v IN y | v.name]",
               {{"Adam", "Remy"},
                {"Remy", "Adam"}});
}

TEST_F(MatchOrderByGroupTest, ordersByAnotherGroupOfTheSameWalk) {
    expectRows("MATCH (n:Person) ((x)-[:KNOWS_WELL]->(y)){1,1} (c) ORDER BY x "
               "RETURN [v IN x | v.name], [v IN y | v.name]",
               {{"Remy", "Adam"},
                {"Adam", "Remy"}});
}

TEST_F(MatchOrderByGroupTest, ordersByAGroupOfASeveralHopBody) {
    expectRows("MATCH (n:Person) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,1} (c) ORDER BY y DESC "
               "RETURN n.name",
               {{"Remy"},
                {"Adam"}});
}

TEST_F(MatchOrderByGroupTest, ordersByTheRelationshipsOfAWalk) {
    expectRows("MATCH (n:Person)-[e:KNOWS_WELL*1..1]->(c) ORDER BY e DESC RETURN n.name",
               {{"Adam"},
                {"Remy"}});
}
