#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// simpledb's KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy, so the
// two-hop walks are Remy -> Adam -> Remy, Adam -> Remy -> Adam and Ghosts -> Remy -> Adam
class QuantifiedGroupKeyTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(QuantifiedGroupKeyTest, groupsOnAGroupOfASeveralHopBody) {
    expectSortedRows("MATCH (n:Person) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,1} (c) "
                     "UNWIND [1, 2] AS i WITH y, count(*) AS rows RETURN [v IN y | v.name], rows",
                     {{"Adam", "2"}, {"Remy", "2"}});
}

TEST_F(QuantifiedGroupKeyTest, dedupsAGroupOfASeveralHopBody) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,1} (c) "
                     "WITH DISTINCT y RETURN [v IN y | v.name]",
                     {{"Adam"}, {"Remy"}});
}

TEST_F(QuantifiedGroupKeyTest, groupsOnTheRelationshipsOfAOneHopWalk) {
    expectSortedRows("MATCH (n:Person)-[e:KNOWS_WELL*1..1]->(c) UNWIND [1, 2] AS i "
                     "WITH e, count(*) AS rows RETURN size(e), rows",
                     {{"1", "2"}, {"1", "2"}});
}
