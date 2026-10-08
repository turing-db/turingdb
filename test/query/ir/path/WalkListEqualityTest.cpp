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

// simpledb's KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy
class WalkListEqualityTest : public CallV3Test {
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

TEST_F(WalkListEqualityTest, comparesTheRelationshipsOfTwoWalks) {
    expectSortedRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL*1..1]->(b), "
                     "(c:Person {name: 'Remy'})-[f:KNOWS_WELL*1..1]->(d) RETURN e = f",
                     {{"true"}});
}

TEST_F(WalkListEqualityTest, comparesTwoGroupsOfOneWalk) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN size(x), x = y",
                     {{"1", "false"}, {"2", "false"}});
}
