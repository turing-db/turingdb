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

// simpledb's KNOWS_WELL edges are Remy (0) -> Adam (1), Adam -> Remy and Ghosts -> Remy, so
// from Remy a walk of one or two of them ends at Adam, then back at Remy. Remy -> Adam is
// edge 0 and Adam -> Remy edge 4, after Remy's three INTERESTED_IN edges.
class GroupInLiteralTest : public CallV3Test {
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

TEST_F(GroupInLiteralTest, aListLiteralHoldsTheListOfAGroup) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "UNWIND [y] AS w RETURN w",
                     {{"1"},
                      {"1, 0"}});
}

TEST_F(GroupInLiteralTest, aListLiteralHoldsTheListsOfASeveralHopBody) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,1} (c) "
                     "UNWIND [x, y, z] AS w RETURN w",
                     {{"0"},
                      {"1"},
                      {"0"}});
}

TEST_F(GroupInLiteralTest, aListLiteralHoldsTheRelationshipsOfAWalk) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(c) "
                     "UNWIND [e] AS w RETURN w",
                     {{"0"},
                      {"0, 4"}});
}

TEST_F(GroupInLiteralTest, aMapLiteralHoldsAGroup) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "WITH {k: y} AS m, c RETURN c.name",
                     {{"Adam"},
                      {"Remy"}});
}
