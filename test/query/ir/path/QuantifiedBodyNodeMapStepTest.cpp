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

// simpledb's KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy; Remy and
// Adam are both 32 and Ghosts has no age. The KNOWS chains Ann -> Bob -> Cy and Dee -> Eve -> Fay
// differ in whether the first node's age is the second edge's since.
class QuantifiedBodyNodeMapStepTest : public CallV3Test {
protected:
    void initialize() override {
        CallV3Test::initialize();

        runWrite("CREATE (:Person {name: 'Ann', age: 2})-[:KNOWS {since: 1}]->"
                 "(:Person {name: 'Bob', age: 1})-[:KNOWS {since: 2}]->"
                 "(:Person {name: 'Cy'})");
        runWrite("CREATE (:Person {name: 'Dee', age: 5})-[:KNOWS {since: 1}]->"
                 "(:Person {name: 'Eve'})-[:KNOWS {since: 1}]->"
                 "(:Person {name: 'Fay'})");
    }

    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(QuantifiedBodyNodeMapStepTest, aNodeMapReadsARelationshipALaterHopBinds) {
    expectSortedRows("MATCH (n)((a {age: f.since})-[e:KNOWS]->(b)-[f:KNOWS]->(c)){1,1}(m) RETURN n.name, m.name",
                     {{"Ann", "Cy"}});
}

TEST_F(QuantifiedBodyNodeMapStepTest, aNodeMapReadsANodeAnEarlierHopBindsOnAReversedWalk) {
    expectSortedRows("MATCH (m:Person {name: 'Remy'}) "
                     "MATCH (n)((a)-[:KNOWS_WELL]->(b)-[:KNOWS_WELL]->(c {age: a.age})){1,1}(m) RETURN n.name",
                     {{"Remy"}});
}
