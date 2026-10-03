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

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

// A walk restricted to several relationship types, as -[:KNOWS_WELL|INTERESTED_IN*]-> spells
// it. simpledb carries exactly those two types, so a disjunction over both walks the same
// edges as an unrestricted walk, and either one alone walks strictly fewer.
class ExploreEdgeTypeDisjunctionTest : public CallV3Test {
protected:
    void rowsOf(std::string_view query, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink);
        sink.sortedRows(rows);
    }

    void expectSameRows(std::string_view query, std::string_view reference) {
        Rows expected;
        Rows actual;
        rowsOf(reference, expected);
        rowsOf(query, actual);

        EXPECT_EQ(sorted(actual), sorted(expected)) << query << "\nagainst\n" << reference;
    }
};

TEST_F(ExploreEdgeTypeDisjunctionTest, walksEveryTypeItNames) {
    expectSameRows("MATCH (n:Person)-[e:KNOWS_WELL|INTERESTED_IN*1..3]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e*1..3]->(m) RETURN n.name, m.name");
}

TEST_F(ExploreEdgeTypeDisjunctionTest, readsTheOrderOfTheNamesAsTheSameSet) {
    expectSameRows("MATCH (n:Person)-[e:INTERESTED_IN|KNOWS_WELL*1..3]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e:KNOWS_WELL|INTERESTED_IN*1..3]->(m) RETURN n.name, m.name");
}

TEST_F(ExploreEdgeTypeDisjunctionTest, dropsANameNoEdgeCarries) {
    expectSameRows("MATCH (n:Person)-[e:KNOWS_WELL|MISSING*1..3]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e:KNOWS_WELL*1..3]->(m) RETURN n.name, m.name");
}

TEST_F(ExploreEdgeTypeDisjunctionTest, walksNoEdgeWhenEveryNameIsAbsent) {
    Rows rows;
    rowsOf("MATCH (n:Person)-[e:MISSING|ALSO_MISSING*1..3]->(m) RETURN n.name, m.name", rows);

    EXPECT_TRUE(rows.empty());
}

// A minimum of zero still reports each seed against itself, whatever the types resolve to
TEST_F(ExploreEdgeTypeDisjunctionTest, keepsTheZeroLengthRowsOfAnAbsentName) {
    expectSameRows("MATCH (n:Person)-[e:MISSING|ALSO_MISSING*0..3]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person) RETURN n.name AS seed, n.name AS end");
}

TEST_F(ExploreEdgeTypeDisjunctionTest, walksFewerEdgesThanTheDisjunctionForOneType) {
    Rows both;
    Rows knows;
    rowsOf("MATCH (n:Person)-[e:KNOWS_WELL|INTERESTED_IN*1..3]->(m) RETURN n.name, m.name", both);
    rowsOf("MATCH (n:Person)-[e:KNOWS_WELL*1..3]->(m) RETURN n.name, m.name", knows);

    EXPECT_LT(knows.size(), both.size());
}

TEST_F(ExploreEdgeTypeDisjunctionTest, walksAfterAByTypeHop) {
    expectSameRows("MATCH (n:Person)-[:KNOWS_WELL]->(b)-[e:KNOWS_WELL|INTERESTED_IN*1..2]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[:KNOWS_WELL]->(b)-[e*1..2]->(m) RETURN n.name, m.name");
}

TEST_F(ExploreEdgeTypeDisjunctionTest, sharesTheTypeSetWithAHopThatNamesIt) {
    expectSameRows("MATCH (n:Person)-[:KNOWS_WELL]->(b)-[e:KNOWS_WELL*1..2]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[:KNOWS_WELL]->(b) MATCH (b)-[e:KNOWS_WELL*1..2]->(m) RETURN n.name, m.name");
}

TEST_F(ExploreEdgeTypeDisjunctionTest, walksBackwardOverEveryTypeItNames) {
    expectSameRows("MATCH (n)<-[e:KNOWS_WELL|INTERESTED_IN*1..3]-(m:Person) RETURN n.name, m.name",
                   "MATCH (n)<-[e*1..3]-(m:Person) RETURN n.name, m.name");
}

TEST_F(ExploreEdgeTypeDisjunctionTest, walksUndirectedOverEveryTypeItNames) {
    expectSameRows("MATCH (n:Person)-[e:KNOWS_WELL|INTERESTED_IN*1..2]-(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e*1..2]-(m) RETURN n.name, m.name");
}
