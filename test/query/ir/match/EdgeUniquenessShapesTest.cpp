#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

using Rows = std::vector<StringRowSink::Row>;

// Relationship isomorphism over the shapes EdgeUniquenessTest and PathEdgeUniquenessTest do
// not reach: overlapping type sets, comma patterns that spell a chain, a path between two
// hops, and the rows, groups and distinct keys the rule leaves. Expected values are an
// enumeration of simpledb's 18 edges under openCypher's rule.
class EdgeUniquenessShapesTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(EdgeUniquenessShapesTest, excludesAnEdgeOfTheTypeTwoTypeSetsShare) {
    expectRows("MATCH (a)-[e1:KNOWS_WELL|INTERESTED_IN]->(b)<-[e2:KNOWS_WELL]-(c) RETURN count(*)", {{"2"}});
    expectRows("MATCH (a)-[e1:KNOWS_WELL|INTERESTED_IN]-(b)-[e2:KNOWS_WELL]-(c) RETURN count(*)", {{"22"}});
}

TEST_F(EdgeUniquenessShapesTest, countsACommaPatternAsTheChainItSpells) {
    expectRows("MATCH (a)-[e1]-(b), (b)-[e2]-(c) RETURN count(*)", {{"64"}});
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c) RETURN count(*)", {{"64"}});
}

TEST_F(EdgeUniquenessShapesTest, excludesEveryPairOfAStarOfThreePatterns) {
    expectRows("MATCH (a)-[e1]->(b), (a)-[e2]->(c), (a)-[e3]->(d) RETURN count(*)", {{"30"}});
}

TEST_F(EdgeUniquenessShapesTest, excludesTheHopBeforeAZeroLengthPath) {
    expectRows("MATCH (a)-[e1]->(b)-[e*0..2]->(c) RETURN count(*)", {{"42"}});
}

TEST_F(EdgeUniquenessShapesTest, excludesTheHopBeforeAnUndirectedPath) {
    expectRows("MATCH (a)-[e1]-(b)-[e*1..2]-(c) RETURN count(*)", {{"170"}});
}

TEST_F(EdgeUniquenessShapesTest, excludesTwoHopsFromAPathOnEitherSide) {
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c)-[e*1..2]-(d) RETURN count(*)", {{"258"}});
    expectRows("MATCH (a)-[e1]-(b)-[e*1..2]-(c)-[e2]-(d) RETURN count(*)", {{"258"}});
    expectRows("MATCH (a)-[e*1..2]-(b)-[e1]-(c)-[e2]-(d) RETURN count(*)", {{"258"}});
}

TEST_F(EdgeUniquenessShapesTest, returnsTheRowsLeftAfterTheBacktrack) {
    expectRows("MATCH (a {name: 'Adam'})-[e1]->(b)-[e2]-(c) RETURN b.name, c.name ORDER BY b.name, c.name",
               {{"Bio", "Maxime"},
                {"Cooking", "Martina"},
                {"Remy", "Adam"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Remy", "Ghosts"}});
}

TEST_F(EdgeUniquenessShapesTest, groupsTheRowsLeftAfterTheBacktrack) {
    expectRows("MATCH (a)-[e1]->(b)-[e2]-(c) RETURN a.name, count(*) ORDER BY a.name",
               {{"Adam", "7"},
                {"Cyrus", "2"},
                {"Doruk", "2"},
                {"Ghosts", "5"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Maxime", "1"},
                {"Remy", "5"},
                {"Suhas", "2"}});
}

TEST_F(EdgeUniquenessShapesTest, keysDistinctOnTheRowsLeftAfterTheBacktrack) {
    expectRows("MATCH (a)-[e1]->(b)-[e2]-(c) WITH DISTINCT a, c RETURN count(*)", {{"23"}});
    expectRows("MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(DISTINCT e1), count(DISTINCT e2)", {{"13", "14"}});
}

TEST_F(EdgeUniquenessShapesTest, filtersAnEdgePropertyOnTheRowsLeftAfterTheBacktrack) {
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c) WHERE e1.duration = 20 RETURN count(*)", {{"28"}});
}
