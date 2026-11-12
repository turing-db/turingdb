#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

class ZeroMaximumHopsTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }
};

TEST_F(ZeroMaximumHopsTest, walksZeroToZeroHops) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0..0]->(x) RETURN p, x.name, length(p)",
               {{"(0)", "Remy", "0"}});
}

TEST_F(ZeroMaximumHopsTest, walksExactlyZeroHops) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x) RETURN p, x.name, length(p)",
               {{"(0)", "Remy", "0"}});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsForAQuantifier) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN]->{0,0}(x) RETURN p, x.name",
               {{"(0)", "Remy"}});
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN]->{0}(x) RETURN p, x.name",
               {{"(0)", "Remy"}});
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN]->{,0}(x) RETURN p, x.name",
               {{"(0)", "Remy"}});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsForAQuantifiedPathPattern) {
    expectRows("MATCH (m:Person {name: 'Remy'}) ((a)-[:INTERESTED_IN]->(b)){0,0} (x) RETURN x.name, size(a), a = []",
               {{"Remy", "0", "true"}});
    expectRows("MATCH (m:Person {name: 'Remy'}) ((a)-[:INTERESTED_IN]->(b)){0} (x) RETURN x.name, size(b), b = []",
               {{"Remy", "0", "true"}});
}

TEST_F(ZeroMaximumHopsTest, bindsTheRelationshipsToTheEmptyList) {
    expectRows("MATCH (m:Person {name: 'Remy'})-[r:INTERESTED_IN*0..0]->(x) RETURN size(r), r = []",
               {{"0", "true"}});
    expectRows("MATCH (m:Person {name: 'Remy'})-[r:INTERESTED_IN*0]->(x) RETURN size(r), r = []",
               {{"0", "true"}});
}

TEST_F(ZeroMaximumHopsTest, returnsTheEmptyRelationshipList) {
    expectRows("MATCH (m:Person {name: 'Remy'})-[r:INTERESTED_IN*0..0]->(x) RETURN r, size(r), x.name",
               {{"", "0", "Remy"}});
    expectRows("MATCH (m:Person {name: 'Remy'})-[r:INTERESTED_IN*0]->(x) RETURN all(e IN r WHERE e.duration > 100)",
               {{"true"}});
}

TEST_F(ZeroMaximumHopsTest, measuresAPathOfTheStartNodeAlone) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x) RETURN length(p)", {{"0"}});
    expectRows("MATCH p = (m:Person)-[:INTERESTED_IN*0]->(x) RETURN count(DISTINCT p)", {{"8"}});
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x) RETURN collect(p)", {{"(0)"}});
}

TEST_F(ZeroMaximumHopsTest, namesAPathOfTheStartNodeAlone) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x) "
               "RETURN size(nodes(p)), size(relationships(p)), id(x) = id(m), x = m",
               {{"1", "0", "true", "true"}});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsFromEveryStartNode) {
    expectRows("MATCH (n:Person)-[*0]->(m) RETURN n.name, m.name",
               {{"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Maxime", "Maxime"},
                {"Luc", "Luc"},
                {"Martina", "Martina"},
                {"Suhas", "Suhas"},
                {"Cyrus", "Cyrus"},
                {"Doruk", "Doruk"}});
    expectRows("MATCH (n)-[*0..0]->(m) WHERE n <> m RETURN count(*)", {{"0"}});
    expectRows("MATCH (n)-[*0..0]->(m) RETURN count(*)", {{"18"}});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsInEveryDirection) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})<-[:INTERESTED_IN*0]-(x) RETURN p, x.name",
               {{"(0)", "Remy"}});
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]-(x) RETURN p, x.name",
               {{"(0)", "Remy"}});
}

TEST_F(ZeroMaximumHopsTest, matchesWhereTheStartNodeHasTheEndLabel) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0..0]->(x:Interest) RETURN p", {});
    expectRows("MATCH (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x:Person) RETURN x.name", {{"Remy"}});
}

TEST_F(ZeroMaximumHopsTest, matchesWhereTheStartNodeHasTheEndProperties) {
    expectRows("MATCH (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x {name: 'Adam'}) RETURN x.name", {});
    expectRows("MATCH (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x {name: 'Remy'}) RETURN x.name", {{"Remy"}});
    expectRows("MATCH (m:Person {name: 'Remy'})-[:INTERESTED_IN*0]->(x) WHERE x.age > 40 RETURN x.name", {});
}

TEST_F(ZeroMaximumHopsTest, ignoresTheHopPropertiesOfAWalkThatTakesNoHop) {
    expectRows("MATCH (m:Person {name: 'Remy'})-[:INTERESTED_IN*0 {duration: 999}]->(x) RETURN x.name", {{"Remy"}});
}

TEST_F(ZeroMaximumHopsTest, matchesABoundEndOnlyWhereItIsTheStartNode) {
    expectRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) MATCH (a)-[*0..0]->(b) RETURN count(*)",
               {{"0"}});
    expectRows("MATCH (a:Person {name: 'Remy'}) MATCH (a)-[*0..0]->(a) RETURN count(*)", {{"1"}});
}

TEST_F(ZeroMaximumHopsTest, continuesWithAHopFromTheStartNode) {
    expectRows("MATCH (m:Person {name: 'Remy'})-[:KNOWS_WELL*0]->(x)-[:INTERESTED_IN]->(i) RETURN x.name, i.name",
               {{"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"}});
}

TEST_F(ZeroMaximumHopsTest, joinsAZeroHopSegmentToTheNextHopInAPath) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:KNOWS_WELL*0]->(x)-[:INTERESTED_IN]->(i) RETURN p, length(p)",
               {{"(0), [1], (6)", "1"},
                {"(0), [2], (2)", "1"},
                {"(0), [3], (3)", "1"}});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsSeededFromTheEndNode) {
    expectRows("MATCH (x:Person {name: 'Remy'}) MATCH (m)-[:INTERESTED_IN*0]->(x) RETURN m.name",
               {{"Remy"}});
    expectRows("MATCH (x:Interest {name: 'Ghosts'}) MATCH (m:Person)-[:INTERESTED_IN*0]->(x) RETURN m.name",
               {});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsInsideExists) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN EXISTS { (n)-[:INTERESTED_IN*0]->(:Interest) }",
               {{"false"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN EXISTS { (n)-[:INTERESTED_IN*0]->(:Person) }",
               {{"true"}});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsInsideAPatternComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[*0]->(y) | y.name]",
               {{"Remy"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[:INTERESTED_IN*0]->(y:Interest) | y.name]",
               {{""}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN size([(n)-[:INTERESTED_IN*0]->(y:Interest) | y.name])",
               {{"0"}});
}

TEST_F(ZeroMaximumHopsTest, walksZeroHopsOverAnEdgeTypeDisjunction) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[r:KNOWS_WELL|INTERESTED_IN*0]->(x) RETURN p, x.name, size(r)",
               {{"(0)", "Remy", "0"}});
}

TEST_F(ZeroMaximumHopsTest, padsAnOptionalWalkWhoseEndFails) {
    expectRows("MATCH (n:Person {name: 'Remy'}) OPTIONAL MATCH (n)-[:KNOWS_WELL*0]->(x:Interest) RETURN n.name, x.name",
               {{"Remy", "null"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) OPTIONAL MATCH (n)-[:KNOWS_WELL*0]->(x) RETURN n.name, x.name",
               {{"Remy", "Remy"}});
}

TEST_F(ZeroMaximumHopsTest, stillWalksAMinimumOfZeroBesideOneHop) {
    expectRows("MATCH p = (m:Person {name: 'Remy'})-[:INTERESTED_IN*0..1]->(x) RETURN p, length(p)",
               {{"(0)", "0"},
                {"(0), [1], (6)", "1"},
                {"(0), [2], (2)", "1"},
                {"(0), [3], (3)", "1"}});
}
