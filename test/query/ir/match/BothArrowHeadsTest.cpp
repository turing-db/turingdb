#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class BothArrowHeadsTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(BothArrowHeadsTest, matchesEveryEdgeFromBothEnds) {
    expectRows("MATCH (a)<-->(b) RETURN count(*)", {{"36"}});
    expectRows("MATCH (a)--(b) RETURN count(*)", {{"36"}});
}

TEST_F(BothArrowHeadsTest, matchesATypedEdgeInBothDirections) {
    expectRows("MATCH (a:Person {name: 'Remy'})<-[e:KNOWS_WELL]->(b) RETURN b.name",
               {{"Adam"}, {"Adam"}, {"Ghosts"}});
}

TEST_F(BothArrowHeadsTest, acceptsSpacesBetweenTheTokens) {
    expectRows("MATCH (a:Person {name: 'Remy'}) < - [:KNOWS_WELL] - > (b) RETURN b.name",
               {{"Adam"}, {"Adam"}, {"Ghosts"}});
    expectRows("MATCH (a:Person {name: 'Remy'}) < - - > (b) RETURN count(*)", {{"6"}});
}

TEST_F(BothArrowHeadsTest, closesACycle) {
    expectRows("MATCH (a)-->(b)<-->(a) RETURN count(*)", {{"22"}});
    expectRows("MATCH (a)<-->(b)<-->(a) RETURN count(*)", {{"44"}});
}

TEST_F(BothArrowHeadsTest, walksAVariableLengthPath) {
    expectRows("MATCH (a:Person {name: 'Remy'})<-[:KNOWS_WELL*2]->(b) RETURN b.name", {{"Remy"}, {"Remy"}});
}

TEST_F(BothArrowHeadsTest, hopsInsideAWherePattern) {
    expectRows("MATCH (a:Person) WHERE (a)<-->(:Interest {name: 'Ghosts'}) RETURN a.name", {{"Remy"}});
}

TEST_F(BothArrowHeadsTest, hopsInsideAPatternComprehension) {
    expectRows("MATCH (a:Person {name: 'Remy'}) RETURN size([(a)<-->(b) | b.name])", {{"6"}});
}

TEST_F(BothArrowHeadsTest, mergesOnAnExistingEdgeInEitherDirection) {
    StringRowSink sink;
    runWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) MERGE (a)<-[:KNOWS_WELL]->(b) RETURN b.name", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, (std::vector<StringRowSink::Row> {{"Adam"}, {"Adam"}}));
}

TEST_F(BothArrowHeadsTest, rejectsCreate) {
    runWriteExpectingError("CREATE (a:Person)<-[:KNOWS]->(b:Person)", "Only directed relationships are supported in CREATE");
    runWriteExpectingError("CREATE (a:Person)<-->(b:Person)", "Only directed relationships are supported in CREATE");
    runWriteExpectingError("CREATE (a:Person)-[:KNOWS]-(b:Person)", "Only directed relationships are supported in CREATE");
}
