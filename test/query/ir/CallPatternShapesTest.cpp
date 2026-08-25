#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallPatternShapesTest : public CallV3Test {
};

TEST_F(CallPatternShapesTest, propertyConstraintBeforeACall) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) CALL db.edgeTypes() YIELD edgeType RETURN n.name, edgeType", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Remy", "INTERESTED_IN"}, {"Remy", "KNOWS_WELL"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallPatternShapesTest, propertyConstraintOnAYieldedNode) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1, 2]) YIELD id AS a MATCH (a {name: 'Adam'}) RETURN a", sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Remy and Adam are Person nodes, Computers is not.
TEST_F(CallPatternShapesTest, labelOnAYieldedNode) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1, 2]) YIELD id AS a MATCH (a:Person) RETURN a", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}};
    EXPECT_EQ(rows, expected);
}

// Remy's four out-edges each crossed with the two edge types.
TEST_F(CallPatternShapesTest, edgeVariableAroundACall) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})-[e]->(m) CALL db.edgeTypes() YIELD id RETURN e, count(id)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "2"}, {"1", "2"}, {"2", "2"}, {"3", "2"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallPatternShapesTest, edgeTypeConstraintAroundACall) {
    StringRowSink sink;
    runQuery("MATCH (n)-[e:KNOWS_WELL]->(m) CALL db.getNodes([0]) YIELD id RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallPatternShapesTest, incomingEdgeAroundACall) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Adam'})<--(m) CALL db.getNodes([0]) YIELD id RETURN m.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Adam's neighbours in both directions: Remy twice, once per edge between them.
TEST_F(CallPatternShapesTest, undirectedEdgeAroundACall) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Adam'})--(m) CALL db.getNodes([0]) YIELD id RETURN m.name", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Bio"}, {"Cooking"}, {"Remy"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallPatternShapesTest, edgeTypeOfACarriedEdge) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})-[e]->(m {name: 'Adam'}) CALL db.getNodes([0]) YIELD id RETURN edgeType(e)", sink);

    const std::vector<StringRowSink::Row> expected {{"KNOWS_WELL"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallPatternShapesTest, labelsOfAYieldedNode) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0]) YIELD id AS a RETURN labels(a)", sink);

    const std::vector<StringRowSink::Row> expected {{"Person, SoftwareEngineering, Founder"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallPatternShapesTest, edgeTypeOfAYieldedEdge) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([0]) YIELD id AS e RETURN edgeType(e)", sink);

    const std::vector<StringRowSink::Row> expected {{"KNOWS_WELL"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// All four return values of gnn.neighbourhoodSample yielded at once: the sampled edge is one
// of Remy's, from Remy to one of the four nodes it points at.
TEST_F(CallPatternShapesTest, yieldsEveryGnnColumnAtOnce) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) CALL gnn.neighbourhoodSample(n, 1, 42) "
             "YIELD src, edge, edgeType, tgt RETURN src, edge, edgeType, tgt",
             sink);

    const std::vector<StringRowSink::Row>& rows = sink.getRows();
    ASSERT_EQ(rows.size(), 1u);

    const StringRowSink::Row& row = rows.front();
    ASSERT_EQ(row.size(), 4u);
    EXPECT_EQ(row[0], "0");

    const std::vector<std::string> remyEdges {"0", "1", "2", "3"};
    const std::vector<std::string> remyTargets {"1", "2", "3", "6"};
    EXPECT_TRUE(std::find(remyEdges.begin(), remyEdges.end(), row[1]) != remyEdges.end()) << row[1];
    EXPECT_TRUE(row[2] == "0" || row[2] == "1") << row[2];
    EXPECT_TRUE(std::find(remyTargets.begin(), remyTargets.end(), row[3]) != remyTargets.end()) << row[3];
}
