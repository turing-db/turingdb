#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallGroupedAggregateTest : public CallV3Test {
};

TEST_F(CallGroupedAggregateTest, countsYieldsPerCarriedRow) {
    StringRowSink sink;
    runQuery("UNWIND [0, 1] AS x CALL db.getNodes([0, 1]) YIELD id RETURN x, count(id)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "2"}, {"1", "2"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallGroupedAggregateTest, countsCarriedRowsPerYield) {
    StringRowSink sink;
    runQuery("UNWIND [0, 1, 2] AS x CALL db.getNodes([0, 1]) YIELD id RETURN id, count(x)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "3"}, {"1", "3"}};
    EXPECT_EQ(rows, expected);
}

// The crossed yield is the grouping key, and every label sees the eight Person nodes.
TEST_F(CallGroupedAggregateTest, countsMatchedNodesPerCrossedYield) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) CALL db.labels() YIELD label RETURN label, count(n)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Bioinformatics", "8"},
                                                    {"Exotic", "8"},
                                                    {"Founder", "8"},
                                                    {"Interest", "8"},
                                                    {"Person", "8"},
                                                    {"Sales", "8"},
                                                    {"SleepDisturber", "8"},
                                                    {"SoftwareEngineering", "8"},
                                                    {"Supernatural", "8"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallGroupedAggregateTest, countsYieldsPerMatchedProperty) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) CALL db.edgeTypes() YIELD edgeType RETURN n.name, count(edgeType)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Adam", "2"},
                                                    {"Cyrus", "2"},
                                                    {"Doruk", "2"},
                                                    {"Luc", "2"},
                                                    {"Martina", "2"},
                                                    {"Maxime", "2"},
                                                    {"Remy", "2"},
                                                    {"Suhas", "2"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallGroupedAggregateTest, countsPerYieldedEdgeType) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([0, 1, 2, 3]) YIELD edgeTypeID RETURN edgeTypeID, count(*)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "1"}, {"1", "3"}};
    EXPECT_EQ(rows, expected);
}

// The yielded node drives the traversal and is the grouping key of what it expanded to:
// Remy points at four nodes, Adam at three.
TEST_F(CallGroupedAggregateTest, countsExpansionsPerDrivenRoot) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD id AS a MATCH (a)-->(m) RETURN a, count(m)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "4"}, {"1", "3"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallGroupedAggregateTest, countsEveryRowOfACrossedCall) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) CALL db.edgeTypes() YIELD edgeType RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"16"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallGroupedAggregateTest, countsEveryRowOfADrivenTraversal) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD id AS a MATCH (a)-->(m) RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallGroupedAggregateTest, countsAYieldedList) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD labels RETURN count(labels)", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}
