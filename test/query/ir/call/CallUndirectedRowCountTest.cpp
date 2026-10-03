#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallUndirectedRowCountTest : public CallV3Test {
};

// simpledb holds 18 edges and no self-loop, so an undirected pattern drives a row from
// either end of each of them.
TEST_F(CallUndirectedRowCountTest, undirectedMatchDrivesTwoRowsPerEdge) {
    StringRowSink sink;
    runQuery("MATCH (n)--(m) RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"36"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The call reads no column of the pattern, so its two edge types repeat over every row the
// pattern drove.
TEST_F(CallUndirectedRowCountTest, crossedCallMultipliesEveryUndirectedRow) {
    StringRowSink sink;
    runQuery("MATCH (n)--(m) CALL db.edgeTypes() YIELD edgeType RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"72"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Remy's six incident edges put Adam and Ghosts twice in m, Computers and Eighties once. A
// sample of four is above every in-degree, so a row fans out to all of m's in-edges: one
// each for Adam, Ghosts and Eighties, two for Computers.
TEST_F(CallUndirectedRowCountTest, countsTheFanOutOfEveryFarEnd) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})--(m) CALL gnn.neighbourhoodSample(m, 4, 42) YIELD src RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallUndirectedRowCountTest, yieldsTheFanOutOfEveryFarEnd) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})--(m) CALL gnn.neighbourhoodSample(m, 4, 42) YIELD src RETURN src.name", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Luc"},
                                                    {"Remy"},
                                                    {"Remy"},
                                                    {"Remy"},
                                                    {"Remy"},
                                                    {"Remy"},
                                                    {"Remy"}};
    EXPECT_EQ(rows, expected);
}

// Each of the 36 rows fans out to the in-degree of its far end, summing degree(m) times
// inDegree(m) over the graph: 12 for Remy, nine for Gym, four each for Adam, Computers,
// Bio and Cooking, two for Ghosts, one each for Eighties, Padel, Animals, Travel and
// JiuJitsu.
TEST_F(CallUndirectedRowCountTest, countsTheFanOutOverTheWholeGraph) {
    StringRowSink sink;
    runQuery("MATCH (n)--(m) CALL gnn.neighbourhoodSample(m, 4, 42) YIELD src RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"44"}};
    EXPECT_EQ(sink.getRows(), expected);
}
