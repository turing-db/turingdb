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
// sample of four is above every out-degree, so a row fans out to all of m's out-edges:
// three for Adam, one for Ghosts, none at all for the two interests.
TEST_F(CallUndirectedRowCountTest, countsTheFanOutOfEveryFarEnd) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})--(m) CALL gnn.neighbourhoodSample(m, 4, 42) YIELD tgt RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"8"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallUndirectedRowCountTest, yieldsTheFanOutOfEveryFarEnd) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})--(m) CALL gnn.neighbourhoodSample(m, 4, 42) YIELD tgt RETURN tgt.name", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Bio"},
                                                    {"Bio"},
                                                    {"Cooking"},
                                                    {"Cooking"},
                                                    {"Remy"},
                                                    {"Remy"},
                                                    {"Remy"},
                                                    {"Remy"}};
    EXPECT_EQ(rows, expected);
}

// Each of the 36 rows fans out to the out-degree of its far end, summing degree(m) times
// outDegree(m) over the graph: 24 for Remy, 12 for Adam, four each for Maxime, Luc, Suhas
// and Cyrus, two for Ghosts, one each for Martina and Doruk.
TEST_F(CallUndirectedRowCountTest, countsTheFanOutOverTheWholeGraph) {
    StringRowSink sink;
    runQuery("MATCH (n)--(m) CALL gnn.neighbourhoodSample(m, 4, 42) YIELD tgt RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"56"}};
    EXPECT_EQ(sink.getRows(), expected);
}
