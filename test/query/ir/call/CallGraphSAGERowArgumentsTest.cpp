#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "iterators/ChunkConfig.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

// CALL gnn.graphSAGE with arguments read from the rows before it, on the simpledb
// fixture. The call is made once per row, from that row's seeds, fanouts and RNG seed.
class CallGraphSAGERowArgumentsTest : public CallV3Test {};

TEST_F(CallGraphSAGERowArgumentsTest, samplesTheSeedsAMatchCollected) {
    StringRowSink sink;
    runQuery("WITH [2, 2, 2] AS fanouts "
             "MATCH (n:Person) "
             "WITH fanouts, collect(n) AS seeds "
             "CALL gnn.graphSAGE(seeds, fanouts, 42) YIELD dst_nodes0 "
             "WHERE dst_nodes0 IS NOT NULL "
             "RETURN dst_nodes0 ORDER BY dst_nodes0",
             sink);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}, {"8"}, {"9"}, {"11"}, {"12"}, {"15"}, {"17"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallGraphSAGERowArgumentsTest, variablesSampleWhatTheSameLiteralsDo) {
    StringRowSink literals;
    runQuery("CALL gnn.graphSAGE([0, 1, 8, 9], [2, 2, 2], 4242) YIELD src_nodes1, tgt_nodes1 "
             "WHERE src_nodes1 IS NOT NULL "
             "RETURN src_nodes1, tgt_nodes1 ORDER BY src_nodes1, tgt_nodes1",
             literals);

    StringRowSink variables;
    runQuery("WITH [0, 1, 8, 9] AS seeds, [2, 2, 2] AS fanouts, 4242 AS rngSeed "
             "CALL gnn.graphSAGE(seeds, fanouts, rngSeed) YIELD src_nodes1, tgt_nodes1 "
             "WHERE src_nodes1 IS NOT NULL "
             "RETURN src_nodes1, tgt_nodes1 ORDER BY src_nodes1, tgt_nodes1",
             variables);

    ASSERT_FALSE(literals.getRows().empty());
    EXPECT_EQ(variables.getRows(), literals.getRows());
}

// A one-seed sample's first frontier is its seed, so each row's dst_nodes0 is its own node
TEST_F(CallGraphSAGERowArgumentsTest, eachRowIsSampledFromItsOwnSeeds) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) "
             "CALL gnn.graphSAGE([n], [2, 2, 2], 42) YIELD dst_nodes0 "
             "WHERE dst_nodes0 IS NOT NULL "
             "RETURN n, dst_nodes0 ORDER BY n",
             sink);

    const std::vector<StringRowSink::Row> expected {
        {"0", "0"},
        {"1", "1"},
        {"8", "8"},
        {"9", "9"},
        {"11", "11"},
        {"12", "12"},
        {"15", "15"},
        {"17", "17"},
    };
    EXPECT_EQ(sink.getRows(), expected);
}

// With no hop expanded a sample is its seed alone: one row per Person
TEST_F(CallGraphSAGERowArgumentsTest, aSeedPerRowAnswersOneRowPerRow) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) "
             "CALL gnn.graphSAGE([n], [0, 0, 0], 42) YIELD dst_nodes0 "
             "RETURN count(*)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"8"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Computers (2) has two in-edges, so a row's first fanout is how many of them it draws
TEST_F(CallGraphSAGERowArgumentsTest, eachRowReadsItsOwnFanouts) {
    StringRowSink sink;
    runQuery("UNWIND [0, 1, 2] AS width "
             "CALL gnn.graphSAGE([2], [width, 0, 0], 42) YIELD src_nodes0 "
             "RETURN width, count(src_nodes0) ORDER BY width",
             sink);

    const std::vector<StringRowSink::Row> expected {{"0", "0"}, {"1", "1"}, {"2", "2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Gym (13) has three in-edges and the fanout draws two, so each row draws two of three.
// A row's sample depends on its own arguments alone, so eight rows with one RNG seed
// draw the same two.
TEST_F(CallGraphSAGERowArgumentsTest, rowsWithTheSameRNGSeedDrawTheSameSample) {
    StringRowSink sink;
    runQuery("UNWIND [7, 7, 7, 7, 7, 7, 7, 7] AS rngSeed "
             "CALL gnn.graphSAGE([13], [2, 0, 0], rngSeed) YIELD src_nodes0 "
             "WHERE src_nodes0 IS NOT NULL "
             "RETURN src_nodes0, count(*) ORDER BY src_nodes0",
             sink);

    ASSERT_EQ(sink.getRows().size(), 2U);

    for (const StringRowSink::Row& row : sink.getRows()) {
        EXPECT_EQ(row[1], "8") << "node " << row[0] << " was not drawn by every row";
    }
}

TEST_F(CallGraphSAGERowArgumentsTest, aRowWithNoSeedsIsAnsweredWithNoRows) {
    StringRowSink sink;
    runQuery("UNWIND [[0], null, [], [8]] AS seeds "
             "CALL gnn.graphSAGE(seeds, [0, 0, 0], 42) YIELD dst_nodes0 "
             "RETURN dst_nodes0 ORDER BY dst_nodes0",
             sink);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"8"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A subquery body that aggregates reads the row it was called for as a constant, null
// included
TEST_F(CallGraphSAGERowArgumentsTest, aSubqueryBodyCallsItForEachOuterRow) {
    StringRowSink sink;
    runQuery("UNWIND [[0], null, [8, 9]] AS seeds "
             "CALL { "
             "WITH seeds "
             "CALL gnn.graphSAGE(seeds, [0, 0, 0], 42) YIELD dst_nodes0 "
             "RETURN count(dst_nodes0) AS seedCount "
             "} "
             "RETURN seedCount",
             sink);

    const std::vector<StringRowSink::Row> expected {{"1"}, {"0"}, {"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallGraphSAGERowArgumentsTest, rejectsAFanoutListOfTheWrongSizeReadFromARow) {
    runQueryExpectingError("UNWIND [[2, 2, 2], [2, 2]] AS fanouts "
                           "CALL gnn.graphSAGE([0], fanouts) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "Fanout");
}

TEST_F(CallGraphSAGERowArgumentsTest, rejectsAFanoutWiderThanAChunkReadFromARow) {
    const std::string overWide = std::to_string(ChunkConfig::CHUNK_SIZE + 1);

    runQueryExpectingError("UNWIND [2, " + overWide + "] AS width "
                           "CALL gnn.graphSAGE([0], [width, 2, 2]) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "Fanout");
}

TEST_F(CallGraphSAGERowArgumentsTest, rejectsANullFanoutList) {
    runQueryExpectingError("UNWIND [[2, 2, 2], null] AS fanouts "
                           "CALL gnn.graphSAGE([0], fanouts) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "Fanout");
}
