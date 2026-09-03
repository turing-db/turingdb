#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An UNWIND of node IDs naming a pattern node seeds the traversal from exactly those
// nodes. Unlike the node-ID disjunction it resembles, the seed is a list and not a set:
// the nodes come in the order written, and a repeated ID is matched again.
class UnwindNodeSeedV3Test : public CallV3Test {
};

TEST_F(UnwindNodeSeedV3Test, seedsAHopFromTheListedNodes) {
    StringRowSink sink;
    runQuery("UNWIND [6, 0] AS x MATCH (x)-->(w) RETURN w", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    // Ghosts (6) knows Remy (0), whose neighbours are Adam, Computers, Eighties and Ghosts.
    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}, {"2"}, {"3"}, {"6"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(UnwindNodeSeedV3Test, matchesTheDisjunctionFormItResembles) {
    StringRowSink bySeed;
    runQuery("UNWIND [6, 0] AS x MATCH (x)-->(w) RETURN w", bySeed);

    StringRowSink byDisjunction;
    runQuery("MATCH (n)-->(w) WHERE n = 6 OR n = 0 RETURN w", byDisjunction);

    std::vector<StringRowSink::Row> bySeedRows;
    bySeed.sortedRows(bySeedRows);
    std::vector<StringRowSink::Row> byDisjunctionRows;
    byDisjunction.sortedRows(byDisjunctionRows);

    EXPECT_FALSE(bySeedRows.empty());
    EXPECT_EQ(bySeedRows, byDisjunctionRows);
}

TEST_F(UnwindNodeSeedV3Test, yieldsTheNodesInListOrder) {
    StringRowSink sink;
    runQuery("UNWIND [6, 0] AS x MATCH (x) RETURN x", sink);

    // The disjunction form yields the same two nodes in scan order, 0 then 6.
    const std::vector<StringRowSink::Row> expected {{"6"}, {"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, repeatedIDYieldsTheNodeTwice) {
    StringRowSink sink;
    runQuery("UNWIND [3, 3] AS x MATCH (x) RETURN x", sink);

    const std::vector<StringRowSink::Row> expected {{"3"}, {"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, absentIDMatchesNothing) {
    StringRowSink sink;
    runQuery("UNWIND [2, 99999] AS x MATCH (x) RETURN x", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, readsAPropertyOffTheSeededNodes) {
    StringRowSink sink;
    runQuery("UNWIND [1, 0] AS x MATCH (x) RETURN x.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, aLabelKeepsOnlyTheLabelledSeeds) {
    StringRowSink sink;
    runQuery("UNWIND [2, 0] AS x MATCH (x:Person) RETURN x.name", sink);

    // Remy (0) is a Person; Computers (2) is an Interest.
    const std::vector<StringRowSink::Row> expected {{"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, hopsByEdgeTypeFromASeededNode) {
    StringRowSink sink;
    runQuery("UNWIND [0] AS x MATCH (x)-[:KNOWS_WELL]->(w) RETURN w.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, countsOverTheSeededTraversal) {
    StringRowSink sink;
    runQuery("UNWIND [0, 1] AS x MATCH (x)-->(w) RETURN count(w)", sink);

    // Remy has four out edges, Adam three.
    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, limitBoundsTheSeededTraversal) {
    StringRowSink sink;
    runQuery("UNWIND [6, 0] AS x MATCH (x)-->(w) RETURN w LIMIT 1", sink);

    // Ghosts (6) comes first and reaches Remy (0) alone.
    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindNodeSeedV3Test, seedAtTheFarEndWalksTheHopBackwards) {
    StringRowSink sink;
    runQuery("UNWIND [2] AS x MATCH (a)-->(x) RETURN a.name", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    // Computers (2) is reached by Remy and Luc.
    const std::vector<StringRowSink::Row> expected {{"Luc"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}
