#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

// The sample is undirected. Remy (0) is joined to Adam (1) twice and to Ghosts (6) twice,
// once each way, and to Computers (2) and Eighties (3) once; Adam to Remy twice, Bio (4)
// and Cooking (5); Ghosts to Remy twice. A sample size above every degree takes them all.
const std::vector<StringRowSink::Row> seededNeighbours {
    {"0", "1"}, {"0", "1"}, {"0", "2"}, {"0", "3"}, {"0", "6"}, {"0", "6"},
    {"1", "0"}, {"1", "0"}, {"1", "4"}, {"1", "5"},
    {"6", "0"}, {"6", "0"}};

}

// An UNWIND of node IDs compared to a pattern node seeds the calls chained behind it from
// exactly those nodes, whether the query still reads the unwound values or not.
class UnwindNodeSeedCallV3Test : public CallV3Test {
};

TEST_F(UnwindNodeSeedCallV3Test, seedsACallFromTheListedNodes) {
    StringRowSink sink;
    runQuery("UNWIND [0, 1, 6] AS id MATCH (n) WHERE n = id "
             "CALL gnn.neighbourhoodSample(n, 10, 11) YIELD tgt RETURN n, tgt",
             sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, seededNeighbours);
}

TEST_F(UnwindNodeSeedCallV3Test, readsTheUnwoundValuePastTheCall) {
    StringRowSink sink;
    runQuery("UNWIND [0, 1, 6] AS id MATCH (n) WHERE n = id "
             "CALL gnn.neighbourhoodSample(n, 10, 11) YIELD tgt RETURN id, tgt",
             sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, seededNeighbours);
}

// Every three-hop undirected path from the seeds: 70 from Remy, 41 from Adam and 30 from
// Ghosts, whose only neighbour is Remy.
TEST_F(UnwindNodeSeedCallV3Test, matchesTheDisjunctionFormThroughChainedCalls) {
    const std::string calls = " CALL gnn.neighbourhoodSample(n, 10, 11) YIELD tgt AS m"
                              " CALL gnn.neighbourhoodSample(m, 10, 22) YIELD tgt AS k"
                              " CALL gnn.neighbourhoodSample(k, 10, 33) YIELD tgt AS l"
                              " RETURN n, m, k, l";

    StringRowSink bySeed;
    runQuery("UNWIND [0, 1, 6] AS id MATCH (n) WHERE n = id" + calls, bySeed);

    StringRowSink byDisjunction;
    runQuery("MATCH (n) WHERE n = 0 OR n = 1 OR n = 6" + calls, byDisjunction);

    std::vector<StringRowSink::Row> bySeedRows;
    bySeed.sortedRows(bySeedRows);
    std::vector<StringRowSink::Row> byDisjunctionRows;
    byDisjunction.sortedRows(byDisjunctionRows);

    EXPECT_EQ(bySeedRows.size(), 141u);
    EXPECT_EQ(bySeedRows, byDisjunctionRows);
}
