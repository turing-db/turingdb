#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class GnnFrontierUnionTest : public CallV3Test {
};

namespace {

// The sample is undirected, so a node's neighbourhood is what it points to and what points
// to it. Remy(0) is joined to Adam(1) twice, to Ghosts(6) twice and to Computers(2) and
// Eighties(3) once; Adam to Remy twice, Bio(4) and Cooking(5); Computers to Remy and
// Luc(9). A fan-out of 8 exceeds every degree, so each sample takes the whole
// neighbourhood and the expectation is exact.
constexpr const char* newestHopQuery =
    "MATCH (n {name: 'Remy'}) "
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD tgt AS t1 "
    "CALL gnn.neighbourhoodSample(t1, 8, 43) YIELD src AS s2, tgt AS t2 "
    "RETURN DISTINCT s2, t2";

// The second call samples what the first yielded - Adam, Computers, Eighties and Ghosts.
// Remy is a neighbour of that layer but never a seed of it, so its own edges are absent.
const std::vector<StringRowSink::Row> newestHopEdges {{"1", "0"},
                                                      {"1", "4"},
                                                      {"1", "5"},
                                                      {"2", "0"},
                                                      {"2", "9"},
                                                      {"3", "0"},
                                                      {"6", "0"}};

}

// Chaining the sample calls covers one hop per layer, not the whole frontier a GraphSAGE
// layer aggregates over: each layer samples only what the one below it yielded, so every
// node carried forward reaches the next layer with no incoming edge of its own.
TEST_F(GnnFrontierUnionTest, chainingOnlySamplesTheNewestHop) {
    StringRowSink sink;
    runQuery(newestHopQuery, sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, newestHopEdges);
}
