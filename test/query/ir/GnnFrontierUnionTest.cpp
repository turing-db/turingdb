#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class GnnFrontierUnionTest : public CallV3Test {
};

namespace {

// Remy(0) cites Adam(1), Computers(2), Eighties(3) and Ghosts(6); Adam cites Remy, Bio(4)
// and Cooking(5); Ghosts cites Remy; the rest cite nobody. A fan-out of 8 exceeds every
// out-degree, so each sample takes the whole neighbourhood and the expectation is exact.
constexpr const char* newestHopQuery =
    "MATCH (n {name: 'Remy'}) "
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD tgt AS t1 "
    "CALL gnn.neighbourhoodSample(t1, 8, 43) YIELD src AS s2, tgt AS t2 "
    "RETURN DISTINCT s2, t2";

// The second call samples what the first yielded - Adam, Computers, Eighties and Ghosts.
// Remy is a destination of that layer but never a source, so its own four edges are absent.
const std::vector<StringRowSink::Row> newestHopEdges {{"1", "0"},
                                                      {"1", "4"},
                                                      {"1", "5"},
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
