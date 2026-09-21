#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class GnnFrontierUnionTest : public CallV3Test {
};

namespace {

// Remy(0) is cited by Adam(1) and Ghosts(6); Adam and Ghosts are each cited by Remy
// alone. A fan-out of 8 exceeds every in-degree, so each sample takes the whole
// neighbourhood and the expectation is exact.
constexpr const char* newestHopQuery =
    "MATCH (n {name: 'Remy'}) "
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD src AS s1 "
    "CALL gnn.neighbourhoodSample(s1, 8, 43) YIELD src AS s2, tgt AS t2 "
    "RETURN DISTINCT s2, t2";

// The second call samples what the first yielded - Adam and Ghosts. Remy cites both of
// them but is not itself a node of that layer, so its own two in-edges are absent.
const std::vector<StringRowSink::Row> newestHopEdges {{"0", "1"},
                                                      {"0", "6"}};

}

// Chaining the sample calls covers one hop per layer, not the whole frontier a GraphSAGE
// layer aggregates over: each layer samples only what the one below it yielded, so the
// nodes of every earlier layer are left behind.
TEST_F(GnnFrontierUnionTest, chainingOnlySamplesTheNewestHop) {
    StringRowSink sink;
    runQuery(newestHopQuery, sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, newestHopEdges);
}
