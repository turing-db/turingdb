#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class ConcatenatedListSeedTest : public CallV3Test {
};

namespace {

constexpr const char* collectedListQuery =
    "MATCH (n) WHERE n = 0 OR n = 1 "
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD src "
    "WITH collect(DISTINCT src) AS sampled "
    "UNWIND sampled AS node "
    "CALL gnn.neighbourhoodSample(node, 8, 43) YIELD src, tgt "
    "RETURN DISTINCT src, tgt";

constexpr const char* concatenatedListQuery =
    "MATCH (n) WHERE n = 0 OR n = 1 "
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD src, tgt "
    "WITH collect(DISTINCT src) AS seeds, collect(DISTINCT tgt) AS sampled "
    "UNWIND seeds + sampled AS node "
    "CALL gnn.neighbourhoodSample(node, 8, 43) YIELD src, tgt "
    "RETURN DISTINCT src, tgt";

// Sampling Remy(0) and Adam(1) reaches {1, 6} and {0}, which already holds both seeds, so
// appending them changes nothing: either frontier samples the same four edges. Remy is
// cited by Adam(1) and Ghosts(6); Adam and Ghosts are each cited by Remy alone.
const std::vector<StringRowSink::Row> frontierEdges {{"0", "1"},
                                                     {"0", "6"},
                                                     {"1", "0"},
                                                     {"6", "0"}};

}

TEST_F(ConcatenatedListSeedTest, seedsACallFromACollectedList) {
    StringRowSink sink;
    runQuery(collectedListQuery, sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, frontierEdges);
}

TEST_F(ConcatenatedListSeedTest, seedsACallFromTwoCollectedListsConcatenated) {
    StringRowSink sink;
    runQuery(concatenatedListQuery, sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, frontierEdges);
}
