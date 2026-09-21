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
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD tgt "
    "WITH collect(DISTINCT tgt) AS sampled "
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

// Sampling Remy(0) and Adam(1) reaches {1, 6, 2, 3} and {0, 4, 5}, which already holds both
// seeds, so appending them changes nothing: either frontier samples the same eight edges.
// Remy cites Adam(1), Ghosts(6), Computers(2) and Eighties(3); Adam cites Remy, Bio(4) and
// Cooking(5); Ghosts cites Remy; the rest cite nobody.
const std::vector<StringRowSink::Row> frontierEdges {{"0", "1"},
                                                     {"0", "2"},
                                                     {"0", "3"},
                                                     {"0", "6"},
                                                     {"1", "0"},
                                                     {"1", "4"},
                                                     {"1", "5"},
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
