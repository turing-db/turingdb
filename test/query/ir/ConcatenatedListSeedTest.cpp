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

// The sample is undirected, so sampling Remy(0) and Adam(1) reaches {1, 2, 3, 6} and
// {0, 4, 5}, which already holds both seeds: appending them changes nothing and either
// frontier samples the same 15 edges. Remy is joined to Adam, Computers(2), Eighties(3)
// and Ghosts(6); Adam to Remy, Bio(4) and Cooking(5); Computers to Luc(9); Bio to
// Maxime(8); Cooking to Martina(11).
const std::vector<StringRowSink::Row> frontierEdges {{"0", "1"},
                                                     {"0", "2"},
                                                     {"0", "3"},
                                                     {"0", "6"},
                                                     {"1", "0"},
                                                     {"1", "4"},
                                                     {"1", "5"},
                                                     {"2", "0"},
                                                     {"2", "9"},
                                                     {"3", "0"},
                                                     {"4", "1"},
                                                     {"4", "8"},
                                                     {"5", "1"},
                                                     {"5", "11"},
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
