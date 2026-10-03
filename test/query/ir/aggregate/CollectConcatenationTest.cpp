#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CollectConcatenationTest : public CallV3Test {
};

namespace {

constexpr const char* barrierQuery =
    "MATCH (n {name: 'Remy'}) "
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD src, tgt "
    "WITH collect(DISTINCT src) AS seeds, collect(DISTINCT tgt) AS sampled "
    "RETURN seeds + sampled AS nodes";

constexpr const char* returnQuery =
    "MATCH (n {name: 'Remy'}) "
    "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD src, tgt "
    "RETURN collect(DISTINCT src) + collect(DISTINCT tgt) AS nodes";

// Remy(0) is cited by Adam(1) and Ghosts(6). A fan-out of 8 exceeds his in-degree, so the
// sample takes the whole neighbourhood: the sources collect to [1, 6] and the target to
// [0].
const std::vector<StringRowSink::Row> concatenatedNodes {{"1, 6, 0"}};

}

TEST_F(CollectConcatenationTest, concatenatesTwoCollectsAcrossAWithBarrier) {
    StringRowSink sink;
    runQuery(barrierQuery, sink);

    EXPECT_EQ(sink.getRows(), concatenatedNodes);
}

TEST_F(CollectConcatenationTest, concatenatesTwoCollectsInTheSameReturn) {
    StringRowSink sink;
    runQuery(returnQuery, sink);

    EXPECT_EQ(sink.getRows(), concatenatedNodes);
}
