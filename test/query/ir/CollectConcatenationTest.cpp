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

// Remy(0) cites Adam(1), Ghosts(6), Computers(2) and Eighties(3). A fan-out of 8 exceeds
// his out-degree, so the sample takes the whole neighbourhood: the seed collects to [0]
// and the targets to [1, 6, 2, 3].
const std::vector<StringRowSink::Row> concatenatedNodes {{"0, 1, 6, 2, 3"}};

}

TEST_F(CollectConcatenationTest, concatenatesTwoCollectsAcrossAWithBarrier) {
    StringRowSink sink;
    runQuery(barrierQuery, sink);

    EXPECT_EQ(sink.getRows(), concatenatedNodes);
}

// The same two collects concatenated in the RETURN itself, which the engine rejects today
// with "db operands to deeperBlock must be bound in the same loop".
TEST_F(CollectConcatenationTest, concatenatesTwoCollectsInTheSameReturn) {
    StringRowSink sink;
    runQuery(returnQuery, sink);

    EXPECT_EQ(sink.getRows(), concatenatedNodes);
}
