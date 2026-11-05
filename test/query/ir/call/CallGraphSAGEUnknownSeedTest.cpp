#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// simpledb holds nodes 0 to 17, so 18, 999 and -1 name none. A seed that names no node is
// skipped, as a MATCH on the same ID matches nothing.
class CallGraphSAGEUnknownSeedTest : public CallV3Test {};

TEST_F(CallGraphSAGEUnknownSeedTest, aSeedPastTheLastNodeYieldsNoRows) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([999], [2, 2, 2], 42) YIELD dst_nodes0 "
             "RETURN dst_nodes0, labels(dst_nodes0)",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(CallGraphSAGEUnknownSeedTest, theFirstIDPastTheLastNodeYieldsNoRows) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([18], [2, 2, 2], 42) YIELD dst_nodes0 RETURN dst_nodes0", sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(CallGraphSAGEUnknownSeedTest, aNegativeSeedYieldsNoRows) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([-1], [2, 2, 2], 42) YIELD dst_nodes0 RETURN dst_nodes0", sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(CallGraphSAGEUnknownSeedTest, theSampleIsTheSampleOfTheSeedsThatRemain) {
    StringRowSink known;
    runQuery("CALL gnn.graphSAGE([0], [2, 2, 2], 42) YIELD * RETURN *", known);

    StringRowSink mixed;
    runQuery("CALL gnn.graphSAGE([999, 0, -1, 18], [2, 2, 2], 42) YIELD * RETURN *", mixed);

    ASSERT_FALSE(known.getRows().empty());
    EXPECT_EQ(mixed.getRows(), known.getRows());
}

TEST_F(CallGraphSAGEUnknownSeedTest, eachRowSkipsItsOwnUnknownSeeds) {
    StringRowSink sink;
    runQuery("UNWIND [999, 0, -1, 8, 18] AS id "
             "CALL gnn.graphSAGE([id], [0, 0, 0], 42) YIELD dst_nodes0 "
             "RETURN id, dst_nodes0 ORDER BY id",
             sink);

    const std::vector<StringRowSink::Row> expected {{"0", "0"}, {"8", "8"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallGraphSAGEUnknownSeedTest, answersWhatAMatchOnTheSameIDsAnswers) {
    StringRowSink matched;
    runQuery("UNWIND [-1, 0, 17, 18, 999] AS id MATCH (n) WHERE id(n) = id RETURN n ORDER BY n", matched);

    StringRowSink sampled;
    runQuery("CALL gnn.graphSAGE([-1, 0, 17, 18, 999], [0, 0, 0], 42) YIELD dst_nodes0 "
             "RETURN dst_nodes0 ORDER BY dst_nodes0",
             sampled);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"17"}};
    EXPECT_EQ(matched.getRows(), expected);
    EXPECT_EQ(sampled.getRows(), expected);
}

// Doruk is node 17
TEST_F(CallGraphSAGEUnknownSeedTest, aDeletedNodeIsSkipped) {
    runWrite("MATCH (n {name: 'Doruk'}) DETACH DELETE n");

    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([17, 0], [0, 0, 0], 42) YIELD dst_nodes0 RETURN dst_nodes0", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallGraphSAGEUnknownSeedTest, aNodeTheQueryCreatedIsASeed) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Zoe'}) WITH id(n) AS id "
             "CALL gnn.graphSAGE([id], [0, 0, 0], 42) YIELD dst_nodes0 "
             "RETURN dst_nodes0.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Zoe"}};
    EXPECT_EQ(sink.getRows(), expected);
}
