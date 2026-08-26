#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallSkipLimitTest : public CallV3Test {
};

TEST_F(CallSkipLimitTest, limitCutsTheSortedYields) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label ORDER BY label LIMIT 2", sink);

    const std::vector<StringRowSink::Row> expected {{"Bioinformatics"}, {"Exotic"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallSkipLimitTest, skipDropsTheFirstSortedYields) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label ORDER BY label SKIP 7", sink);

    const std::vector<StringRowSink::Row> expected {{"SoftwareEngineering"}, {"Supernatural"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallSkipLimitTest, skipThenLimitCutsAWindowOfYields) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label ORDER BY label SKIP 1 LIMIT 2", sink);

    const std::vector<StringRowSink::Row> expected {{"Exotic"}, {"Founder"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallSkipLimitTest, limitZeroEmptiesTheYields) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label LIMIT 0", sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// The eight Person nodes crossed with the nine labels make 72 rows; the limit keeps five.
TEST_F(CallSkipLimitTest, limitCutsACrossedCall) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) CALL db.labels() YIELD label RETURN n, label LIMIT 5", sink);

    EXPECT_EQ(sink.getRows().size(), 5u);
}
