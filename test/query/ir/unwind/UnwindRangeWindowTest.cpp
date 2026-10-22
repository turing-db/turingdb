#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class UnwindRangeWindowTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectSortedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }
};

TEST_F(UnwindRangeWindowTest, unwindsAReversedRange) {
    expectRows("UNWIND reverse(range(1, 5)) AS x RETURN x", {{"5"}, {"4"}, {"3"}, {"2"}, {"1"}});
}

TEST_F(UnwindRangeWindowTest, unwindsAReversedRangeCountingDown) {
    expectRows("UNWIND reverse(range(10, 1, -3)) AS x RETURN x", {{"1"}, {"4"}, {"7"}, {"10"}});
}

TEST_F(UnwindRangeWindowTest, startsAReversedRangeAtItsLastElement) {
    expectRows("UNWIND reverse(range(1, 10, 4)) AS x RETURN x", {{"9"}, {"5"}, {"1"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForAReversedEmptyRange) {
    expectRows("UNWIND reverse(range(5, 1)) AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForAReversedNullRange) {
    expectRows("UNWIND reverse(range(1, null)) AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsTwiceReversedRangeInOrder) {
    expectRows("UNWIND reverse(reverse(range(1, 3))) AS x RETURN x", {{"1"}, {"2"}, {"3"}});
}

TEST_F(UnwindRangeWindowTest, unwindsASlice) {
    expectRows("UNWIND range(1, 10)[2..5] AS x RETURN x", {{"3"}, {"4"}, {"5"}});
}

TEST_F(UnwindRangeWindowTest, unwindsASliceFromTheStart) {
    expectRows("UNWIND range(1, 10)[..3] AS x RETURN x", {{"1"}, {"2"}, {"3"}});
}

TEST_F(UnwindRangeWindowTest, unwindsASliceToTheEnd) {
    expectRows("UNWIND range(1, 10)[7..] AS x RETURN x", {{"8"}, {"9"}, {"10"}});
}

TEST_F(UnwindRangeWindowTest, countsANegativeLowerBoundFromTheEnd) {
    expectRows("UNWIND range(1, 10)[-3..] AS x RETURN x", {{"8"}, {"9"}, {"10"}});
}

TEST_F(UnwindRangeWindowTest, countsANegativeUpperBoundFromTheEnd) {
    expectRows("UNWIND range(1, 10)[..-8] AS x RETURN x", {{"1"}, {"2"}});
}

TEST_F(UnwindRangeWindowTest, clampsALowerBoundBeforeTheStart) {
    expectRows("UNWIND range(1, 10)[-20..2] AS x RETURN x", {{"1"}, {"2"}});
}

TEST_F(UnwindRangeWindowTest, clampsAnUpperBoundPastTheEnd) {
    expectRows("UNWIND range(1, 10)[5..100] AS x RETURN x", {{"6"}, {"7"}, {"8"}, {"9"}, {"10"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForCrossedBounds) {
    expectRows("UNWIND range(1, 10)[5..2] AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForAnUpperBoundOfZero) {
    expectRows("UNWIND range(1, 10)[0..0] AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForALowerBoundAtTheEnd) {
    expectRows("UNWIND range(1, 10)[10..] AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsTheFirstElementForTheWholeNegativeSpan) {
    expectRows("UNWIND range(1, 10)[-10..1] AS x RETURN x", {{"1"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForANullLowerBound) {
    expectRows("UNWIND range(1, 10)[null..3] AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForANullUpperBound) {
    expectRows("UNWIND range(1, 10)[1..null] AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, slicesASteppedRange) {
    expectRows("UNWIND range(0, 100, 10)[2..4] AS x RETURN x", {{"20"}, {"30"}});
}

TEST_F(UnwindRangeWindowTest, unwindsTheTail) {
    expectRows("UNWIND tail(range(1, 4)) AS x RETURN x", {{"2"}, {"3"}, {"4"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForTheTailOfOneElement) {
    expectRows("UNWIND tail(range(1, 1)) AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsNothingForTheTailOfAnEmptyRange) {
    expectRows("UNWIND tail(range(1, 0)) AS x RETURN count(*)", {{"0"}});
}

TEST_F(UnwindRangeWindowTest, unwindsAReversedSlice) {
    expectRows("UNWIND reverse(range(1, 10)[2..5]) AS x RETURN x", {{"5"}, {"4"}, {"3"}});
}

TEST_F(UnwindRangeWindowTest, unwindsAReversedTail) {
    expectRows("UNWIND reverse(tail(range(1, 4))) AS x RETURN x", {{"4"}, {"3"}, {"2"}});
}

TEST_F(UnwindRangeWindowTest, unwindsASliceOfAReversedRange) {
    expectRows("UNWIND reverse(range(1, 10))[2..5] AS x RETURN x", {{"8"}, {"7"}, {"6"}});
}

TEST_F(UnwindRangeWindowTest, unwindsASliceOfASlice) {
    expectRows("UNWIND range(1, 10)[2..5][1..] AS x RETURN x", {{"4"}, {"5"}});
}

TEST_F(UnwindRangeWindowTest, slicesByTheOuterElement) {
    expectRows("UNWIND range(1, 3) AS i UNWIND range(1, 5)[..i] AS x RETURN i, x",
               {{"1", "1"}, {"2", "1"}, {"2", "2"}, {"3", "1"}, {"3", "2"}, {"3", "3"}});
}

TEST_F(UnwindRangeWindowTest, walksTheWholeInt64RangeBackwardsUpToALimit) {
    expectRows("UNWIND reverse(range(-9223372036854775808, 9223372036854775807)) AS x RETURN x LIMIT 2",
               {{"9223372036854775807"}, {"9223372036854775806"}});
}

TEST_F(UnwindRangeWindowTest, slicesTheEndOfTheWholeInt64Range) {
    expectRows("UNWIND range(-9223372036854775808, 9223372036854775807)[-2..] AS x RETURN x",
               {{"9223372036854775806"}, {"9223372036854775807"}});
}

TEST_F(UnwindRangeWindowTest, slicesTheMiddleOfTheWholeInt64Range) {
    expectRows("UNWIND range(-9223372036854775808, 9223372036854775807)[9223372036854775807..] AS x RETURN x LIMIT 1",
               {{"-1"}});
}

TEST_F(UnwindRangeWindowTest, skipsPastAChunkOfAReversedRange) {
    expectRows("UNWIND reverse(range(1, 5000)) AS x RETURN x SKIP 2048 LIMIT 2", {{"2952"}, {"2951"}});
}

TEST_F(UnwindRangeWindowTest, reversesASliceLongerThanAChunk) {
    expectSortedRows("UNWIND range(1, 3) AS i UNWIND reverse(range(1, 5000)[1000..]) AS x RETURN i, count(x), sum(x)",
                     {{"1", "4000", "12002000"}, {"2", "4000", "12002000"}, {"3", "4000", "12002000"}});
}
