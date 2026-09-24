#include <gtest/gtest.h>

#include "CallV3Test.h"

using namespace turing::test;

class RangeSpanOverflowTest : public CallV3Test {
};

TEST_F(RangeSpanOverflowTest, rejectsTheFullInt64RangeCountingUp) {
    runQueryExpectingError("RETURN range(-9223372036854775808, 9223372036854775807)",
                           "range() size exceeds 100000 integers");
}

TEST_F(RangeSpanOverflowTest, rejectsTheFullInt64RangeCountingDown) {
    runQueryExpectingError("RETURN range(9223372036854775807, -9223372036854775808, -1)",
                           "range() size exceeds 100000 integers");
}
