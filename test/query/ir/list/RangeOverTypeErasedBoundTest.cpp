#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class RangeOverTypeErasedBoundTest : public WriteQueryTest {
};

TEST_F(RangeOverTypeErasedBoundTest, countsToTheIntegerAConstantCellHolds) {
    expectRows("WITH [3, 'a'][0] AS b RETURN range(0, b)", {{"[0, 1, 2, 3]"}});
}

TEST_F(RangeOverTypeErasedBoundTest, countsEveryBoundOutOfAConstantCell) {
    expectRows("RETURN range([1, 'a'][0], [7, 'a'][0], [3, 'a'][0])", {{"[1, 4, 7]"}});
}

TEST_F(RangeOverTypeErasedBoundTest, countsFromTheIntegerACellHolds) {
    expectRows("UNWIND [1, 'a'] AS b WITH b WHERE b = 1 RETURN range(b, 3)", {{"[1, 2, 3]"}});
}

TEST_F(RangeOverTypeErasedBoundTest, countsToTheIntegerACellHolds) {
    expectRows("UNWIND [2, 'a'] AS b WITH b WHERE b = 2 RETURN range(0, b)", {{"[0, 1, 2]"}});
}

TEST_F(RangeOverTypeErasedBoundTest, countsByTheIntegerACellHolds) {
    expectRows("UNWIND [2, 'a'] AS s WITH s WHERE s = 2 RETURN range(0, 6, s)", {{"[0, 2, 4, 6]"}});
}

TEST_F(RangeOverTypeErasedBoundTest, countsOneListPerRowOfCells) {
    expectRows("UNWIND [1, 2, 'a'] AS b WITH b WHERE b <> 'a' RETURN b, range(0, b)",
               {{"1", "[0, 1]"}, {"2", "[0, 1, 2]"}});
}

TEST_F(RangeOverTypeErasedBoundTest, unwindsTheRangeACellBounds) {
    expectRows("UNWIND [2, 'a'] AS b WITH b WHERE b = 2 UNWIND range(0, b) AS i RETURN i",
               {{"0"}, {"1"}, {"2"}});
}

TEST_F(RangeOverTypeErasedBoundTest, returnsNullForACellHoldingANull) {
    expectRows("UNWIND [null, 1, 'a'] AS b WITH b WHERE b IS NULL RETURN range(0, b)", {{"null"}});
}

TEST_F(RangeOverTypeErasedBoundTest, raisesOnACellHoldingAString) {
    expectError("UNWIND [1, 'a'] AS b RETURN range(0, b)",
                "A range or a list slice reads an integer bound, not 'String'");
}

TEST_F(RangeOverTypeErasedBoundTest, raisesOnACellHoldingADouble) {
    expectError("UNWIND [1.5, 'a'] AS b RETURN range(0, b)",
                "A range or a list slice reads an integer bound, not 'Double'");
}

TEST_F(RangeOverTypeErasedBoundTest, raisesOnTheUnwoundRangeOfACellHoldingAString) {
    expectError("UNWIND [1, 'a'] AS b UNWIND range(0, b) AS i RETURN i",
                "A range or a list slice reads an integer bound, not 'String'");
}
