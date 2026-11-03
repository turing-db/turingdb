#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A slice a WITH projects raises on a bound held in a type-erased cell holding no integer,
// though nothing reads it after
class UnreadSliceRaisesTest : public WriteQueryTest {
};

TEST_F(UnreadSliceRaisesTest, aSliceFromACellHoldingAString) {
    expectError("UNWIND [1, 'a'] AS b WITH b, [1, 2, 3][b..] AS s RETURN b",
                "A range or a list slice reads an integer bound, not 'String'");
}

TEST_F(UnreadSliceRaisesTest, aSliceToACellHoldingAString) {
    expectError("UNWIND [1, 'a'] AS b WITH b, [1, 2, 3][..b] AS s RETURN b",
                "A range or a list slice reads an integer bound, not 'String'");
}

TEST_F(UnreadSliceRaisesTest, returnsEveryRowPastASliceByIntegerCells) {
    expectRows("UNWIND [1, 'a'] AS b WITH b WHERE b = 1 WITH b, [1, 2, 3][b..] AS s RETURN b", {{"1"}});
}
