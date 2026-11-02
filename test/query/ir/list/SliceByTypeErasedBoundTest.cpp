#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A slice bound held in a type-erased cell slices by the integer the cell holds, reads a null
// as no bound, and raises on a row holding any other value
class SliceByTypeErasedBoundTest : public WriteQueryTest {
};

TEST_F(SliceByTypeErasedBoundTest, slicesFromTheIntegerACellHolds) {
    expectRows("UNWIND [1, 'a'] AS b WITH b WHERE b = 1 RETURN [1, 2, 3][b..]", {{"[2, 3]"}});
}

TEST_F(SliceByTypeErasedBoundTest, slicesToTheIntegerACellHolds) {
    expectRows("UNWIND [1, 'a'] AS b WITH b WHERE b = 1 RETURN [1, 2, 3][..b]", {{"[1]"}});
}

TEST_F(SliceByTypeErasedBoundTest, slicesByAConstantCell) {
    expectRows("RETURN [1, 2, 3][[1, 'a'][0]..]", {{"[2, 3]"}});
}

TEST_F(SliceByTypeErasedBoundTest, unwindsTheRunOfARangeACellBounds) {
    expectRows("UNWIND [1, 'a'] AS b WITH b WHERE b = 1 UNWIND range(0, 3)[b..] AS i RETURN i",
               {{"1"}, {"2"}, {"3"}});
}

TEST_F(SliceByTypeErasedBoundTest, returnsNullForACellHoldingANull) {
    expectRows("UNWIND [null, 1, 'a'] AS b WITH b WHERE b IS NULL RETURN [1, 2, 3][b..]", {{"null"}});
}

TEST_F(SliceByTypeErasedBoundTest, raisesOnACellHoldingAString) {
    expectError("UNWIND [1, 'a'] AS b RETURN [1, 2, 3][b..]",
                "A range or a list slice reads an integer bound, and this row holds a value that is not one");
}

TEST_F(SliceByTypeErasedBoundTest, raisesNothingForASliceNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN [1, 2, 3][[[1], 2][0]..]", {});
}
