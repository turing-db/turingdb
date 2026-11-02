#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A logical operator a WITH projects raises on a type-erased cell holding no boolean, though
// nothing reads it after
class UnreadLogicalRaisesTest : public WriteQueryTest {
};

TEST_F(UnreadLogicalRaisesTest, aConjunctionOfCellsHoldingNoBoolean) {
    expectError("UNWIND [1, 'a'] AS b WITH b, b AND (b = 1) AS z RETURN count(b)", "Logical operands must be booleans");
}

TEST_F(UnreadLogicalRaisesTest, aDisjunctionOfCellsHoldingNoBoolean) {
    expectError("UNWIND [1, 'a'] AS b WITH b, b OR (b = 1) AS z RETURN count(b)", "Logical operands must be booleans");
}

TEST_F(UnreadLogicalRaisesTest, anExclusiveDisjunctionOfCellsHoldingNoBoolean) {
    expectError("UNWIND [1, 'a'] AS b WITH b, b XOR (b = 1) AS z RETURN count(b)", "Logical operands must be booleans");
}

TEST_F(UnreadLogicalRaisesTest, aNegationOfACellHoldingNoBoolean) {
    expectError("UNWIND [1, 'a'] AS b WITH b, NOT b AS z RETURN count(b)", "Logical operands must be booleans");
}

TEST_F(UnreadLogicalRaisesTest, aConjunctionReadAfter) {
    expectError("UNWIND [1, 'a'] AS b WITH b, b AND (b = 1) AS z RETURN count(z)", "Logical operands must be booleans");
}

TEST_F(UnreadLogicalRaisesTest, returnsEveryRowPastAConjunctionOfBooleans) {
    expectRows("UNWIND [true, false] AS b WITH b, b AND (b = true) AS z RETURN count(b)", {{"2"}});
}

TEST_F(UnreadLogicalRaisesTest, returnsEveryRowPastADisjunctionOfBooleans) {
    expectRows("UNWIND [true, false] AS b WITH b, b OR (b = true) AS z RETURN count(b)", {{"2"}});
}
