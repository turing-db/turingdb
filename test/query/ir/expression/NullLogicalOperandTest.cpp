#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class NullLogicalOperandTest : public WriteQueryTest {
};

TEST_F(NullLogicalOperandTest, comparesNullAgainstTheNegationOfANull) {
    expectRows("RETURN true = (NOT (1 < 'a')), (NOT (1 < 'a')) = (NOT (2 < 'b')), true = (NOT null)",
               {{"null", "null", "null"}});
}

TEST_F(NullLogicalOperandTest, comparesNullAgainstALogicalOverNulls) {
    expectRows("RETURN true = (null AND null), true = ((1 < 'a') OR (2 < 'b')), true <> ((1 < 'a') XOR null)",
               {{"null", "null", "null"}});
}

TEST_F(NullLogicalOperandTest, filtersOutAnInequalityAgainstALogicalOverNulls) {
    expectRows("MATCH (n:Person) WHERE n.hasPhD <> ((n.name < 5) OR (n.name < 6)) RETURN n", {});
}

TEST_F(NullLogicalOperandTest, takesNoCaseBranchComparingAgainstTheNegationOfANull) {
    expectRows("UNWIND [true, null] AS x RETURN x, CASE x WHEN NOT (1 < 'a') THEN 'y' ELSE 'n' END",
               {{"true", "n"}, {"null", "n"}});
}

TEST_F(NullLogicalOperandTest, answersALogicalOverOneNullFromTheOtherSide) {
    expectRows("RETURN false AND (1 < 'a'), true OR (1 < 'a'), true = (false AND null)",
               {{"false", "true", "false"}});
}
