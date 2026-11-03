#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A coalesce nothing reads still computes its arguments over the rows reaching them, so a
// division by zero inside a CASE or a list comprehension it holds raises on such a row
class CoalesceNestedRaiseTest : public WriteQueryTest {
};

TEST_F(CoalesceNestedRaiseTest, raisesInsideACaseItHolds) {
    expectError("UNWIND [0, null] AS x WITH x, coalesce(null, CASE WHEN x = 0 THEN 10 / x END) AS u RETURN count(*)",
                "divide by zero");
    expectError("UNWIND [0, null] AS x WITH x, coalesce(CASE WHEN x = 0 THEN 10 / x END, 1) AS u RETURN count(*)",
                "divide by zero");
}

TEST_F(CoalesceNestedRaiseTest, raisesInsideAListComprehensionItHolds) {
    expectError("UNWIND [0, 1] AS x WITH x, coalesce(null, [y IN [1] | 10 / x]) AS u RETURN count(*)", "divide by zero");
}

TEST_F(CoalesceNestedRaiseTest, countsTheRowsWhereNoRowReachesTheDivision) {
    expectRows("UNWIND [1, null] AS x WITH x, coalesce(null, CASE WHEN x = 1 THEN 10 / x END) AS u RETURN count(*)",
               {{"2"}});
}
