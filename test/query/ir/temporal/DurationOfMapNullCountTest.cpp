#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A null count is a count that is not a number, as in Neo4j 5.26, which answers
// "hours must be a number value, but was a NoValue". An unknown unit is reported first.
class DurationOfMapNullCountTest : public WriteQueryTest {
};

TEST_F(DurationOfMapNullCountTest, stillRejectsAnUnknownUnitSortedAfterTheNull) {
    expectError("RETURN duration({days: null, month: 1})", "Unknown duration component: month");
    expectError("RETURN duration({days: 'x', month: 1})", "Unknown duration component: month");
}

TEST_F(DurationOfMapNullCountTest, stillRejectsACountSortedAfterTheNullThatIsNotANumber) {
    expectError("RETURN duration({days: null, hours: 'x'})", "number");
}

TEST_F(DurationOfMapNullCountTest, rejectsANullCount) {
    expectError("RETURN duration({hours: null})", "duration() reads a number for the unit 'hours', not 'Null'");
    expectError("RETURN duration({days: null, hours: 1})", "duration() reads a number for the unit 'days', not 'Null'");
    expectError("RETURN duration({hours: 1, minutes: null})", "duration() reads a number for the unit 'minutes', not 'Null'");
    expectError("UNWIND [{hours: 1}, {hours: null}] AS m RETURN duration(m)",
                "duration() reads a number for the unit 'hours', not 'Null'");
}

TEST_F(DurationOfMapNullCountTest, answersNullForANullMap) {
    expectRows("RETURN duration(null)", {{"null"}});
}

TEST_F(DurationOfMapNullCountTest, takesANullCountOnlyInABranchACaseSkips) {
    expectRows("UNWIND [480, null] AS minutes "
               "RETURN CASE WHEN minutes IS NULL THEN null "
               "ELSE datetime('2026-11-04T00:00:00Z') + duration({minutes: minutes}) END",
               {{"2026-11-04T08:00:00Z"}, {"null"}});
}
