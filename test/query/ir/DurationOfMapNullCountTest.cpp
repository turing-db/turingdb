#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfMapNullCountTest : public WriteQueryTest {
};

TEST_F(DurationOfMapNullCountTest, stillRejectsAnUnknownUnitSortedAfterTheNull) {
    expectError("RETURN duration({days: null, month: 1})", "Unknown duration component: month");
}

TEST_F(DurationOfMapNullCountTest, stillRejectsACountSortedAfterTheNullThatIsNotANumber) {
    expectError("RETURN duration({days: null, hours: 'x'})", "number");
}

TEST_F(DurationOfMapNullCountTest, returnsNullWhenEveryOtherEntryIsValid) {
    expectRows("RETURN duration({days: null, hours: 1})", {{"null"}});
}
