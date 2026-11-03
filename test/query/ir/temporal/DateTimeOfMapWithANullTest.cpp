#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DateTimeOfMapWithANullTest : public WriteQueryTest {
};

TEST_F(DateTimeOfMapWithANullTest, readsANullUnitAsItsFirstValue) {
    expectRows("RETURN datetime({year: 2010, month: 1, day: null})", {{"2010-01-01T00:00:00Z"}});
    expectRows("RETURN datetime({year: 2010, month: null})", {{"2010-01-01T00:00:00Z"}});
    expectRows("RETURN datetime({year: 2010, month: null, day: 31})", {{"2010-01-31T00:00:00Z"}});
    expectRows("RETURN datetime({year: null, month: 2, day: 29})", {{"0000-02-29T00:00:00Z"}});
    expectRows("RETURN datetime({year: 2010, month: 3, day: 4, hour: null, minute: null, second: null, millisecond: null})",
               {{"2010-03-04T00:00:00Z"}});
    expectRows("RETURN datetime({epochSeconds: 0, nanosecond: null})", {{"1970-01-01T00:00:00Z"}});
}

TEST_F(DateTimeOfMapWithANullTest, raisesForANullEpochOrTimezone) {
    expectError("RETURN datetime({epochSeconds: null})", "The datetime component epochSeconds must be an integer");
    expectError("RETURN datetime({epochMillis: null})", "The datetime component epochMillis must be an integer");
    expectError("RETURN datetime({year: 2010, timezone: null})", "The datetime component timezone must be a string");
}

TEST_F(DateTimeOfMapWithANullTest, raisesForAValueOutsideItsUnitBesideANull) {
    expectError("RETURN datetime({year: 2010, month: 13, day: null})",
                "The datetime component month must be between 1 and 12, not 13");
    expectError("RETURN datetime({year: 2010, month: 2, day: 30, hour: null})",
                "The datetime component day must be between 1 and 28, not 30");
    expectError("RETURN datetime({year: 2010, month: null, day: 32})",
                "The datetime component day must be between 1 and 31, not 32");
    expectError("RETURN datetime({year: 2010, month: null, day: 30, hour: 0, minute: 0, second: 0, millisecond: null, microsecond: 1000})",
                "The datetime component microsecond must be between 0 and 999, not 1000");
}

TEST_F(DateTimeOfMapWithANullTest, raisesForAUnitWithoutTheOneAboveItBesideANull) {
    expectError("RETURN datetime({year: 2010, day: null})", "The datetime component day needs the component month");
    expectError("RETURN datetime({month: null})", "The datetime component month needs the component year");
}

TEST_F(DateTimeOfMapWithANullTest, raisesForAnEpochBesideANullCalendarUnit) {
    expectError("RETURN datetime({epochSeconds: 0, year: null})",
                "A datetime of an epoch takes no component but nanosecond and timezone, not year");
}

TEST_F(DateTimeOfMapWithANullTest, raisesForANamedTimezoneBesideANull) {
    expectError("RETURN datetime({year: null, timezone: 'Europe/Paris'})",
                "The timezone 'Europe/Paris' is not an offset such as '+01:00' or 'Z'");
}
