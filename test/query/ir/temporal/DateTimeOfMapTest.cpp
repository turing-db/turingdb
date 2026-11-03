#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DateTimeOfMapTest : public WriteQueryTest {
};

TEST_F(DateTimeOfMapTest, readsACalendarDate) {
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25})", {{"2010-11-25T00:00:00Z"}});
}

TEST_F(DateTimeOfMapTest, readsADateAndATime) {
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, hour: 12, minute: 31, second: 14})",
               {{"2010-11-25T12:31:14Z"}});
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, hour: 12, minute: 31, second: 14, millisecond: 645})",
               {{"2010-11-25T12:31:14.645000Z"}});
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, hour: 12, minute: 31, second: 14, microsecond: 645876})",
               {{"2010-11-25T12:31:14.645876Z"}});
}

TEST_F(DateTimeOfMapTest, addsTheSubSecondUnitsAndTruncatesNanoseconds) {
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, hour: 0, minute: 0, second: 14, millisecond: 645, microsecond: 876, nanosecond: 123})",
               {{"2010-11-25T00:00:14.645876Z"}});
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, hour: 0, minute: 0, second: 0, nanosecond: 999999999})",
               {{"2010-11-25T00:00:00.999999Z"}});
}

TEST_F(DateTimeOfMapTest, startsEachMissingUnitAtItsFirstValue) {
    expectRows("RETURN datetime({year: 1984})", {{"1984-01-01T00:00:00Z"}});
    expectRows("RETURN datetime({year: 1984, month: 10})", {{"1984-10-01T00:00:00Z"}});
    expectRows("RETURN datetime({year: 1984, month: 10, day: 11, hour: 12})", {{"1984-10-11T12:00:00Z"}});
}

TEST_F(DateTimeOfMapTest, readsAnEpoch) {
    expectRows("RETURN datetime({epochMillis: 1290673245079})", {{"2010-11-25T08:20:45.079000Z"}});
    expectRows("RETURN datetime({epochSeconds: 1290673245, nanosecond: 79000000})", {{"2010-11-25T08:20:45.079000Z"}});
    expectRows("RETURN datetime({epochSeconds: -1})", {{"1969-12-31T23:59:59Z"}});
}

TEST_F(DateTimeOfMapTest, spendsTheTimezoneOffsetOnTheInstant) {
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, hour: 12, minute: 31, second: 14, timezone: '+01:00'})",
               {{"2010-11-25T11:31:14Z"}});
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, hour: 12, minute: 31, second: 14, timezone: '-05:30'})",
               {{"2010-11-25T18:01:14Z"}});
    expectRows("RETURN datetime({year: 2010, month: 11, day: 25, timezone: 'Z'})", {{"2010-11-25T00:00:00Z"}});
}

TEST_F(DateTimeOfMapTest, readsAMapPerRow) {
    expectRowsInOrder("UNWIND [{year: 2010, month: 1, day: 2}, {epochSeconds: 0}, {year: 2010, month: 1, day: null}] AS m RETURN datetime(m)",
                      {{"2010-01-02T00:00:00Z"}, {"1970-01-01T00:00:00Z"}, {"2010-01-01T00:00:00Z"}});
}

TEST_F(DateTimeOfMapTest, readsACellHoldingAStringOrAnEpoch) {
    expectRowsInOrder("UNWIND ['2010-11-25T00:00:00Z', 0, null] AS x RETURN datetime(x)",
                      {{"2010-11-25T00:00:00Z"}, {"1970-01-01T00:00:00Z"}, {"null"}});
}

TEST_F(DateTimeOfMapTest, rejectsAnUnknownUnit) {
    expectError("RETURN datetime({year: 2010, weekday: 1})", "Unknown datetime component: weekday");
    expectError("RETURN datetime({year: 2010, days: 1})", "Unknown datetime component: days");
}

TEST_F(DateTimeOfMapTest, rejectsAValueOutsideItsUnit) {
    expectError("RETURN datetime({year: 2010, month: 13})", "The datetime component month must be between 1 and 12, not 13");
    expectError("RETURN datetime({year: 2010, month: 2, day: 30})", "The datetime component day must be between 1 and 28, not 30");
    expectError("RETURN datetime({year: 2010, month: 1, day: 1, hour: 24})", "The datetime component hour must be between 0 and 23, not 24");
    expectError("RETURN datetime({year: 2010, month: 1, day: 1, hour: 0, minute: 0, second: 60})", "The datetime component second must be between 0 and 59, not 60");
    expectError("RETURN datetime({year: 2010, month: 1, day: 1, hour: 0, minute: 0, second: 0, millisecond: 645, microsecond: 1000})",
                "The datetime component microsecond must be between 0 and 999, not 1000");
    expectError("RETURN datetime({year: 10000})", "The datetime component year must be between 0 and 9999, not 10000");
}

TEST_F(DateTimeOfMapTest, rejectsAUnitWithoutTheOneAboveIt) {
    expectError("RETURN datetime({year: 2010, day: 3})", "The datetime component day needs the component month");
    expectError("RETURN datetime({month: 3})", "The datetime component month needs the component year");
    expectError("RETURN datetime({year: 2010, month: 3, day: 3, minute: 5})", "The datetime component minute needs the component hour");
    expectError("RETURN datetime({year: 2010, hour: 5})", "The datetime component hour needs the component day");
    expectError("RETURN datetime({year: 2010, month: 3, day: 3, hour: 1, minute: 1, nanosecond: 5})",
                "The datetime component nanosecond needs the component second");
}

TEST_F(DateTimeOfMapTest, rejectsAMapWithNoYearAndNoEpoch) {
    expectError("RETURN datetime({})", "A datetime of a map needs the component year or an epoch");
}

TEST_F(DateTimeOfMapTest, rejectsAnEpochBesideACalendarUnit) {
    expectError("RETURN datetime({epochSeconds: 0, year: 2010})",
                "A datetime of an epoch takes no component but nanosecond and timezone, not year");
    expectError("RETURN datetime({epochSeconds: 0, epochMillis: 0})",
                "A datetime of an epoch takes no component but nanosecond and timezone, not epochSeconds");
}

TEST_F(DateTimeOfMapTest, rejectsAValueOfAnotherType) {
    expectError("RETURN datetime({year: '2010'})", "The datetime component year must be an integer");
    expectError("RETURN datetime({year: 2010, timezone: 1})", "The datetime component timezone must be a string");
}

TEST_F(DateTimeOfMapTest, rejectsANamedTimezone) {
    expectError("RETURN datetime({year: 2010, timezone: 'Europe/Paris'})",
                "The timezone 'Europe/Paris' is not an offset such as '+01:00' or 'Z'");
}

TEST_F(DateTimeOfMapTest, computesNoBranchACaseDoesNotTake) {
    expectRows("UNWIND [{year: 2010, month: 13}] AS m RETURN CASE WHEN false THEN datetime(m) END", {{"null"}});
    expectRows("RETURN CASE WHEN false THEN datetime({year: 2010, month: 13}) END", {{"null"}});
}

TEST_F(DateTimeOfMapTest, raisesForAValueNothingReads) {
    expectError("WITH datetime({year: 2010, month: 13}) AS d RETURN 1",
                "The datetime component month must be between 1 and 12, not 13");
    expectError("UNWIND [{year: 2010, month: 13}] AS m WITH datetime(m) AS d RETURN 1",
                "The datetime component month must be between 1 and 12, not 13");
}
