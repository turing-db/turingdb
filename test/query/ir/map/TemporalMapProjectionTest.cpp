#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A map projection of a datetime or a duration reads the components its selectors name, as
// `d.year` and `d.hours` do. Where the variable's type says so, `.*` and a name no component
// has are rejected at analysis; where a tagged cell holds the value, they raise on its row.
class TemporalMapProjectionTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query, std::string_view reason) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR)
            << "query: " << query << "\nerror: " << status.getError();
        EXPECT_NE(status.getError().find(reason), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRaised(std::string_view query, std::string_view reason) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR)
            << "query: " << query << "\nerror: " << status.getError();
        EXPECT_NE(status.getError().find(reason), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void writeEvents() {
        applyWrite("CREATE (:Event {name: 'a', at: datetime('2026-09-23T14:05:06Z'), took: duration({hours: 2})})");
        applyWrite("CREATE (:Event {name: 'b', at: datetime('2019-01-02T03:04:05Z'), took: duration({minutes: 3})})");
        applyWrite("CREATE (:Event {name: 'c'})");
    }
};

TEST_F(TemporalMapProjectionTest, readsAComponentOfABoundDuration) {
    expectRows("WITH duration({hours: 1}) AS d RETURN d {.hours}", {{"{hours: 1}"}});
}

TEST_F(TemporalMapProjectionTest, readsAComponentOfABoundDatetime) {
    expectRows("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {.year}", {{"{year: 2020}"}});
}

TEST_F(TemporalMapProjectionTest, combinesComponentsWithOtherEntries) {
    expectRows("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {.year, x: 1}", {{"{x: 1, year: 2020}"}});
    expectRows("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {x: 1, .year}", {{"{x: 1, year: 2020}"}});
    expectRows("WITH datetime('2020-03-04T00:00:00Z') AS d, 5 AS x RETURN d {.month, x}", {{"{month: 3, x: 5}"}});
    expectRows("WITH duration({hours: 1}) AS d RETURN d {.hours, .minutes, n: 7}", {{"{hours: 1, minutes: 60, n: 7}"}});
}

TEST_F(TemporalMapProjectionTest, buildsAnEmptyMapWithoutItems) {
    expectRows("WITH duration({hours: 1}) AS d RETURN d {}", {{"{}"}});
}

TEST_F(TemporalMapProjectionTest, letsALaterEntryReplaceAComponent) {
    expectRows("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {.year, year: 0}", {{"{year: 0}"}});
    expectRows("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {year: 0, .year}", {{"{year: 2020}"}});
}

TEST_F(TemporalMapProjectionTest, readsEveryComponentOfADatetime) {
    expectRows("WITH datetime('2026-09-23T14:05:06.123456Z') AS d "
               "RETURN d {.year, .month, .day, .hour, .minute, .second, .millisecond, .microsecond}",
               {{"{day: 23, hour: 14, microsecond: 123456, millisecond: 123, minute: 5, month: 9, second: 6, year: 2026}"}});
}

TEST_F(TemporalMapProjectionTest, readsEveryUnitOfADuration) {
    expectRows("WITH duration(90061123456) AS d "
               "RETURN d {.years, .quarters, .months, .weeks, .days, .hours, .minutes, .seconds, "
               ".milliseconds, .microseconds}",
               {{"{days: 1, hours: 25, microseconds: 90061123456, milliseconds: 90061123, minutes: 1501, "
                 "months: 0, quarters: 0, seconds: 90061, weeks: 0, years: 0}"}});
}

TEST_F(TemporalMapProjectionTest, readsEveryRemainderOfADuration) {
    expectRows("WITH duration(90061123456) AS d "
               "RETURN d {.minutesOfHour, .secondsOfMinute, .millisecondsOfSecond, .microsecondsOfSecond, .daysOfWeek}",
               {{"{daysOfWeek: 1, microsecondsOfSecond: 123456, millisecondsOfSecond: 123, minutesOfHour: 1, "
                 "secondsOfMinute: 1}"}});
    expectRows("WITH duration(45569682000000) AS d RETURN d {.quartersOfYear, .monthsOfYear, .monthsOfQuarter}",
               {{"{monthsOfQuarter: 2, monthsOfYear: 5, quartersOfYear: 1}"}});
}

TEST_F(TemporalMapProjectionTest, readsAComponentOfAStoredDatetime) {
    writeEvents();

    expectRows("MATCH (n:Event) WITH n.name AS name, n.at AS d RETURN name, d {.year, .month}",
               {{"a", "{month: 9, year: 2026}"}, {"b", "{month: 1, year: 2019}"}, {"c", "null"}});
}

TEST_F(TemporalMapProjectionTest, readsAComponentOfAStoredDuration) {
    writeEvents();

    expectRows("MATCH (n:Event) WITH n.name AS name, n.took AS d RETURN name, d {.hours, .minutes}",
               {{"a", "{hours: 2, minutes: 120}"}, {"b", "{hours: 0, minutes: 3}"}, {"c", "null"}});
}

TEST_F(TemporalMapProjectionTest, computesNoEntryOfANullTemporal) {
    writeEvents();

    expectRows("MATCH (n:Event) WHERE n.name = 'c' WITH n, n.at AS d RETURN n.name, d {.year, v: 1 / 0}",
               {{"c", "null"}});
    expectRows("OPTIONAL MATCH (n:Event {name: 'nobody'}) WITH n.took AS d RETURN d {.hours}", {{"null"}});
}

TEST_F(TemporalMapProjectionTest, countsAnEntryBesideAComponentOfAGroupingKey) {
    writeEvents();

    expectRows("MATCH (n:Event) WITH n.at AS d WITH d, d {.year, c: count(*)} AS m RETURN m",
               {{"{c: 1, year: 2026}"}, {"{c: 1, year: 2019}"}, {"null"}});
}

TEST_F(TemporalMapProjectionTest, readsAComponentOfADurationAMapHolds) {
    expectRows("WITH {d: duration({hours: 1})} AS m WITH m.d AS d RETURN d {.hours}", {{"{hours: 1}"}});
    expectRows("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m WITH m.d AS d RETURN d {.year, x: 1}",
               {{"{x: 1, year: 2020}"}});
}

TEST_F(TemporalMapProjectionTest, readsEveryComponentThroughATaggedCell) {
    expectRows("WITH {d: datetime('2026-09-23T14:05:06.123456Z')} AS m WITH m.d AS d "
               "RETURN d {.year, .month, .day, .hour, .minute, .second, .millisecond, .microsecond}",
               {{"{day: 23, hour: 14, microsecond: 123456, millisecond: 123, minute: 5, month: 9, second: 6, year: 2026}"}});
    expectRows("WITH {d: duration(90061123456)} AS m WITH m.d AS d "
               "RETURN d {.years, .quarters, .months, .weeks, .days, .hours, .minutes, .seconds, "
               ".milliseconds, .microseconds, .minutesOfHour, .secondsOfMinute, .millisecondsOfSecond, "
               ".microsecondsOfSecond, .daysOfWeek}",
               {{"{days: 1, daysOfWeek: 1, hours: 25, microseconds: 90061123456, microsecondsOfSecond: 123456, "
                 "milliseconds: 90061123, millisecondsOfSecond: 123, minutes: 1501, minutesOfHour: 1, months: 0, "
                 "quarters: 0, seconds: 90061, secondsOfMinute: 1, weeks: 0, years: 0}"}});
    expectRows("WITH {d: duration(45569682000000)} AS m WITH m.d AS d "
               "RETURN d {.quartersOfYear, .monthsOfYear, .monthsOfQuarter}",
               {{"{monthsOfQuarter: 2, monthsOfYear: 5, quartersOfYear: 1}"}});
}

TEST_F(TemporalMapProjectionTest, readsAComponentOfAListElement) {
    expectRows("WITH [datetime('2020-03-04T00:00:00Z')] AS l WITH l[0] AS d RETURN d {.year}", {{"{year: 2020}"}});
    expectRows("WITH [duration({hours: 1}), 'x'] AS l WITH l[0] AS d RETURN d {.hours}", {{"{hours: 1}"}});
}

TEST_F(TemporalMapProjectionTest, readsAComponentOfAnUnwoundElement) {
    expectRows("UNWIND [duration({hours: 1}), {hours: 2}] AS d RETURN d {.hours}", {{"{hours: 1}"}, {"{hours: 2}"}});
    expectRows("UNWIND [datetime('2020-03-04T00:00:00Z'), null] AS d RETURN d {.year}", {{"{year: 2020}"}, {"null"}});
    expectRows("UNWIND [duration({hours: 1}), duration({hours: 2})] AS d RETURN d {.hours}",
               {{"{hours: 1}"}, {"{hours: 2}"}});
}

TEST_F(TemporalMapProjectionTest, readsAComponentOfAComprehensionElement) {
    expectRows("RETURN [x IN [duration({hours: 1}), duration({hours: 2})] | x {.hours}]",
               {{"[{hours: 1}, {hours: 2}]"}});
}

TEST_F(TemporalMapProjectionTest, rejectsEveryPropertyOfATypedTemporal) {
    expectRejected("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {.*}",
                   "A map projection reads .* off a node, a relationship or a map, and 'd' is 'DateTime'");
    expectRejected("WITH duration({hours: 1}) AS d RETURN d {.*}",
                   "A map projection reads .* off a node, a relationship or a map, and 'd' is 'Duration'");
    expectRejected("WITH duration({hours: 1}) AS d RETURN d {.hours, .*}",
                   "A map projection reads .* off a node, a relationship or a map, and 'd' is 'Duration'");
}

TEST_F(TemporalMapProjectionTest, rejectsEveryPropertyOfAStoredTemporal) {
    writeEvents();

    expectRejected("MATCH (n:Event) WITH n.at AS d RETURN d {.*}",
                   "A map projection reads .* off a node, a relationship or a map, and 'd' is 'DateTime'");
}

TEST_F(TemporalMapProjectionTest, rejectsANameNoComponentOfATypedTemporalHas) {
    expectRejected("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {.nosuch}",
                   "'nosuch' is not a component of a datetime");
    expectRejected("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d {.hours}",
                   "'hours' is not a component of a datetime");
    expectRejected("WITH duration({hours: 1}) AS d RETURN d {.year}",
                   "'year' is not a component of a duration");
}

TEST_F(TemporalMapProjectionTest, rejectsAValueWithNoComponents) {
    expectRejected("WITH 1 AS d RETURN d {.hours}",
                   "A map projection reads a node, a relationship, a map, a datetime, a duration or a point, and 'd' is 'Integer'");
}

TEST_F(TemporalMapProjectionTest, raisesOnEveryPropertyOfATemporalCell) {
    expectRaised("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m WITH m.d AS d RETURN d {.*}",
                 "a node, a relationship or a map");
    expectRaised("UNWIND [duration({hours: 1}), {hours: 2}] AS d RETURN d {.*}",
                 "a node, a relationship or a map");
}

TEST_F(TemporalMapProjectionTest, raisesOnANameNoComponentOfATemporalCellHas) {
    expectRaised("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m WITH m.d AS d RETURN d {.nosuch}",
                 "'nosuch' is not a component of a datetime");
    expectRaised("UNWIND [duration({hours: 1}), {year: 2}] AS d RETURN d {.year}",
                 "'year' is not a component of a duration");
}
