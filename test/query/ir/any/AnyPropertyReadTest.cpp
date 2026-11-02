#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class AnyPropertyReadTest : public WriteQueryTest {
protected:
    void expectPropertyError(std::string_view query, std::string_view key) {
        expectError(query, "Type mismatch: property '" + std::string(key) + "' is read off a map");
    }

    void expectKeyError(std::string_view query, std::string_view key) {
        expectError(query, "Type mismatch: key '" + std::string(key) + "' is read off a map");
    }
};

TEST_F(AnyPropertyReadTest, readsAComponentOfADatetimeCell) {
    expectRows("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m WITH m.d AS d RETURN d.year", {{"2020"}});
    expectRows("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m RETURN (m.d).year", {{"2020"}});
}

TEST_F(AnyPropertyReadTest, readsEveryComponentOfADatetimeCell) {
    expectRows("WITH {d: datetime('2026-09-23T14:05:06.123456Z')} AS m WITH m.d AS d "
               "RETURN d.year, d.month, d.day, d.hour, d.minute, d.second, d.millisecond, d.microsecond",
               {{"2026", "9", "23", "14", "5", "6", "123", "123456"}});
}

TEST_F(AnyPropertyReadTest, readsAComponentOfADurationCell) {
    expectRows("WITH {d: duration({hours: 25, minutes: 1, seconds: 1})} AS m WITH m.d AS d "
               "RETURN d.hours, d.minutesOfHour, d.secondsOfMinute",
               {{"25", "1", "1"}});
}

TEST_F(AnyPropertyReadTest, readsAKeyOrAComponentAsEachRowHolds) {
    expectRows("UNWIND [{year: 1}, datetime('2020-03-04T00:00:00Z')] AS x RETURN x.year", {{"1"}, {"2020"}});
}

TEST_F(AnyPropertyReadTest, readsAPropertyOfANodeBesideAKeyOfAMap) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [n, {name: 'Adam'}] AS x RETURN x.name", {{"Remy"}, {"Adam"}});
}

TEST_F(AnyPropertyReadTest, readsNullOffANullCell) {
    expectRows("UNWIND [{a: 1}, null] AS x RETURN x.a", {{"1"}, {"null"}});
}

TEST_F(AnyPropertyReadTest, raisesOnAnIntegerCell) {
    expectPropertyError("UNWIND [{a: 1}, 2] AS x RETURN x.a", "a");
}

TEST_F(AnyPropertyReadTest, raisesOnEveryCellWithNoProperty) {
    for (const std::string value : {"2.5", "true", "'b'", "[1]"}) {
        expectPropertyError("UNWIND [{a: 1}, " + value + "] AS x RETURN x.a", "a");
    }
}

TEST_F(AnyPropertyReadTest, raisesOnANameNoComponentOfADatetimeHas) {
    expectError("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m WITH m.d AS d RETURN d.a",
                "'a' is not a component of a datetime");
}

TEST_F(AnyPropertyReadTest, raisesOnANameNoComponentOfADurationHas) {
    expectError("WITH {d: duration({hours: 1})} AS m WITH m.d AS d RETURN d.year",
                "'year' is not a component of a duration");
}

TEST_F(AnyPropertyReadTest, raisesOnACellNothingReadsTheProperty) {
    expectPropertyError("UNWIND [{a: 1}, 2] AS x WITH x, x.a AS l RETURN x", "a");
}

TEST_F(AnyPropertyReadTest, raisesOnAConstantCell) {
    expectPropertyError("RETURN [{a: 1}, 2][1].a", "a");
}

TEST_F(AnyPropertyReadTest, readsNothingWhereNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN [{a: 1}, 2][1].a", {});
}

TEST_F(AnyPropertyReadTest, readsOnlyTheRowsACaseTakes) {
    expectRows("UNWIND [{a: 1}, 2] AS x RETURN CASE WHEN x = 2 THEN -1 ELSE x.a END", {{"-1"}, {"1"}});
}

TEST_F(AnyPropertyReadTest, raisesOnAProjectionOfAnIntegerCell) {
    expectPropertyError("UNWIND [{a: 1}, 2] AS x RETURN x {.a}", "a");
}

TEST_F(AnyPropertyReadTest, readsNullOffANullCellByAKeyComputedPerRow) {
    expectRows("WITH 'a' AS k UNWIND [{a: 1}, null] AS x RETURN x[k]", {{"1"}, {"null"}});
}

TEST_F(AnyPropertyReadTest, readsAPropertyOfANodeByAKeyComputedPerRow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, 'name' AS k UNWIND [n, {name: 'Adam'}] AS x RETURN x[k]",
               {{"Remy"}, {"Adam"}});
}

TEST_F(AnyPropertyReadTest, raisesOnAnIntegerCellByAKeyComputedPerRow) {
    expectKeyError("WITH 'a' AS k UNWIND [{a: 1}, 2] AS x RETURN x[k]", "a");
}

TEST_F(AnyPropertyReadTest, raisesOnAListCellByAKeyComputedPerRow) {
    expectKeyError("WITH 'a' AS k UNWIND [{a: 1}, [1]] AS x RETURN x[k]", "a");
}

TEST_F(AnyPropertyReadTest, raisesOnADatetimeCellByAKeyComputedPerRow) {
    expectKeyError("WITH 'year' AS k, {d: datetime('2020-03-04T00:00:00Z')} AS m WITH k, m.d AS d RETURN d[k]", "year");
}
