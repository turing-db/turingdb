#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfMapTest : public WriteQueryTest {
};

TEST_F(DurationOfMapTest, sumsEveryUnitOfTheMap) {
    expectRows("RETURN duration({days: 2, hours: 3})", {{"PT51H"}});
    expectRows("RETURN duration({weeks: 1, days: 1, hours: 1, minutes: 1, seconds: 1})", {{"PT193H1M1S"}});
    expectRows("RETURN duration({seconds: 1, milliseconds: 500, microseconds: 1})", {{"PT1.500001S"}});
}

TEST_F(DurationOfMapTest, readsMonthsAndYearsAsAverageGregorianLengths) {
    expectRows("RETURN duration({years: 100, months: 20, days: 2})", {{"PT891239H42M"}});
    expectRows("WITH duration({months: 1}) AS d RETURN d.months, d.days", {{"1", "30"}});
    expectRows("WITH duration({years: 1}) AS d RETURN d.years, d.months", {{"1", "12"}});
    expectRows("WITH duration({quarters: 1}) AS d RETURN d.months", {{"3"}});
}

TEST_F(DurationOfMapTest, readsFractionalAndNegativeCounts) {
    expectRows("RETURN duration({days: 1.5})", {{"PT36H"}});
    expectRows("RETURN duration({seconds: 1.001}) = duration({seconds: 1}) * 1.001", {{"true"}});
    expectRows("RETURN duration({minutes: -90})", {{"PT-1H-30M"}});
}

TEST_F(DurationOfMapTest, readsAnEmptyMapAsZero) {
    expectRows("RETURN duration({})", {{"PT0S"}});
}

TEST_F(DurationOfMapTest, readsAMapBoundToAVariable) {
    expectRows("WITH {days: 1, hours: 1} AS m RETURN duration(m)", {{"PT25H"}});
}

TEST_F(DurationOfMapTest, returnsNullForANullCount) {
    expectRows("RETURN duration({days: null})", {{"null"}});
}

TEST_F(DurationOfMapTest, rejectsAnUnknownUnit) {
    expectError("RETURN duration({month: 20})", "Unknown duration component: month");
    expectError("RETURN duration({nanoseconds: 1500})", "Unknown duration component: nanoseconds");
    expectError("RETURN duration({monthsOfYear: 1})", "Unknown duration component: monthsOfYear");
}

TEST_F(DurationOfMapTest, rejectsACountThatIsNotANumber) {
    expectError("RETURN duration({days: 'x'})", "number");
}

TEST_F(DurationOfMapTest, rejectsAnOverflow) {
    expectError("RETURN duration({years: 9223372036854775807})", "overflow");
    expectError("RETURN duration({years: 1e30})", "overflow");
}
