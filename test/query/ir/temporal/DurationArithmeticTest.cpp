#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationArithmeticTest : public WriteQueryTest {
};

TEST_F(DurationArithmeticTest, addsAndSubtractsDurations) {
    expectRows("RETURN duration(3600000000) + duration(60000000)", {{"PT1H1M"}});
    expectRows("RETURN duration(3600000000) - duration(60000000)", {{"PT59M"}});
    expectRows("RETURN duration(60000000) - duration(3600000000)", {{"PT-59M"}});
}

TEST_F(DurationArithmeticTest, multipliesADurationByANumber) {
    expectRows("RETURN duration(3600000000) * 2", {{"PT2H"}});
    expectRows("RETURN 2 * duration(3600000000)", {{"PT2H"}});
    expectRows("RETURN duration(3600000000) * 0.5", {{"PT30M"}});
    expectRows("RETURN 0.5 * duration(3600000000)", {{"PT30M"}});
    expectRows("RETURN duration(3600000000) * -1", {{"PT-1H"}});
}

TEST_F(DurationArithmeticTest, dividesADurationByANumber) {
    expectRows("RETURN duration(3600000000) / 2", {{"PT30M"}});
    expectRows("RETURN duration(3600000000) / 0.5", {{"PT2H"}});
}

TEST_F(DurationArithmeticTest, truncatesTowardZero) {
    expectRows("RETURN duration(3) / 2", {{"PT0.000001S"}});
    expectRows("RETURN duration(3) * 0.5", {{"PT0.000001S"}});
    expectRows("RETURN duration(-3) / 2", {{"PT-0.000001S"}});
}

TEST_F(DurationArithmeticTest, negatesADuration) {
    expectRows("RETURN -duration(3600000000)", {{"PT-1H"}});
    expectRows("WITH duration(3600000000) AS d RETURN -(-d) = d", {{"true"}});
}

TEST_F(DurationArithmeticTest, readsAComponentOfASum) {
    expectRows("WITH duration(3600000000) + duration(120000000) AS d RETURN d.minutes", {{"62"}});
}

TEST_F(DurationArithmeticTest, computesOverStoredDurations) {
    applyWrite("CREATE (n:Trip {name: 'a', d: duration(3600000000)})");
    applyWrite("CREATE (n:Trip {name: 'b'})");

    expectRows("MATCH (n:Trip) RETURN n.name, n.d * 2, n.d + duration(60000000), -n.d",
               {{"a", "PT2H", "PT1H1M", "PT-1H"},
                {"b", "null", "null", "null"}});
}

TEST_F(DurationArithmeticTest, computesOverTaggedCells) {
    expectRows("UNWIND [duration(3600000000), null] AS d RETURN d * 2", {{"PT2H"}, {"null"}});
    expectRows("UNWIND [duration(3600000000)] AS d RETURN d / 2, 2 * d, d - duration(60000000)",
               {{"PT30M", "PT2H", "PT59M"}});
    expectRows("WITH [duration(3600000000), 'x'] AS l RETURN l[0] + duration(60000000)", {{"PT1H1M"}});
    expectRows("UNWIND [2, 3] AS k RETURN duration(3600000000) * k", {{"PT2H"}, {"PT3H"}});
}

TEST_F(DurationArithmeticTest, returnsNullAgainstANull) {
    expectRows("RETURN duration(1) + null", {{"null"}});
    expectRows("RETURN duration(1) * null", {{"null"}});
}

TEST_F(DurationArithmeticTest, rejectsDivisionByZero) {
    expectError("RETURN duration(1) / 0", "divide by zero");
    expectError("RETURN duration(1) / 0.0", "divide by zero");
}

TEST_F(DurationArithmeticTest, rejectsOperandsWithNoDurationMeaning) {
    expectError("RETURN 2 / duration(1)", "'Integer' and 'Duration'");
    expectError("RETURN duration(1) / duration(1)", "'Duration' and 'Duration'");
    expectError("RETURN duration(1) * duration(1)", "'Duration' and 'Duration'");
    expectError("RETURN duration(1) % 2", "'Duration' and 'Integer'");
}

TEST_F(DurationArithmeticTest, rejectsAnOverflow) {
    expectError("RETURN duration(9223372036854775807) + duration(1)", "overflow");
    expectError("RETURN duration(-9223372036854775807) - duration(2)", "overflow");
    expectError("RETURN duration(9223372036854775807) * 2", "overflow");
    expectError("RETURN duration(9223372036854775807) * 2.0", "overflow");
}
