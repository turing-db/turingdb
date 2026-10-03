#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class TemporalArithmeticTest : public WriteQueryTest {
};

TEST_F(TemporalArithmeticTest, subtractsTwoInstants) {
    expectRows("RETURN datetime('2024-01-02T00:00:00Z') - datetime('2024-01-01T00:00:00Z')", {{"PT24H"}});
    expectRows("RETURN datetime('2024-01-01T00:00:00Z') - datetime('2024-01-02T00:00:00Z')", {{"PT-24H"}});
}

TEST_F(TemporalArithmeticTest, subtractsTheSameInstantWrittenWithTwoOffsets) {
    expectRows("RETURN datetime('2024-01-01T16:05:00+02:00') - datetime('2024-01-01T14:05:00Z')", {{"PT0S"}});
}

TEST_F(TemporalArithmeticTest, readsAComponentOfADifference) {
    expectRows("WITH datetime('2024-01-02T00:00:00Z') - datetime('2024-01-01T00:00:00Z') AS d RETURN d.hours",
               {{"24"}});
}

TEST_F(TemporalArithmeticTest, addsADurationToAnInstant) {
    expectRows("RETURN datetime('2024-01-01T00:00:00Z') + duration(3600000000)", {{"2024-01-01T01:00:00Z"}});
    expectRows("RETURN duration(3600000000) + datetime('2024-01-01T00:00:00Z')", {{"2024-01-01T01:00:00Z"}});
    expectRows("RETURN datetime('2024-01-01T00:00:00Z') + duration(-1000000)", {{"2023-12-31T23:59:59Z"}});
}

TEST_F(TemporalArithmeticTest, subtractsADurationFromAnInstant) {
    expectRows("RETURN datetime('2024-01-01T00:00:00Z') - duration(86400000000)", {{"2023-12-31T00:00:00Z"}});
}

TEST_F(TemporalArithmeticTest, addsTheDifferenceBackToTheInstant) {
    expectRows("WITH datetime('2024-01-01T00:00:00Z') AS a, datetime('2026-09-29T12:34:56Z') AS b "
               "RETURN a + (b - a) = b",
               {{"true"}});
}

TEST_F(TemporalArithmeticTest, computesOverStoredInstants) {
    applyWrite("CREATE (n:Event {name: 'a', at: datetime('2024-01-01T12:00:00Z')})");
    applyWrite("CREATE (n:Event {name: 'b', at: datetime('2024-01-03T00:00:00Z')})");
    applyWrite("CREATE (n:Event {name: 'c'})");

    expectRows("MATCH (n:Event) RETURN n.name, n.at - datetime('2024-01-01T00:00:00Z'), n.at + duration(60000000)",
               {{"a", "PT12H", "2024-01-01T12:01:00Z"},
                {"b", "PT48H", "2024-01-03T00:01:00Z"},
                {"c", "null", "null"}});
}

TEST_F(TemporalArithmeticTest, returnsNullAgainstANull) {
    expectRows("RETURN datetime('2024-01-01T00:00:00Z') - null", {{"null"}});
    expectRows("RETURN datetime('2024-01-01T00:00:00Z') + null", {{"null"}});
}

TEST_F(TemporalArithmeticTest, rejectsOperandsWithNoTemporalMeaning) {
    expectError("RETURN duration(1) - datetime('2024-01-01T00:00:00Z')", "'Duration' and 'DateTime'");
    expectError("RETURN datetime('2024-01-01T00:00:00Z') + datetime('2024-01-01T00:00:00Z')",
                "'DateTime' and 'DateTime'");
    expectError("RETURN datetime('2024-01-01T00:00:00Z') * datetime('2024-01-01T00:00:00Z')",
                "'DateTime' and 'DateTime'");
}

TEST_F(TemporalArithmeticTest, rejectsAnInstantPastTheLastYear) {
    expectError("RETURN datetime('9999-12-31T00:00:00Z') + duration(31556952000000)", "outside");
    expectError("RETURN datetime('2024-01-01T00:00:00Z') + duration(9223372036854775807)", "outside");
    expectError("RETURN datetime('0000-01-01T00:00:00Z') - duration(1)", "outside");
    expectError("RETURN datetime('2024-01-01T00:00:00Z') - duration(-9223372036854775807)", "outside");
}
