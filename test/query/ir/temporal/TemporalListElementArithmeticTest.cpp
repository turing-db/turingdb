#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class TemporalListElementArithmeticTest : public WriteQueryTest {
};

TEST_F(TemporalListElementArithmeticTest, subtractsAnInstantFromAnUnwoundInstant) {
    expectRows("UNWIND [datetime('2024-01-02T00:00:00Z'), null] AS t "
               "RETURN t - datetime('2024-01-01T00:00:00Z')",
               {{"PT24H"}, {"null"}});
}

TEST_F(TemporalListElementArithmeticTest, subtractsTwoUnwoundInstants) {
    expectRows("UNWIND [datetime('2024-01-01T00:00:00Z')] AS a "
               "UNWIND [datetime('2024-01-02T00:00:00Z')] AS b "
               "RETURN b - a",
               {{"PT24H"}});
}

TEST_F(TemporalListElementArithmeticTest, addsAnUnwoundDurationToAnInstant) {
    expectRows("UNWIND [duration(3600000000), null] AS d RETURN datetime('2024-01-01T00:00:00Z') + d",
               {{"2024-01-01T01:00:00Z"}, {"null"}});
    expectRows("UNWIND [duration(3600000000)] AS d RETURN d + datetime('2024-01-01T00:00:00Z')",
               {{"2024-01-01T01:00:00Z"}});
    expectRows("UNWIND [duration(3600000000)] AS d RETURN datetime('2024-01-01T00:00:00Z') - d",
               {{"2023-12-31T23:00:00Z"}});
}

TEST_F(TemporalListElementArithmeticTest, addsADurationToCollectedInstants) {
    applyWrite("CREATE (n:Event {at: datetime('2024-01-01T12:00:00Z')})");
    applyWrite("CREATE (n:Event {at: datetime('2024-01-03T00:00:00Z')})");

    expectRows("MATCH (n:Event) WITH collect(n.at) AS ts UNWIND ts AS t RETURN t + duration(60000000)",
               {{"2024-01-01T12:01:00Z"}, {"2024-01-03T00:01:00Z"}});
}

TEST_F(TemporalListElementArithmeticTest, rejectsAnUnwoundInstantPastTheLastYear) {
    expectError("UNWIND [datetime('9999-12-31T00:00:00Z')] AS t RETURN t + duration(31556952000000)", "outside");
}
