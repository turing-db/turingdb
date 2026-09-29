#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class TemporalMixedListElementArithmeticTest : public WriteQueryTest {
};

TEST_F(TemporalMixedListElementArithmeticTest, addsAnIndexedDurationToAnInstant) {
    expectRows("WITH [duration(3600000000), 'x'] AS l RETURN datetime('2024-01-01T00:00:00Z') + l[0]",
               {{"2024-01-01T01:00:00Z"}});
    expectRows("WITH [duration(3600000000), 'x'] AS l RETURN l[0] + datetime('2024-01-01T00:00:00Z')",
               {{"2024-01-01T01:00:00Z"}});
}

TEST_F(TemporalMixedListElementArithmeticTest, subtractsAnInstantFromAnIndexedInstant) {
    expectRows("WITH [datetime('2024-01-02T00:00:00Z'), 1] AS l RETURN l[0] - datetime('2024-01-01T00:00:00Z')",
               {{"PT24H"}});
}

TEST_F(TemporalMixedListElementArithmeticTest, subtractsAnInstantFromEachUnwoundElement) {
    expectRows("UNWIND [datetime('2024-01-02T00:00:00Z'), 1] AS t RETURN t - datetime('2024-01-01T00:00:00Z')",
               {{"PT24H"}, {"null"}});
}
