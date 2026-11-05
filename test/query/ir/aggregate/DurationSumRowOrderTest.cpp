#include <gtest/gtest.h>

#include <string>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// 60,000,000 days is 5.18e18 microseconds, so two of them sum past INT64_MAX (9.22e18).
// A sum that passes the range on the way and ends inside it answers its total in every
// row order, as Neo4j 5.26 does (P60000000D, which is PT1440000000H).
class DurationSumRowOrderTest : public WriteQueryTest {
};

TEST_F(DurationSumRowOrderTest, sumsPastTheRangeAndBackInEveryOrder) {
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000}), duration({days: -60000000})] AS d "
               "RETURN sum(d)",
               {{"PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: -60000000}), duration({days: 60000000})] AS d "
               "RETURN sum(d)",
               {{"PT1440000000H"}});
    expectRows("UNWIND [duration({days: -60000000}), duration({days: -60000000}), duration({days: 60000000})] AS d "
               "RETURN sum(d)",
               {{"PT-1440000000H"}});
}

TEST_F(DurationSumRowOrderTest, sumsPastTheRangeAndBackInAGroup) {
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000}), duration({days: -60000000})] AS d "
               "RETURN 1 AS k, sum(d)",
               {{"1", "PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: -60000000}), duration({days: 60000000})] AS d "
               "RETURN 1 AS k, sum(d)",
               {{"1", "PT1440000000H"}});
}

TEST_F(DurationSumRowOrderTest, sumsPastTheRangeAndBackInSeveralGroups) {
    expectRows("UNWIND [[1, 1], [2, 1], [3, -1], [1, 1], [2, -1], [3, -1], [1, -1], [2, 1], [3, 1]] AS p "
               "WITH toInteger(p[0]) AS k, toInteger(p[1]) AS sign "
               "RETURN k, sum(duration({days: 60000000 * sign}))",
               {{"1", "PT1440000000H"},
                {"2", "PT1440000000H"},
                {"3", "PT-1440000000H"}});
}

TEST_F(DurationSumRowOrderTest, sumsDistinctDurationsPastTheRangeAndBack) {
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 50000000}), duration({days: 60000000}), "
               "duration({days: -60000000})] AS d RETURN sum(DISTINCT d)",
               {{"PT1200000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 50000000}), duration({days: 60000000}), "
               "duration({days: -60000000})] AS d RETURN 1 AS k, sum(DISTINCT d)",
               {{"1", "PT1200000000H"}});
}

TEST_F(DurationSumRowOrderTest, sumsCellsPastTheRangeAndBack) {
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000}), duration({days: -60000000}), 'x'] AS d "
               "WITH d LIMIT 3 RETURN sum(d)",
               {{"PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: -60000000}), duration({days: 60000000}), 'x'] AS d "
               "WITH d LIMIT 3 RETURN sum(d)",
               {{"PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000}), duration({days: -60000000}), 'x'] AS d "
               "WITH d LIMIT 3 RETURN 1 AS k, sum(d)",
               {{"1", "PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: -60000000}), duration({days: 60000000}), 'x'] AS d "
               "WITH d LIMIT 3 RETURN 1 AS k, sum(d)",
               {{"1", "PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 50000000}), duration({days: 60000000}), "
               "duration({days: -60000000}), 'x'] AS d WITH d LIMIT 4 RETURN 1 AS k, sum(DISTINCT d)",
               {{"1", "PT1200000000H"}});
}

// The two big values go into the first chunk of 65,536 rows and the one that brings the
// total back into the range into the second: 60M days plus 69,997 seconds.
TEST_F(DurationSumRowOrderTest, sumsPastTheRangeAndBackAcrossChunks) {
    const std::string durations = "CASE WHEN i <= 2 THEN duration({days: 60000000}) "
                                  "WHEN i = 70000 THEN duration({days: -60000000}) "
                                  "ELSE duration({seconds: 1}) END";

    expectRows("UNWIND range(1, 70000) AS i RETURN sum(" + durations + ")", {{"PT1440000019H26M37S"}});
    expectRows("UNWIND range(1, 70000) AS i RETURN 1 AS k, sum(" + durations + ")", {{"1", "PT1440000019H26M37S"}});
}

// The totals run 60M, 120M, 60M, 0, -60M and -60M days less 1 microsecond, so 6 values
// average to -864,000,000,000,000,000.17 microseconds, which truncates toward zero to
// PT-240000000H. The second list runs to 60M days less an hour, 5,183,999,996,400 seconds,
// whose sixth is PT239999999H50M.
TEST_F(DurationSumRowOrderTest, averagesATotalThatCrossesZero) {
    const std::string fallingList = "[duration({days: 60000000}), duration({days: 60000000}), "
                                    "duration({days: -60000000}), duration({days: -60000000}), "
                                    "duration({days: -60000000}), duration({microseconds: -1})]";
    const std::string risingList = "[duration({days: -60000000}), duration({days: -60000000}), "
                                   "duration({days: 60000000}), duration({days: 60000000}), "
                                   "duration({days: 60000000}), duration({hours: -1})]";

    expectRows("UNWIND " + fallingList + " AS d RETURN avg(d)", {{"PT-240000000H"}});
    expectRows("UNWIND " + fallingList + " AS d RETURN 1 AS k, avg(d)", {{"1", "PT-240000000H"}});
    expectRows("UNWIND " + fallingList + " + ['x'] AS d WITH d LIMIT 6 RETURN avg(d)", {{"PT-240000000H"}});
    expectRows("UNWIND " + fallingList + " + ['x'] AS d WITH d LIMIT 6 RETURN 1 AS k, avg(d)",
               {{"1", "PT-240000000H"}});

    expectRows("UNWIND " + risingList + " AS d RETURN avg(d)", {{"PT239999999H50M"}});
    expectRows("UNWIND " + risingList + " AS d RETURN 1 AS k, avg(d)", {{"1", "PT239999999H50M"}});
    expectRows("UNWIND " + risingList + " + ['x'] AS d WITH d LIMIT 6 RETURN avg(d)", {{"PT239999999H50M"}});
    expectRows("UNWIND " + risingList + " + ['x'] AS d WITH d LIMIT 6 RETURN 1 AS k, avg(d)",
               {{"1", "PT239999999H50M"}});
}
