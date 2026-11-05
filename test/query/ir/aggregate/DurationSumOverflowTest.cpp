#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// 60,000,000 days is 5.18e18 microseconds. Two of them sum past INT64_MAX (9.22e18), so
// sum() has no duration to answer and raises as duration + duration does, while their
// average fits. Neo4j 5.26 answers P120000000D and P60000000D.
class DurationSumOverflowTest : public WriteQueryTest {
};

TEST_F(DurationSumOverflowTest, raisesOnASumPastTheDurationRange) {
    expectError("UNWIND [duration({days: 60000000}), duration({days: 60000000})] AS d RETURN sum(d)",
                "Duration overflow");
    expectError("UNWIND [duration({days: -60000000}), duration({days: -60000000})] AS d RETURN sum(d)",
                "Duration overflow");
    expectError("UNWIND range(1, 2) AS i RETURN sum(duration({days: 60000000}))", "Duration overflow");
    expectError("UNWIND [duration({days: 60000000}), duration({days: 60000000})] AS d RETURN 1 AS k, sum(d)",
                "Duration overflow");
    expectError("UNWIND [duration({days: 60000000}), duration({days: 50000000})] AS d RETURN 1 AS k, sum(DISTINCT d)",
                "Duration overflow");
}

TEST_F(DurationSumOverflowTest, raisesOnASumOfCellsPastTheDurationRange) {
    expectError("UNWIND [duration({days: 60000000}), duration({days: 60000000}), 'x'] AS d WITH d LIMIT 2 "
                "RETURN sum(d)",
                "Duration overflow");
    expectError("UNWIND [duration({days: 60000000}), duration({days: 60000000}), 'x'] AS d WITH d LIMIT 2 "
                "RETURN 1 AS k, sum(d)",
                "Duration overflow");
}

TEST_F(DurationSumOverflowTest, averagesDurationsWhoseSumOverflows) {
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000})] AS d RETURN avg(d)",
               {{"PT1440000000H"}});
    expectRows("UNWIND [duration({days: -60000000}), duration({days: -60000000})] AS d RETURN avg(d)",
               {{"PT-1440000000H"}});
    expectRows("UNWIND range(1, 2) AS i RETURN avg(duration({days: 60000000}))", {{"PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000})] AS d RETURN 1 AS k, avg(d)",
               {{"1", "PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 50000000}), duration({days: 50000000})] AS d "
               "RETURN 1 AS k, avg(DISTINCT d)",
               {{"1", "PT1320000000H"}});
}

TEST_F(DurationSumOverflowTest, averagesCellsWhoseSumOverflows) {
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000}), 'x'] AS d WITH d LIMIT 2 "
               "RETURN avg(d)",
               {{"PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 60000000}), 'x'] AS d WITH d LIMIT 2 "
               "RETURN 1 AS k, avg(d)",
               {{"1", "PT1440000000H"}});
    expectRows("UNWIND [duration({days: 60000000}), duration({days: 50000000}), duration({days: 50000000}), 'x'] AS d "
               "WITH d LIMIT 3 RETURN 1 AS k, avg(DISTINCT d)",
               {{"1", "PT1320000000H"}});
}
