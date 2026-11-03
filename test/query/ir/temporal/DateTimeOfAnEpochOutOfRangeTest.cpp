#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DateTimeOfAnEpochOutOfRangeTest : public WriteQueryTest {
};

TEST_F(DateTimeOfAnEpochOutOfRangeTest, readsTheFirstAndTheLastSecondAnInstantCanHold) {
    expectRows("RETURN datetime(-62167219200)", {{"0000-01-01T00:00:00Z"}});
    expectRows("RETURN datetime(253402300799)", {{"9999-12-31T23:59:59Z"}});
}

TEST_F(DateTimeOfAnEpochOutOfRangeTest, raisesForACountOfSecondsNoInstantCanHold) {
    expectError("RETURN datetime(253402300800)",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not 253402300800");
    expectError("RETURN datetime(-62167219201)",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not -62167219201");
    expectError("RETURN datetime(9223372036854775807)",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not 9223372036854775807");
}

TEST_F(DateTimeOfAnEpochOutOfRangeTest, raisesForAnUnwoundCountOfSecondsNoInstantCanHold) {
    expectError("UNWIND [0, 253402300800] AS x RETURN datetime(x)",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not 253402300800");
    expectError("UNWIND [253402300800, 'a'] AS x RETURN datetime(x)",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not 253402300800");
}

TEST_F(DateTimeOfAnEpochOutOfRangeTest, raisesForACountAndAMapOfTheSameEpochAlike) {
    expectError("RETURN datetime({epochSeconds: 253402300800})",
                "The datetime component epochSeconds must be between -62167219200 and 253402300799, not 253402300800");
    expectError("UNWIND [{epochSeconds: 0}, 253402300800] AS x RETURN datetime(x)",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not 253402300800");
}

TEST_F(DateTimeOfAnEpochOutOfRangeTest, computesNoBranchACaseDoesNotTake) {
    expectRows("RETURN CASE WHEN false THEN datetime(253402300800) END", {{"null"}});
    expectRows("UNWIND [253402300800] AS x RETURN CASE WHEN false THEN datetime(x) END", {{"null"}});
}

TEST_F(DateTimeOfAnEpochOutOfRangeTest, raisesForACountNothingReads) {
    expectError("WITH datetime(253402300800) AS d RETURN 1",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not 253402300800");
    expectError("UNWIND [253402300800] AS x WITH datetime(x) AS d RETURN 1",
                "datetime() reads a count of seconds between -62167219200 and 253402300799, not 253402300800");
}

TEST_F(DateTimeOfAnEpochOutOfRangeTest, readsACountFromAColumnHoldingNoNull) {
    expectRows("MATCH (n:Person) WITH count(n) AS c RETURN datetime(c)", {{"1970-01-01T00:00:08Z"}});
}
