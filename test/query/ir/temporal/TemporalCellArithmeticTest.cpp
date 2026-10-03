#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class TemporalCellArithmeticTest : public WriteQueryTest {
};

TEST_F(TemporalCellArithmeticTest, computesEachCellInItsOwnType) {
    expectRows("UNWIND [duration(1000000), 1] AS x RETURN x + x", {{"PT2S"}, {"2"}});
    expectRows("UNWIND [duration(1000000), 1] AS x RETURN x * 2", {{"PT2S"}, {"2"}});
    expectRows("UNWIND [duration(1000000), 1] AS x RETURN 2 * x", {{"PT2S"}, {"2"}});
}

TEST_F(TemporalCellArithmeticTest, computesTwoCellsInTheirOwnTypes) {
    expectRows("UNWIND [datetime('2024-01-02T00:00:00Z'), 'x'] AS a "
               "UNWIND [datetime('2024-01-01T00:00:00Z'), 'y'] AS b RETURN a - b",
               {{"PT24H"}, {"null"}, {"null"}, {"null"}});
    expectRows("UNWIND [datetime('2024-01-01T00:00:00Z'), 'x'] AS a "
               "UNWIND [duration(3600000000), 'y'] AS b RETURN a + b, b + a",
               {{"2024-01-01T01:00:00Z", "2024-01-01T01:00:00Z"}, {"null", "null"}, {"null", "null"}, {"null", "null"}});
}

TEST_F(TemporalCellArithmeticTest, rejectsCellsWithNoOperatorBetweenThem) {
    expectError("UNWIND [datetime('2024-01-02T00:00:00Z'), 1] AS a "
                "UNWIND [datetime('2024-01-01T00:00:00Z'), 2] AS b RETURN a - b",
                "Operands are not valid and compatible types");
    expectError("UNWIND [datetime('2024-01-01T00:00:00Z'), 1] AS x RETURN x * x",
                "Operands are not valid and compatible types");
}

TEST_F(TemporalCellArithmeticTest, rejectsADurationMinusAnInstant) {
    expectError("UNWIND [duration(1), 'x'] AS x RETURN x - datetime('2024-01-01T00:00:00Z')",
                "A duration minus a datetime is not defined.");
}
