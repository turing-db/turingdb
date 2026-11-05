#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationCellDividendTest : public WriteQueryTest {
};

TEST_F(DurationCellDividendTest, rejectsACellDividedByADuration) {
    expectError("UNWIND [null] AS x RETURN x / duration(1)",
                "Operands are not valid and compatible numeric types: 'Null' and 'Duration'");
    expectError("UNWIND [null, 1, 'x'] AS x RETURN x / duration(1)",
                "Operands are not valid and compatible numeric types: 'Any' and 'Duration'");
    expectError("UNWIND [null, 1, 'x'] AS x RETURN x % duration(1)",
                "Operands are not valid and compatible numeric types: 'Any' and 'Duration'");
}

TEST_F(DurationCellDividendTest, dividesADurationByACell) {
    expectRows("UNWIND [2, 0.5, null, 'x'] AS x WITH x LIMIT 3 RETURN duration(3600000000) / x",
               {{"PT30M"}, {"PT2H"}, {"null"}});
}
