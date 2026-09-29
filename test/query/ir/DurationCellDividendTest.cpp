#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationCellDividendTest : public WriteQueryTest {
};

TEST_F(DurationCellDividendTest, dividesACellByADuration) {
    expectRows("UNWIND [null] AS x RETURN x / duration(1)", {{"null"}});
    expectRows("UNWIND [null, 'x', true] AS x RETURN x / duration(1)", {{"null"}, {"null"}, {"null"}});
    expectError("UNWIND [null, 'x', 1] AS x RETURN x / duration(1)", "A number divided by a duration is not defined.");
}
