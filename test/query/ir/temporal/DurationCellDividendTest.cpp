#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationCellDividendTest : public WriteQueryTest {
};

TEST_F(DurationCellDividendTest, dividesACellByADuration) {
    expectRows("UNWIND [null] AS x RETURN x / duration(1)", {{"null"}});
    expectError("UNWIND [null, 'x', true] AS x RETURN x / duration(1)",
                "Operands are not valid and compatible numeric types: 'String' and 'Duration'");
    expectError("UNWIND [null, 1, 'x'] AS x RETURN x / duration(1)",
                "Operands are not valid and compatible numeric types: 'Integer' and 'Duration'");
}
