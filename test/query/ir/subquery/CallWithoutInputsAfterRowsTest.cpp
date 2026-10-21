#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CALL () run one row at a time over rows in flight that hold only a projected constant
// leaves those rows in flight
class CallWithoutInputsAfterRowsTest : public WriteQueryTest {
};

TEST_F(CallWithoutInputsAfterRowsTest, keepsTheRowsOfAProjectedConstant) {
    expectWriteRows("UNWIND [1, 2, 3] AS x WITH 1 AS one "
                    "CALL () { WITH count(*) AS c CREATE (:T {c: c}) } "
                    "RETURN one",
                    {{"1"}, {"1"}, {"1"}});
}
