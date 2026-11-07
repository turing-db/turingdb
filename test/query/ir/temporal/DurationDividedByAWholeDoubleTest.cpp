#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationDividedByAWholeDoubleTest : public WriteQueryTest {
};

TEST_F(DurationDividedByAWholeDoubleTest, roundsToTheNearestMicrosecond) {
    expectRows("RETURN duration(3) / 2.0", {{"PT0.000002S"}});
    expectRows("RETURN duration(-3) / 2.0", {{"PT-0.000002S"}});
    expectRows("RETURN duration(5) / 2.0", {{"PT0.000003S"}});
    expectRows("RETURN duration(5) / -2.0", {{"PT-0.000003S"}});
    expectRows("RETURN duration(4) / 3.0", {{"PT0.000001S"}});
    expectRows("RETURN duration(5) / 3.0", {{"PT0.000002S"}});
    expectRows("RETURN duration(7) / 2.0 = duration(7) * 0.5", {{"true"}});
}

TEST_F(DurationDividedByAWholeDoubleTest, keepsEveryMicrosecondOfALargeQuotient) {
    expectRows("RETURN duration(9223372036854775807) / 2.0", {{"PT1281023894H27.387904S"}});
    expectRows("RETURN duration(-9223372036854775807) / 2.0", {{"PT-1281023894H-27.387904S"}});
}
