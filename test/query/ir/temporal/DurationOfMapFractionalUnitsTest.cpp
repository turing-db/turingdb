#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfMapFractionalUnitsTest : public WriteQueryTest {
};

TEST_F(DurationOfMapFractionalUnitsTest, roundsToTheNearestMicrosecond) {
    expectRows("RETURN duration({seconds: 0.000249})", {{"PT0.000249S"}});
}

TEST_F(DurationOfMapFractionalUnitsTest, roundsTheSumOnce) {
    expectRows("RETURN duration({seconds: 0.0000004, milliseconds: 0.0004})", {{"PT0.000001S"}});
    expectRows("RETURN duration({seconds: 0.0000006, milliseconds: 0.0006})", {{"PT0.000001S"}});
    expectRows("RETURN duration({hours: 1, seconds: 0.0000004, milliseconds: 0.0004})", {{"PT1H0.000001S"}});
}
