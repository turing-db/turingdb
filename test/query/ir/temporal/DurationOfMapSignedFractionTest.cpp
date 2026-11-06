#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfMapSignedFractionTest : public WriteQueryTest {
};

TEST_F(DurationOfMapSignedFractionTest, roundsTheTotalNotItsFraction) {
    expectRows("RETURN duration({seconds: 1, microseconds: -0.5})", {{"PT1S"}});
    expectRows("RETURN duration({seconds: -1, microseconds: 0.5})", {{"PT-1S"}});
    expectRows("RETURN duration({seconds: 1, microseconds: -0.6})", {{"PT0.999999S"}});
    expectRows("RETURN duration({seconds: 1.5, microseconds: -0.5})", {{"PT1.5S"}});
}

TEST_F(DurationOfMapSignedFractionTest, rejectsAUnitOutsideTheRange) {
    expectError("RETURN duration({hours: 0.5, minutes: -1e19, seconds: 6e20})", "overflow");
}
