#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationScaledByADoubleTest : public WriteQueryTest {
};

TEST_F(DurationScaledByADoubleTest, keepsEveryMicrosecondOfAWholeFactor) {
    expectRows("RETURN duration(9223372036854775807) * 1.0", {{"PT2562047788H54.775807S"}});
    expectRows("RETURN duration(9007199254740993) * 1.0", {{"PT2501999H47M34.740993S"}});
    expectRows("RETURN duration(9007199254740993) / 1.0", {{"PT2501999H47M34.740993S"}});
    expectRows("RETURN duration(9007199254740993) * -1.0", {{"PT-2501999H-47M-34.740993S"}});
}

TEST_F(DurationScaledByADoubleTest, rejectsAFactorThatIsNotANumber) {
    expectError("RETURN duration(1) * toFloat('NaN')", "NaN");
    expectError("RETURN duration(1) / toFloat('NaN')", "NaN");
}

TEST_F(DurationScaledByADoubleTest, rejectsAProductThatIsNotANumber) {
    expectError("RETURN duration(0) * toFloat('Infinity')", "NaN");
    expectError("RETURN duration(1) * toFloat('Infinity')", "overflow");
}
