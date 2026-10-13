#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class IntegerConversionOfDecimalTextTest : public WriteQueryTest {
};

TEST_F(IntegerConversionOfDecimalTextTest, decimalTextIsTruncated) {
    expectRows("RETURN toInteger('4.7'), toInteger('-4.7'), toInteger('1.7'), toInteger('1e3')", {{"4", "-4", "1", "1000"}});
    expectRows("RETURN toIntegerOrNull('4.7'), toIntegerOrNull('-0.5')", {{"4", "0"}});
}

TEST_F(IntegerConversionOfDecimalTextTest, decimalTextInATaggedCellIsTruncated) {
    expectRows("UNWIND ['4.7', '12', '-2.5'] AS v RETURN toIntegerOrNull(v)", {{"4"}, {"12"}, {"-2"}});
}

TEST_F(IntegerConversionOfDecimalTextTest, textNamingNoIntegerIsNull) {
    expectRows("RETURN toInteger('x'), toInteger('4.7x'), toInteger(''), toInteger('1e400'), toInteger('nan')",
               {{"null", "null", "null", "null", "null"}});
    expectRows("RETURN toIntegerOrNull('9.3e18'), toIntegerOrNull('inf')", {{"null", "null"}});
}

TEST_F(IntegerConversionOfDecimalTextTest, onlyDecimalNotationIsRead) {
    expectRows("RETURN toInteger('0x1F'), toInteger(' 7'), toInteger('7 '), toInteger('infinity'), toIntegerOrNull('0x1F')",
               {{"null", "null", "null", "null", "null"}});
}

TEST_F(IntegerConversionOfDecimalTextTest, integerTextPastTheIntegerRangeIsNull) {
    expectRows("RETURN toInteger('-9223372036854775809'), toInteger('9223372036854775808'), toInteger('-9223372036854775808')",
               {{"null", "null", "-9223372036854775808"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
