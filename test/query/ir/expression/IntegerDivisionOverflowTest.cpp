#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class IntegerDivisionOverflowTest : public WriteQueryTest {
};

TEST_F(IntegerDivisionOverflowTest, minimumDividedByMinusOneOverflows) {
    expectError("RETURN -9223372036854775808 / -1", "Division overflow.");
}

TEST_F(IntegerDivisionOverflowTest, minimumModuloMinusOneIsZero) {
    expectRows("RETURN -9223372036854775808 % -1", {{"0"}});
}

TEST_F(IntegerDivisionOverflowTest, minimumDividedByAPropertyOfMinusOneOverflows) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN -9223372036854775808 / (n.age - 33)", "Division overflow.");
}

TEST_F(IntegerDivisionOverflowTest, minimumModuloAPropertyOfMinusOneIsZero) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN -9223372036854775808 % (n.age - 33)", {{"0"}});
}

TEST_F(IntegerDivisionOverflowTest, minimumDividedByMinusOneOnOneRowOfManyOverflows) {
    expectError("UNWIND [1, 2, -1] AS x RETURN -9223372036854775808 / x", "Division overflow.");
}

TEST_F(IntegerDivisionOverflowTest, minimumDividedByMinusOneInAFilterOverflows) {
    expectError("UNWIND [-1] AS x WITH x WHERE -9223372036854775808 / x < 0 RETURN x", "Division overflow.");
}

TEST_F(IntegerDivisionOverflowTest, minimumDividedByOtherDivisors) {
    expectRows("UNWIND [1, 2, -2] AS x RETURN -9223372036854775808 / x",
               {{"-9223372036854775808"}, {"-4611686018427387904"}, {"4611686018427387904"}});
}

TEST_F(IntegerDivisionOverflowTest, anyIntegerModuloMinusOneIsZero) {
    expectRows("UNWIND [-9223372036854775808, -7, 0, 7, 9223372036854775807] AS x RETURN x % -1",
               {{"0"}, {"0"}, {"0"}, {"0"}, {"0"}});
}

TEST_F(IntegerDivisionOverflowTest, maximumDividedByMinusOne) {
    expectRows("RETURN 9223372036854775807 / -1", {{"-9223372036854775807"}});
}

TEST_F(IntegerDivisionOverflowTest, minimumPlusOneDividedByMinusOne) {
    expectRows("RETURN -9223372036854775807 / -1", {{"9223372036854775807"}});
}

TEST_F(IntegerDivisionOverflowTest, minimumDividedByMinusOneAsDoubleDoesNotOverflow) {
    expectRows("RETURN -9223372036854775808 / -1.0", {{"9223372036854775808.000000"}});
}

TEST_F(IntegerDivisionOverflowTest, minimumAsDoubleDividedByMinusOneDoesNotOverflow) {
    expectRows("RETURN -9223372036854775808.0 / -1", {{"9223372036854775808.000000"}});
}

TEST_F(IntegerDivisionOverflowTest, divisionByZeroIsStillAnError) {
    expectError("RETURN -9223372036854775808 / 0", "Attempted to divide by zero.");
}

TEST_F(IntegerDivisionOverflowTest, moduloByZeroIsStillAnError) {
    expectError("RETURN -9223372036854775808 % 0", "Attempted modulo by zero.");
}
