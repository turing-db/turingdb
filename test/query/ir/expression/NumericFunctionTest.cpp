#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class NumericFunctionTest : public WriteQueryTest {
};

TEST_F(NumericFunctionTest, absOfIntegerLiterals) {
    expectRows("RETURN abs(-3), abs(3), abs(0)", {{"3", "3", "0"}});
}

TEST_F(NumericFunctionTest, absOfDoubleLiterals) {
    expectRows("RETURN abs(-2.5), abs(2.5)", {{"2.500000", "2.500000"}});
}

TEST_F(NumericFunctionTest, absOfNullIsNull) {
    expectRows("RETURN abs(null)", {{"null"}});
}

TEST_F(NumericFunctionTest, absOnEachRow) {
    expectRows("UNWIND [-4, 2, -7] AS x RETURN abs(x)", {{"4"}, {"2"}, {"7"}});
}

TEST_F(NumericFunctionTest, absOfAnIntegerProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN abs(0 - p.age)", {{"32"}});
}

TEST_F(NumericFunctionTest, absOfAnAbsentPropertyIsNull) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Maxime' RETURN p.name, abs(p.age)",
               {{"Remy", "32"}, {"Maxime", "null"}});
}

TEST_F(NumericFunctionTest, absOfADoubleProperty) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.score = -2.5");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN abs(p.score)", {{"2.500000"}});
}

TEST_F(NumericFunctionTest, absInAFilter) {
    expectRows("UNWIND [-4, 2, -7] AS x WITH x WHERE abs(x) > 3 RETURN x", {{"-4"}, {"-7"}});
}

TEST_F(NumericFunctionTest, signOfIntegersAndDoubles) {
    expectRows("RETURN sign(-3), sign(0), sign(5), sign(-0.5), sign(0.0), sign(2.5)",
               {{"-1", "0", "1", "-1", "0", "1"}});
}

TEST_F(NumericFunctionTest, ceilAndFloor) {
    expectRows("RETURN ceil(0.1), floor(0.9), ceil(-0.1), floor(-0.1), ceil(3), floor(3)",
               {{"1.000000", "0.000000", "-0.000000", "-1.000000", "3.000000", "3.000000"}});
}

TEST_F(NumericFunctionTest, roundsHalfWayValuesUp) {
    expectRows("RETURN round(3.141592), round(2.5), round(-2.5), round(-1.6), round(7)",
               {{"3.000000", "3.000000", "-2.000000", "-2.000000", "7.000000"}});
}

TEST_F(NumericFunctionTest, logarithmsAndExponentials) {
    expectRows("RETURN sqrt(16), exp(0), log(1), log10(1000), log(e())",
               {{"4.000000", "1.000000", "0.000000", "3.000000", "1.000000"}});
}

TEST_F(NumericFunctionTest, trigonometry) {
    expectRows("RETURN sin(0), cos(0), tan(0), asin(1), acos(1), atan(1), cot(pi() / 4), haversin(pi())",
               {{"0.000000", "1.000000", "0.000000", "1.570796", "0.000000", "0.785398", "1.000000", "1.000000"}});
}

TEST_F(NumericFunctionTest, degreesAndRadians) {
    expectRows("RETURN degrees(pi()), radians(180), radians(90.0)",
               {{"180.000000", "3.141593", "1.570796"}});
}

TEST_F(NumericFunctionTest, constants) {
    expectRows("RETURN pi(), e()", {{"3.141593", "2.718282"}});
}

TEST_F(NumericFunctionTest, sqrtOnEachRowOfAProperty) {
    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, sqrt(p.age * 2)",
               {{"Remy", "8.000000"}, {"Adam", "8.000000"}});
}

TEST_F(NumericFunctionTest, nestedCalls) {
    expectRows("UNWIND [-2.4, 2.6] AS x RETURN abs(round(x)), sign(floor(x))",
               {{"2.000000", "-1"}, {"3.000000", "1"}});
}

TEST_F(NumericFunctionTest, aggregateOfAFunction) {
    expectRows("UNWIND [-4, 2, -7] AS x RETURN sum(abs(x))", {{"13"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
