#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class StrictConversionOfACellTest : public WriteQueryTest {
};

TEST_F(StrictConversionOfACellTest, toIntegerOfACell) {
    expectRows("UNWIND ['12', 3, 2.7, true, 'x', null] AS v RETURN toInteger(v)",
               {{"12"}, {"3"}, {"2"}, {"1"}, {"null"}, {"null"}});
}

TEST_F(StrictConversionOfACellTest, toFloatOfACell) {
    expectRows("UNWIND ['1.5', 2, 2.5, 'x', null] AS v RETURN toFloat(v)",
               {{"1.500000"}, {"2.000000"}, {"2.500000"}, {"null"}, {"null"}});
}

TEST_F(StrictConversionOfACellTest, toBooleanOfACell) {
    expectRows("UNWIND ['true', 0, 7, true, 'x', null] AS v RETURN toBoolean(v)",
               {{"true"}, {"false"}, {"true"}, {"true"}, {"null"}, {"null"}});
}

TEST_F(StrictConversionOfACellTest, toStringOfACell) {
    expectRows("UNWIND [1, 'a', 2.5, true, datetime('2020-01-02T03:04:05Z'), duration(90000000), null] AS v RETURN toString(v)",
               {{"1"}, {"a"}, {"2.5"}, {"true"}, {"2020-01-02T03:04:05Z"}, {"PT1M30S"}, {"null"}});
}

TEST_F(StrictConversionOfACellTest, aCellNoRuleConvertsIsAnError) {
    expectError("UNWIND [[1], 'a'] AS v RETURN toInteger(v)", "toInteger()");
    expectError("UNWIND [true, 'a'] AS v RETURN toFloat(v)", "toFloat()");
    expectError("UNWIND [1.5, 'a'] AS v RETURN toBoolean(v)", "toBoolean()");
    expectError("UNWIND [[1], 'a'] AS v RETURN toString(v)", "toString()");
}

TEST_F(StrictConversionOfACellTest, theOrNullConversionsAnswerNullWhereTheStrictOnesRaise) {
    expectRows("UNWIND [[1], true, 1.5] AS v RETURN toIntegerOrNull(v), toFloatOrNull(v), toBooleanOrNull(v), toStringOrNull(v)",
               {{"null", "null", "null", "null"}, {"1", "null", "true", "true"}, {"1", "1.500000", "null", "1.5"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
