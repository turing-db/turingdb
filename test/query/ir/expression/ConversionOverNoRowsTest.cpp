#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A conversion of a constant cell holding a value it has no conversion for, here a list,
// raises on the rows that reach it, so a query no row reaches it in returns no row
class ConversionOverNoRowsTest : public WriteQueryTest {
};

TEST_F(ConversionOverNoRowsTest, raisesNothingForAnIntegerConversionNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN toInteger([[1], 2][0])", {});
}

TEST_F(ConversionOverNoRowsTest, raisesNothingForAFloatConversionNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN toFloat([[1], 2][0])", {});
}

TEST_F(ConversionOverNoRowsTest, raisesNothingForABooleanConversionNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN toBoolean([[1], 2][0])", {});
}

TEST_F(ConversionOverNoRowsTest, raisesNothingForAStringConversionNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN toString([[1], 2][0])", {});
}

TEST_F(ConversionOverNoRowsTest, raisesNothingForAConversionOfAFactorNoProductRowReaches) {
    expectRows("MATCH (a:Person), (b:Person) WHERE b.name = 'Nobody' AND toInteger([[1], a.name][0]) = 1 RETURN count(*)",
               {{"0"}});
}

TEST_F(ConversionOverNoRowsTest, raisesOnAConversionARowReaches) {
    expectError("MATCH (n:Person) WHERE n.name = 'Remy' RETURN toInteger([[1], 2][0])",
                "toInteger() has no conversion for 'List'");
}
