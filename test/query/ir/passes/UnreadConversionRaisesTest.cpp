#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A conversion a WITH projects raises on a cell holding a value it has no conversion for,
// here a list, though nothing reads it after
class UnreadConversionRaisesTest : public WriteQueryTest {
};

TEST_F(UnreadConversionRaisesTest, anIntegerConversionOfAList) {
    expectError("MATCH (n:Person) WITH n, toInteger([[1], n.name][0]) AS l RETURN n.name",
                "toInteger() has no conversion for 'List'");
}

TEST_F(UnreadConversionRaisesTest, aFloatConversionOfAList) {
    expectError("MATCH (n:Person) WITH n, toFloat([[1], n.name][0]) AS l RETURN n.name",
                "toFloat() has no conversion for 'List'");
}

TEST_F(UnreadConversionRaisesTest, aBooleanConversionOfAList) {
    expectError("MATCH (n:Person) WITH n, toBoolean([[1], n.name][0]) AS l RETURN n.name",
                "toBoolean() has no conversion for 'List'");
}

TEST_F(UnreadConversionRaisesTest, aStringConversionOfAList) {
    expectError("MATCH (n:Person) WITH n, toString([[1], n.name][0]) AS l RETURN n.name",
                "toString() has no conversion for 'List'");
}

TEST_F(UnreadConversionRaisesTest, returnsEveryRowPastAConversionToNull) {
    expectRows("MATCH (n:Person) WITH n, toIntegerOrNull([[1], n.name][0]) AS l RETURN n.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}
