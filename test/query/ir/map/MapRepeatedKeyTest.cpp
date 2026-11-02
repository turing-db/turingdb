#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A map written with a repeated key evaluates every value it was given, and the map holds
// the last one. Remy is 32.
class MapRepeatedKeyTest : public WriteQueryTest {
};

TEST_F(MapRepeatedKeyTest, raisesOnTheEarlierValueOfAMapLiteral) {
    expectError("RETURN {x: 1 / 0, x: 2} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, raisesOnTheEarlierValueOfAMapLiteralReadingARow) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN {x: n.age / 0, x: n.name} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, raisesOnTheEarlierValueOfAMapLiteralOnSomeRows) {
    expectError("UNWIND [1, 0] AS i RETURN {x: 1 / i, x: i} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, raisesOnTheEarlierValueOfANestedMapLiteral) {
    expectError("RETURN {x: {y: 1 / 0}, x: 2} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, raisesOnTheEarlierValueOfAMapProjection) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN n {x: 1 / 0, x: 2} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, raisesOnTheEarlierValueOfAMapProjectionOfEveryProperty) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN n {.*, x: 1 / 0, x: 2} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, analyzesTheEarlierValueOfAMapLiteral) {
    expectError("RETURN {x: y, x: 2} AS m", "Variable 'y' not found");
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueOfAMapLiteral) {
    expectRows("RETURN {x: 1, y: 3, x: 2}.x, {x: 1, y: 3, x: 2} = {x: 2, y: 3}", {{"2", "true"}});
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueOfAMapLiteralReadingARow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {x: n.age, y: 3, x: n.name} AS m RETURN m.x, m.y, size(keys(m))",
               {{"Remy", "3", "2"}});
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueOfAMapProjection) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n {x: n.name, .age, x: 1} AS m RETURN m.x, m.age, size(keys(m))",
               {{"1", "32", "2"}});
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueBesideAnAggregate) {
    expectRows("MATCH (n:Person) RETURN {x: count(n), x: 1}.x, {x: 1, x: count(n)}.x", {{"1", "8"}});
}

TEST_F(MapRepeatedKeyTest, keepsALaterNull) {
    expectRows("RETURN {x: 1, x: null}.x, size(keys({x: 1, x: null}))", {{"null", "1"}});
}
