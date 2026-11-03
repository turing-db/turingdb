#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A map written with a repeated key holds the last value under it and never computes an
// earlier one, as Neo4j does. The earlier one is still analyzed. Remy is 32.
class MapRepeatedKeyTest : public WriteQueryTest {
};

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapLiteral) {
    expectRows("RETURN {x: 1 / 0, x: 2} AS m", {{"{x: 2}"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapLiteralReadingAVariable) {
    expectRows("WITH 0 AS z RETURN {x: 1 / z, x: 2} AS m", {{"{x: 2}"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapLiteralOnEveryRow) {
    expectRows("UNWIND [0, 1] AS z RETURN {x: 1 / z, x: z} AS m", {{"{x: 0}"}, {"{x: 1}"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapLiteralReadingARow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN {x: n.age / 0, x: n.name} AS m", {{"{x: Remy}"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfANestedMapLiteral) {
    expectRows("RETURN {x: {y: 1 / 0}, x: 2} AS m", {{"{x: 2}"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapLiteralInAList) {
    expectRows("RETURN [{x: 1 / 0, x: 2}] AS l", {{"[{x: 2}]"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAnUnwoundMapLiteral) {
    expectRows("UNWIND [{x: 1 / 0, x: 2}] AS m RETURN m.x", {{"2"}});
}

TEST_F(MapRepeatedKeyTest, skipsAnEarlierSubquery) {
    expectRows("RETURN {x: COUNT { MATCH (n) WHERE 1 / 0 = 1 RETURN n }, x: 2} AS m", {{"{x: 2}"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapLiteralReadThroughAVariable) {
    expectRows("WITH {x: 1 / 0, x: 2} AS m RETURN m.x", {{"2"}});
}

TEST_F(MapRepeatedKeyTest, analyzesTheEarlierValueOfAMapLiteral) {
    expectError("RETURN {x: y, x: 2} AS m", "Variable 'y' not found");
}

TEST_F(MapRepeatedKeyTest, typesTheEarlierValueOfAMapLiteral) {
    expectError("RETURN {x: true + 1, x: 2} AS m", "Operands are not valid and compatible types for '+'");
}

TEST_F(MapRepeatedKeyTest, aggregatesOverAnEarlierAggregate) {
    expectRows("UNWIND [1, 2] AS i RETURN {x: count(i), x: 2} AS m", {{"{x: 2}"}});
}

TEST_F(MapRepeatedKeyTest, groupsOnTheMapTheLaterValuesMake) {
    expectRows("UNWIND [1, 2] AS i RETURN {x: i, x: 2} AS m, count(*) AS c", {{"{x: 2}", "2"}});
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueOfAMapLiteral) {
    expectRows("RETURN {x: 1, y: 3, x: 2}.x, {x: 1, y: 3, x: 2} = {x: 2, y: 3}", {{"2", "true"}});
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueOfAMapLiteralReadingARow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {x: n.age, y: 3, x: n.name} AS m RETURN m.x, m.y, size(keys(m))",
               {{"Remy", "3", "2"}});
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueBesideAnAggregate) {
    expectRows("MATCH (n:Person) RETURN {x: count(n), x: 1}.x, {x: 1, x: count(n)}.x", {{"1", "8"}});
}

TEST_F(MapRepeatedKeyTest, keepsALaterNull) {
    expectRows("RETURN {x: 1, x: null}.x, size(keys({x: 1, x: null}))", {{"null", "1"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapProjection) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n {x: 1 / 0, x: 2} AS m", {{"{x: 2}"}});
}

TEST_F(MapRepeatedKeyTest, skipsTheEarlierValueOfAMapProjectionOfEveryProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n {.*, x: 1 / 0, x: 2} AS m RETURN m.x, m.name", {{"2", "Remy"}});
}

TEST_F(MapRepeatedKeyTest, skipsAValueWrittenBeforeAPropertyRead) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n {name: 1 / 0, .name} AS m", {{"{name: Remy}"}});
}

TEST_F(MapRepeatedKeyTest, skipsAValueWrittenBeforeAVariable) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, 'Y' AS name RETURN n {name: 1 / 0, name} AS m", {{"{name: Y}"}});
}

TEST_F(MapRepeatedKeyTest, skipsAValueWrittenBeforeAComponentRead) {
    expectRows("WITH duration({hours: 1}) AS d RETURN d {hours: 1 / 0, .hours} AS m", {{"{hours: 1}"}});
}

TEST_F(MapRepeatedKeyTest, readsARepeatedPropertyOnce) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n {.name, .name} AS m", {{"{name: Remy}"}});
}

TEST_F(MapRepeatedKeyTest, computesAValueWrittenAfterAPropertyRead) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN n {.name, name: 1 / 0} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, computesAValueWrittenBeforeEveryProperty) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN n {name: 1 / 0, .*} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, computesAValueUnderAKeyNoPropertyHas) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN n {nosuch: 1 / 0, .*} AS m", "Attempted to divide by zero");
}

TEST_F(MapRepeatedKeyTest, keepsTheLaterValueOfAMapProjection) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n {x: n.name, .age, x: 1} AS m RETURN m.x, m.age, size(keys(m))",
               {{"1", "32", "2"}});
}

TEST_F(MapRepeatedKeyTest, analyzesTheEarlierValueOfAMapProjection) {
    expectError("MATCH (n:Person {name: 'Remy'}) RETURN n {x: y, x: 2} AS m", "Variable 'y' not found");
}
