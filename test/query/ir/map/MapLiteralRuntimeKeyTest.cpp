#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class MapLiteralRuntimeKeyTest : public WriteQueryTest {
};

TEST_F(MapLiteralRuntimeKeyTest, readsAMapLiteralByAStringKey) {
    expectRows("WITH 'a' AS k RETURN {a: 1}[k]", {{"1"}});
    expectRows("UNWIND ['a', 'b', 'c'] AS k RETURN {a: 1, b: 'x'}[k]", {{"1"}, {"x"}, {"null"}});
}

TEST_F(MapLiteralRuntimeKeyTest, readsAMapLiteralByAPropertyOfEachRow) {
    expectRows("MATCH (p:Person) RETURN {Remy: 1, Adam: 2}[p.name]",
               {{"1"}, {"2"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});
}

TEST_F(MapLiteralRuntimeKeyTest, readsAMapLiteralBuiltPerRow) {
    expectRows("UNWIND [1, 2] AS x WITH x, 'v' AS k RETURN {v: x}[k]", {{"1"}, {"2"}});
}

TEST_F(MapLiteralRuntimeKeyTest, readsThroughTheValueAMapLiteralHolds) {
    expectRows("WITH 'a' AS k RETURN {a: {b: 2}}[k].b", {{"2"}});
    expectRows("WITH 'a' AS k RETURN {a: [5, 6]}[k][1]", {{"6"}});
}

TEST_F(MapLiteralRuntimeKeyTest, readsNullByANullKey) {
    expectRows("WITH null AS k RETURN {a: 1}[k]", {{"null"}});
}

TEST_F(MapLiteralRuntimeKeyTest, readsAMapLiteralByAKeyACellHolds) {
    expectRows("WITH ['a', 0][0] AS k RETURN {a: 1}[k]", {{"1"}});
    expectError("WITH [0, 'a'][0] AS k RETURN {a: 1}[k]",
                "A list index reads a list, and this row holds a value that is not one");
}
