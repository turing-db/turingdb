#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CREATE or a MERGE whose step holds no row writes nothing, so a value held in a
// type-erased cell, even a constant one, gives the property it names no type there.
class CellWriteOfNoRowTest : public WriteQueryTest {
};

TEST_F(CellWriteOfNoRowTest, createsNoTypeFromAStepOfNoRow) {
    applyWrite("UNWIND [1, 2] AS i WITH i WHERE i > 5 CREATE (:T {v: [1, 'a'][0]})");
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.v = 'x'");
    expectRows("MATCH (n) WHERE n.v IS NOT NULL RETURN n.v", {{"x"}});
}

TEST_F(CellWriteOfNoRowTest, mergesNoTypeFromAStepOfNoRow) {
    applyWrite("UNWIND [1, 2] AS i WITH i WHERE i > 5 MERGE (:T {v: [i, 'a'][0], w: [1, 'a'][0]})");
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.w = 'x'");
    expectRows("MATCH (n) WHERE n.w IS NOT NULL RETURN n.w", {{"x"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
