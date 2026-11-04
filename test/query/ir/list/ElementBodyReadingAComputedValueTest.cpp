#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The body of an op over the elements of a list reads a value computed before the op, not one
// of its operands: once over constants, or per row where computing it may raise
class ElementBodyReadingAComputedValueTest : public WriteQueryTest {
};

TEST_F(ElementBodyReadingAComputedValueTest, buildsAComprehensionOfTheValue) {
    expectRows("WITH [1, 2][0] AS i RETURN [l IN [[10, 20], [30, 40]] | i]", {{"[1, 1]"}});
}

TEST_F(ElementBodyReadingAComputedValueTest, indexesEachElementByTheValue) {
    expectRows("WITH [1, 2][0] AS i RETURN [l IN [[10, 20], [30, 40]] | l[i]]", {{"[20, 40]"}});
}

TEST_F(ElementBodyReadingAComputedValueTest, readsTheValueInANestedComprehension) {
    expectRows("WITH [1, 2][0] AS i RETURN [x IN [1, 2] | [y IN [3, 4] | x * y * i]]", {{"[[3, 4], [6, 8]]"}});
}

TEST_F(ElementBodyReadingAComputedValueTest, testsEachElementAgainstTheValue) {
    expectRows("WITH [1, 2][0] AS i RETURN any(x IN [1, 2] WHERE x = i)", {{"true"}});
    expectRows("WITH [1, 2][0] AS i RETURN all(x IN [1, 2] WHERE x = i)", {{"false"}});
}

TEST_F(ElementBodyReadingAComputedValueTest, foldsTheValueIntoAReduce) {
    expectRows("WITH [1, 2][0] AS i RETURN reduce(s = 0, x IN [1, 2] | s + x * i)", {{"3"}});
}

TEST_F(ElementBodyReadingAComputedValueTest, decidesAnAllReduceAgainstTheValue) {
    expectRows("WITH [1, 2][0] AS i RETURN allReduce(s = 0, x IN [1, 2] | s + x, s < i + 2)", {{"false"}});
    expectRows("WITH [1, 2][0] AS i RETURN allReduce(s = 0, x IN [1, 2] | s + x, s < i + 5)", {{"true"}});
}

// [[10], 'a'][0][0] reads an index off a cell that may hold no list, so it is computed on each
// row in flight rather than once
TEST_F(ElementBodyReadingAComputedValueTest, readsAValueComputedPerRow) {
    expectRows("UNWIND [1, 2] AS r WITH r, [[10], 'a'][0][0] AS i RETURN r, [l IN [[10, 20], [30, 40]] | l[0] + i]",
               {{"1", "[20, 40]"}, {"2", "[20, 40]"}});
}

TEST_F(ElementBodyReadingAComputedValueTest, foldsAValueComputedPerRow) {
    expectRows("UNWIND [1, 2] AS r WITH r, [[10], 'a'][0][0] AS i RETURN r, reduce(s = 0, x IN [1, 2] | s + x * i)",
               {{"1", "30"}, {"2", "30"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
