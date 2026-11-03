#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// One operand is a constant and the other a chunk a WHERE kept no row of, so the read
// writes no row
class IndexOverAnEmptiedChunkTest : public WriteQueryTest {
};

TEST_F(IndexOverAnEmptiedChunkTest, readsNoRowByAConstantIndexHeldInACell) {
    expectRows("UNWIND [[1, 2], [3]] AS l WITH l, [0, 'a'][0] AS i WHERE size(l) > 5 RETURN l[i]", {});
}

TEST_F(IndexOverAnEmptiedChunkTest, readsNoRowOfAConstantListByAnIndexHeldInACell) {
    expectRows("UNWIND [0, 'a', 1] AS i WITH i WHERE i = 5 RETURN [1, 2][i]", {});
    expectRows("UNWIND [0, 'a', 1] AS i WITH i WHERE i = 1 RETURN [1, 2][i]", {{"2"}});
}

TEST_F(IndexOverAnEmptiedChunkTest, readsNoRowByAConstantKey) {
    expectRows("UNWIND [{a: 1}, {a: 2}] AS m WITH m, toLower('A') AS k WHERE m.a > 5 RETURN m[k]", {});
}

TEST_F(IndexOverAnEmptiedChunkTest, readsNoRowOfAMapLiteralByAKey) {
    expectRows("UNWIND ['a', 'b'] AS k WITH k WHERE k = 'z' RETURN {a: 1}[k]", {});
}
