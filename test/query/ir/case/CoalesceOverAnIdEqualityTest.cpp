#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A coalesce reads an unwound value an equality to a node's ID binds, which a pass reads off
// the node instead. Remy is node 0 and Adam node 1.
class CoalesceOverAnIdEqualityTest : public WriteQueryTest {
};

TEST_F(CoalesceOverAnIdEqualityTest, takesTheBoundValueFirst) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i WITH i RETURN coalesce(i, 5)", {{"0"}, {"1"}});
}

TEST_F(CoalesceOverAnIdEqualityTest, takesTheBoundValueBeforeAProperty) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN coalesce(i, n.age)", {{"0"}, {"1"}});
}

TEST_F(CoalesceOverAnIdEqualityTest, takesAConstantBeforeTheBoundValue) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN coalesce(5, i)", {{"5"}, {"5"}});
}
