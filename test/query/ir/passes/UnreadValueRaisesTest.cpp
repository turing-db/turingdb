#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A value a WITH projects raises on a row it cannot be computed for, though nothing reads it
// after, wherever the division sits in it. Remy and Adam have an age and edges out.
class UnreadValueRaisesTest : public WriteQueryTest {
};

TEST_F(UnreadValueRaisesTest, aDivisionByZero) {
    expectError("MATCH (n:Person) WITH n, 1/0 AS l RETURN n.name", "Attempted to divide by zero");
}

TEST_F(UnreadValueRaisesTest, aDivisionByZeroInAListComprehension) {
    expectError("MATCH (n:Person) WITH n, [x IN [0] | 1/x] AS l RETURN n.name", "Attempted to divide by zero");
}

TEST_F(UnreadValueRaisesTest, aDivisionByZeroInAReduce) {
    expectError("MATCH (n:Person) WITH n, reduce(s = 0, x IN [0] | s + 1/x) AS l RETURN n.name",
                "Attempted to divide by zero");
}

TEST_F(UnreadValueRaisesTest, aDivisionByZeroInAListPredicate) {
    expectError("MATCH (n:Person) WITH n, all(x IN [0] WHERE 1/x = 1) AS l RETURN n.name", "Attempted to divide by zero");
}

TEST_F(UnreadValueRaisesTest, aDivisionByZeroInACaseBranchARowTakes) {
    expectError("MATCH (n:Person) WITH n, CASE WHEN n.age > 0 THEN 1/0 END AS l RETURN n.name",
                "Attempted to divide by zero");
}

TEST_F(UnreadValueRaisesTest, aDivisionByZeroInACountSubquery) {
    expectError("MATCH (n:Person) WITH n, COUNT { MATCH (n)-->(m) WHERE 1/0 = 1 } AS l RETURN n.name",
                "Attempted to divide by zero");
}

TEST_F(UnreadValueRaisesTest, aDivisionByZeroInAnExistsSubquery) {
    expectError("MATCH (n:Person) WITH n, EXISTS { MATCH (n)-->(m) WHERE 1/0 = 1 } AS l RETURN n.name",
                "Attempted to divide by zero");
}

TEST_F(UnreadValueRaisesTest, aDivisionByZeroInAPatternComprehension) {
    expectError("MATCH (n:Person) WITH n, [(n)-->(m) | 1/0] AS l RETURN n.name", "Attempted to divide by zero");
}
