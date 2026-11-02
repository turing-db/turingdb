#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An unwound value an equality binds reads as itself wherever the rows carry it, though a pass
// reads the equality's other side, of another type, in its place. Remy is node 0, Adam node 1.
class UnwoundValueBoundByAnEqualityTest : public WriteQueryTest {
};

TEST_F(UnwoundValueBoundByAnEqualityTest, readsItInAMapProjection) {
    expectRows("UNWIND [1, 0] AS x MATCH (n) WHERE id(n) = x WITH n {.name, x} AS m RETURN m.name, m.x",
               {{"Remy", "0"}, {"Adam", "1"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, readsItInACaseBranch) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN CASE WHEN i = 0 THEN i ELSE 7 END", {{"0"}, {"7"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, readsItInAComprehension) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN [x IN [1, 2] | x + i]", {{"[1, 2]"}, {"[2, 3]"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, collectsIt) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN collect(i)", {{"[0, 1]"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, groupsOnIt) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN i, count(*)", {{"0", "1"}, {"1", "1"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, deduplicatesIt) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN DISTINCT i", {{"0"}, {"1"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, sortsAndCutsIt) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN i ORDER BY i LIMIT 1", {{"0"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, skipsPastIt) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i WITH i SKIP 1 RETURN count(i)", {{"1"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, limitsIt) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN i LIMIT 2", {{"0"}, {"1"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, crossesIt) {
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i UNWIND [1, 2] AS j RETURN i, j",
               {{"0", "1"}, {"0", "2"}, {"1", "1"}, {"1", "2"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, groupsOnAStringElement) {
    expectRows("UNWIND ['Remy'] AS x MATCH (n:Person) WHERE n.name = x RETURN x, count(*)", {{"Remy", "1"}});
}

TEST_F(UnwoundValueBoundByAnEqualityTest, readsAStringElementInAComprehension) {
    expectRows("UNWIND ['Remy'] AS x MATCH (n:Person) WHERE n.name = x RETURN [y IN [1] | x]", {{"[Remy]"}});
}
