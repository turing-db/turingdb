#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// FOREACH over a value that is not a list runs its updates once, over that value, as
// UNWIND does and as Neo4j 5 does for a literal and a computed value alike. Over null it
// runs no update.
//
// simpledb holds no Tag node.
class ForeachNonListTest : public WriteQueryTest {
};

TEST_F(ForeachNonListTest, runsOnceOverAScalarLiteral) {
    applyWrite("FOREACH (x IN 5 | CREATE (:Tag {v: x}))");
    applyWrite("FOREACH (x IN 's' | CREATE (:Tag {s: x}))");
    applyWrite("FOREACH (x IN true | CREATE (:Tag {b: x}))");

    expectRows("MATCH (t:Tag) RETURN t.v, t.s, t.b", {{"5", "null", "null"}, {"null", "s", "null"}, {"null", "null", "true"}});
}

TEST_F(ForeachNonListTest, runsOnceOverAMapLiteral) {
    applyWrite("FOREACH (x IN {a: 1} | CREATE (:Tag {v: x.a}))");

    expectRows("MATCH (t:Tag) RETURN t.v", {{"1"}});
}

TEST_F(ForeachNonListTest, runsOnceOverAComputedValue) {
    applyWrite("WITH 5 AS y FOREACH (x IN y | CREATE (:Tag {v: x}))");
    applyWrite("FOREACH (x IN 2 + 4 | CREATE (:Tag {v: x}))");

    expectRows("MATCH (t:Tag) RETURN t.v", {{"5"}, {"6"}});
}

TEST_F(ForeachNonListTest, runsOncePerRowOverAPropertyOfTheRow) {
    applyWrite("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] FOREACH (x IN p.name | CREATE (:Tag {v: x}))");

    expectRows("MATCH (t:Tag) RETURN t.v", {{"Adam"}, {"Remy"}});
}

TEST_F(ForeachNonListTest, runsNoUpdateOverNull) {
    applyWrite("FOREACH (x IN null | CREATE (:Tag {v: x}))");

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"0"}});
}
