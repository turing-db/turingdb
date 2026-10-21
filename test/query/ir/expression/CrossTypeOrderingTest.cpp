#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class CrossTypeOrderingTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.m = {k: 1, s: 'x'}");
        applyWrite("MATCH (p:Person {name: 'Adam'}) SET p.m = {k: 'a'}");
    }
};

TEST_F(CrossTypeOrderingTest, ordersNoValuesOfDifferentTypes) {
    expectRows("RETURN 1 < 'a', 'a' >= 1, true < 1, 1.5 > true, [1] < 'a'",
               {{"null", "null", "null", "null", "null"}});
}

TEST_F(CrossTypeOrderingTest, ordersNoTemporalsOfDifferentKinds) {
    expectRows("RETURN datetime('2020-01-01T00:00:00Z') < duration(5), duration(5) >= 1",
               {{"null", "null"}});
}

TEST_F(CrossTypeOrderingTest, ordersNoMaps) {
    expectRows("RETURN {a: 1} < {a: 2}, {a: 1} <= 1", {{"null", "null"}});
}

TEST_F(CrossTypeOrderingTest, answersNullForAPropertyOfAnotherType) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name < 5, 5 <= n.name", {{"null", "null"}});
}

TEST_F(CrossTypeOrderingTest, filtersOutAPropertyOfAnotherType) {
    expectRows("MATCH (n) WHERE n.name < 5 RETURN n", {});
}

TEST_F(CrossTypeOrderingTest, filtersOutTheNegationOfAPropertyOfAnotherType) {
    expectRows("MATCH (n) WHERE NOT n.name < 5 RETURN n", {});
}

TEST_F(CrossTypeOrderingTest, ordersNoNodes) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n < n, n >= 0", {{"null", "null"}});
}

TEST_F(CrossTypeOrderingTest, ordersNoEdges) {
    expectRows("MATCH ()-[e]->() WHERE e.name = 'Remy -> Adam' RETURN e < e", {{"null"}});
}

TEST_F(CrossTypeOrderingTest, ordersNoCellAgainstAMap) {
    expectRows("UNWIND [1, {k: 2}] AS x RETURN x < {k: 3}", {{"null"}, {"null"}});
}

TEST_F(CrossTypeOrderingTest, ordersAMapValueAgainstANumber) {
    expectRows("UNWIND [{k: 1}, {k: 2.5}, {k: 'a'}, {k: null}, {j: 1}] AS m RETURN m.k, m.k < 2",
               {{"1", "true"},
                {"2.500000", "false"},
                {"a", "null"},
                {"null", "null"},
                {"null", "null"}});
}

TEST_F(CrossTypeOrderingTest, ordersAMapValueOnTheRight) {
    expectRows("UNWIND [{k: 1}, {k: 2.5}, {k: 'a'}] AS m RETURN m.k, 2 > m.k, 2 <= m.k",
               {{"1", "true", "false"},
                {"2.500000", "false", "true"},
                {"a", "null", "null"}});
}

TEST_F(CrossTypeOrderingTest, ordersAMapValueAgainstItsOwnType) {
    expectRows("WITH {s: 'a', b: false, l: [1], d: datetime('2020-01-01T00:00:00Z'), u: duration(5)} AS m "
               "RETURN m.s < 'b', m.b < true, m.l < [2], m.d < datetime('2021-01-01T00:00:00Z'), m.u < duration(10)",
               {{"true", "true", "true", "true", "true"}});
}

TEST_F(CrossTypeOrderingTest, ordersNoMapValueAgainstAnotherType) {
    expectRows("WITH {s: 'a', b: false, l: [1], d: datetime('2020-01-01T00:00:00Z'), u: duration(5)} AS m "
               "RETURN m.s < 1, m.b < 'a', m.l < 1, m.d < duration(10), m.u < datetime('2021-01-01T00:00:00Z')",
               {{"null", "null", "null", "null", "null"}});
}

TEST_F(CrossTypeOrderingTest, ordersNoMapValueAgainstAMap) {
    expectRows("UNWIND [{k: 1}, {k: {j: 1}}] AS m RETURN m.k < {j: 2}", {{"null"}, {"null"}});
}

TEST_F(CrossTypeOrderingTest, ordersTwoMapValues) {
    expectRows("UNWIND [{k: 1}, {k: 'a'}] AS m UNWIND [{k: 2}, {k: 'b'}] AS p RETURN m.k, p.k, m.k < p.k",
               {{"1", "2", "true"},
                {"1", "b", "null"},
                {"a", "2", "null"},
                {"a", "b", "true"}});
}

TEST_F(CrossTypeOrderingTest, ordersAMapValueAgainstACell) {
    expectRows("UNWIND [{k: 1}, {k: 'a'}] AS m UNWIND [2, 'b'] AS x RETURN m.k, x, m.k < x, x > m.k",
               {{"1", "2", "true", "true"},
                {"1", "b", "null", "null"},
                {"a", "2", "null", "null"},
                {"a", "b", "true", "true"}});
}

TEST_F(CrossTypeOrderingTest, comparesAMapValueAgainstACell) {
    expectRows("UNWIND [{k: 1}, {k: 'a'}] AS m UNWIND [1, 'b'] AS x RETURN m.k, x, m.k = x, x <> m.k",
               {{"1", "1", "true", "false"},
                {"1", "b", "false", "true"},
                {"a", "1", "false", "true"},
                {"a", "b", "false", "true"}});
}

TEST_F(CrossTypeOrderingTest, comparesAMapValueAgainstATemporal) {
    expectRows("WITH {d: datetime('2020-01-01T00:00:00Z'), u: duration(5)} AS m "
               "RETURN m.d = datetime('2020-01-01T00:00:00Z'), m.u = duration(5), m.d <> duration(5)",
               {{"true", "true", "true"}});
}

TEST_F(CrossTypeOrderingTest, filtersOutTheMapValuesThatDoNotCompare) {
    expectRows("UNWIND [{k: 1}, {k: 'a'}, {k: null}] AS m WITH m WHERE m.k < 5 RETURN m.k", {{"1"}});
}

TEST_F(CrossTypeOrderingTest, ordersAConstantMapValue) {
    expectRows("WITH {k: 1, j: 'a'} AS m RETURN m.k < 5, m.j < 'b', m.j < 5", {{"true", "true", "null"}});
}

TEST_F(CrossTypeOrderingTest, ordersAStoredMapValue) {
    expectRows("MATCH (p:Person) WHERE p.m IS NOT NULL RETURN p.name, p.m.k < 5, p.m.s < 'y'",
               {{"Adam", "null", "null"},
                {"Remy", "true", "true"}});
}
