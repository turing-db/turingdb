#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A value read out of a map is a value of a type known only per row, as an element of a
// mixed list is, and goes wherever one can: a CASE or a coalesce, a list, a collect, a
// truth value, a number
class AnyMapValueTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.rows(), expected) << query;
    }
};

TEST_F(AnyMapValueTest, selectsAMapValueInACase) {
    expectRows("WITH {a: 1, c: 'x'} AS m RETURN CASE WHEN true THEN m.a ELSE m.c END", {{"1"}});
    expectRows("UNWIND [{a: 1, c: 'x'}, {c: 'y'}] AS m RETURN CASE WHEN m.a IS NULL THEN m.c ELSE m.a END",
               {{"1"}, {"y"}});
}

TEST_F(AnyMapValueTest, coalescesMapValues) {
    expectRows("UNWIND [{a: 1, c: 'x'}, {c: 'y'}, {}] AS m RETURN coalesce(m.a, m.c)",
               {{"1"}, {"null"}, {"y"}});
}

TEST_F(AnyMapValueTest, mixesAMapValueWithAnotherType) {
    expectRows("UNWIND [{a: 1}, {b: 2}] AS m RETURN coalesce(m.a, 'none')", {{"1"}, {"none"}});
    expectRows("UNWIND [{a: 1}, {b: 2}] AS m RETURN CASE WHEN m.a IS NULL THEN 'none' ELSE m.a END",
               {{"1"}, {"none"}});
}

TEST_F(AnyMapValueTest, listsMapValues) {
    expectRows("WITH {a: 1, c: 'x'} AS m RETURN [m.a, m.c], [m.a, 2], [m.b, 1]",
               {{"[1, x]", "[1, 2]", "[null, 1]"}});
    expectRows("UNWIND [{a: 1}, {a: 'x'}] AS m RETURN [m.a, 0]", {{"[1, 0]"}, {"[x, 0]"}});
}

TEST_F(AnyMapValueTest, collectsMapValues) {
    expectRows("UNWIND [{a: 1}, {a: 'x'}, {b: 2}] AS m RETURN collect(m.a)", {{"[1, x]"}});
}

TEST_F(AnyMapValueTest, readsAMapValueAsATruthValue) {
    expectRows("UNWIND [{f: true}, {f: false}, {g: 1}] AS m RETURN NOT m.f, m.f AND true, m.f OR false",
               {{"false", "true", "true"}, {"null", "null", "null"}, {"true", "false", "false"}});
}

TEST_F(AnyMapValueTest, filtersOnAMapValue) {
    expectRows("UNWIND [{f: true, n: 'a'}, {f: false, n: 'b'}, {n: 'c'}] AS m WITH m WHERE m.f RETURN m.n",
               {{"a"}});
}

TEST_F(AnyMapValueTest, negatesAMapValue) {
    expectRows("UNWIND [{n: 3}, {n: 2.5}, {k: 1}] AS m RETURN -m.n, +m.n",
               {{"-2.500000", "2.500000"}, {"-3", "3"}, {"null", "null"}});
}

TEST_F(AnyMapValueTest, computesOverAMapValue) {
    expectRows("WITH {n: 3, s: 'a'} AS m RETURN m.n + 1, m.n * 2, m.s + 'b'", {{"4", "6", "ab"}});
}

TEST_F(AnyMapValueTest, keepsOneRowPerMapValue) {
    expectRows("UNWIND [{a: 1}, {a: 'x'}, {a: 1}, {a: 1.0}, {b: 2}] AS m RETURN DISTINCT m.a",
               {{"1"}, {"null"}, {"x"}});
}

TEST_F(AnyMapValueTest, ordersByAMapValue) {
    expectOrderedRows("UNWIND [{a: 3}, {a: 'x'}, {b: 1}, {a: true}] AS m RETURN m.a ORDER BY m.a",
                      {{"x"}, {"true"}, {"3"}, {"null"}});
}

TEST_F(AnyMapValueTest, groupsOnAMapValue) {
    expectRows("UNWIND [{a: 1}, {a: 'x'}, {a: 1}, {b: 2}] AS m RETURN m.a, count(*)",
               {{"1", "2"}, {"null", "1"}, {"x", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
