#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyMapValueYieldedTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyMapValueYieldedTest, testsAMapValueInACaseCondition) {
    expectRows("UNWIND [{f: true, n: 'a'}, {f: false, n: 'b'}, {n: 'c'}] AS m "
               "RETURN m.n, CASE WHEN m.f THEN 'y' ELSE 'n' END",
               {{"a", "y"}, {"b", "n"}, {"c", "n"}});
}

TEST_F(AnyMapValueYieldedTest, testsAMapValueInAListPredicate) {
    expectRows("UNWIND [{f: true, n: 'a'}, {f: false, n: 'b'}, {n: 'c'}] AS m "
               "RETURN m.n, any(x IN [1] WHERE m.f), all(x IN [1] WHERE m.f), none(x IN [1] WHERE m.f)",
               {{"a", "true", "true", "false"}, {"b", "false", "false", "true"}, {"c", "null", "null", "null"}});
}

TEST_F(AnyMapValueYieldedTest, filtersAComprehensionOnAMapValue) {
    expectRows("UNWIND [{f: true, n: 'a'}, {f: false, n: 'b'}, {n: 'c'}] AS m "
               "RETURN m.n, [x IN [1, 2] WHERE m.f | x]",
               {{"a", "[1, 2]"}, {"b", "[]"}, {"c", "[]"}});
}

TEST_F(AnyMapValueYieldedTest, projectsAMapValueInAComprehension) {
    expectRows("UNWIND [{a: 1}, {a: 'x'}, {b: 2}] AS m RETURN [x IN [1] | m.a]",
               {{"[1]"}, {"[null]"}, {"[x]"}});
}

TEST_F(AnyMapValueYieldedTest, reducesToAMapValue) {
    expectRows("UNWIND [{a: 1}, {a: 'x'}, {b: 2}] AS m RETURN reduce(acc = 0, x IN [1] | m.a)",
               {{"1"}, {"null"}, {"x"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
