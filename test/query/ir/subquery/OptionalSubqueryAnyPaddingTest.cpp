#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class OptionalSubqueryAnyPaddingTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(OptionalSubqueryAnyPaddingTest, padsAMixedValueWithNull) {
    expectRows("UNWIND [1, 'a'] AS x OPTIONAL CALL (x) { MATCH (k:Nobody) RETURN x AS v } RETURN v",
               {{"null"}, {"null"}});
}

TEST_F(OptionalSubqueryAnyPaddingTest, padsTheMixedValuesOfTheRowsNoPersonMatches) {
    expectRows("UNWIND [1, 'Remy'] AS x OPTIONAL CALL (x) { MATCH (p:Person) WHERE p.name = x RETURN x AS v } RETURN x, v",
               {{"1", "null"}, {"Remy", "Remy"}});
}

TEST_F(OptionalSubqueryAnyPaddingTest, padsAMapValueWithNull) {
    expectRows("UNWIND [{a: 1}] AS m OPTIONAL CALL (m) { MATCH (k:Nobody) RETURN m.a AS v } RETURN v",
               {{"null"}});
}

// Luc is matched and reads null out of a map without the key; Bob is matched by nobody
TEST_F(OptionalSubqueryAnyPaddingTest, padsTheMapValuesOfTheRowsNoPersonMatches) {
    expectRows("UNWIND [{n: 'Remy', a: 1}, {n: 'Adam', a: 'x'}, {n: 'Luc'}, {n: 'Bob', a: 2}] AS m "
               "OPTIONAL CALL (m) { MATCH (p:Person) WHERE p.name = m.n RETURN m.a AS v } "
               "RETURN m.n, v",
               {{"Adam", "x"}, {"Bob", "null"}, {"Luc", "null"}, {"Remy", "1"}});
}

TEST_F(OptionalSubqueryAnyPaddingTest, padsTheMapValuesOfABodyRunPerRow) {
    expectRows("UNWIND [{n: 'Remy', a: 1}, {n: 'Adam', a: 'x'}, {n: 'Luc'}, {n: 'Bob', a: 2}] AS m "
               "OPTIONAL CALL (m) { MATCH (p:Person) WHERE p.name = m.n RETURN m.a AS v LIMIT 1 } "
               "RETURN m.n, v",
               {{"Adam", "x"}, {"Bob", "null"}, {"Luc", "null"}, {"Remy", "1"}});
}

TEST_F(OptionalSubqueryAnyPaddingTest, padsAListOrAMapHeldByAMap) {
    expectRows("UNWIND [{n: 'Remy', a: [1, 2]}, {n: 'Bob', a: [3]}, {n: 'Adam', a: {b: 1}}, {n: 'Zoe', a: {b: 2}}] AS m "
               "OPTIONAL CALL (m) { MATCH (p:Person) WHERE p.name = m.n RETURN m.a AS v } "
               "RETURN m.n, v",
               {{"Adam", "{b: 1}"}, {"Bob", "null"}, {"Remy", "[1, 2]"}, {"Zoe", "null"}});
}

TEST_F(OptionalSubqueryAnyPaddingTest, returnsTheMapValuesOfABodyOverNoInput) {
    expectRows("OPTIONAL CALL { UNWIND [{a: 1}, {a: 'x'}] AS m RETURN m.a AS v } RETURN v",
               {{"1"}, {"x"}});
}

TEST_F(OptionalSubqueryAnyPaddingTest, returnsTheMapValuesWithoutOptional) {
    expectRows("UNWIND [{a: 1}, {a: 'x'}] AS m CALL (m) { UNWIND [1, 2] AS y RETURN m.a AS v } RETURN v",
               {{"1"}, {"1"}, {"x"}, {"x"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
