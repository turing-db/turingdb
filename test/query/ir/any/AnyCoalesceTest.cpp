#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// coalesce over arguments of different types answers a value of type any per row: the first
// argument that is not null, in its own type. Remy and Adam are 32; Maxime was born on 24/07
// and Luc on 28/05; Remy, Adam, Luc and Martina have a PhD; Luc is node 9.
class AnyCoalesceTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyCoalesceTest, answersTheFirstValueOfEachRowInItsOwnType) {
    expectRows("MATCH (p:Person) RETURN p.name, coalesce(p.age, p.dob, p.hasPhD)",
               {{"Adam", "32"}, {"Cyrus", "false"}, {"Doruk", "false"}, {"Luc", "28/05"},
                {"Martina", "true"}, {"Maxime", "24/07"}, {"Remy", "32"}, {"Suhas", "false"}});
}

TEST_F(AnyCoalesceTest, answersNullWhereEveryArgumentIsNull) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Suhas', 'Remy'] RETURN p.name, coalesce(p.age, p.dob)",
               {{"Remy", "32"}, {"Suhas", "null"}});
}

TEST_F(AnyCoalesceTest, coalescesACellWithATypedDefault) {
    expectRows("UNWIND [null, 'x', 2] AS v RETURN coalesce(v, 0)", {{"0"}, {"2"}, {"x"}});
    expectRows("UNWIND [null, 1, 'a'] AS x RETURN coalesce(x, 'default')", {{"1"}, {"a"}, {"default"}});
}

TEST_F(AnyCoalesceTest, coalescesTwoCells) {
    expectRows("UNWIND [[null, 'b'], [1, 'b'], [null, null]] AS pair RETURN coalesce(pair[0], pair[1])",
               {{"1"}, {"b"}, {"null"}});
}

TEST_F(AnyCoalesceTest, coalescesANode) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] RETURN p.name, coalesce(p.age, p)",
               {{"Luc", "9"}, {"Remy", "32"}});
}

TEST_F(AnyCoalesceTest, coalescesTemporalValues) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, coalesce(p.age, duration({days: 1})), coalesce(p.age, datetime('2020-01-01T00:00:00Z'))",
               {{"Luc", "PT24H", "2020-01-01T00:00:00Z"}, {"Remy", "32", "32"}});
}

TEST_F(AnyCoalesceTest, coalescesListsAndMaps) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, coalesce(p.age, [p.name]), coalesce(p.age, {n: p.name})",
               {{"Luc", "[Luc]", "{n: Luc}"}, {"Remy", "32", "32"}});
}

TEST_F(AnyCoalesceTest, coalescesConstants) {
    expectRows("RETURN coalesce(null, 'a', 1)", {{"a"}});
    expectRows("RETURN coalesce(1, 'a')", {{"1"}});
    expectRows("RETURN coalesce(null, [1, 'a'][5], 2.5)", {{"2.500000"}});
}

TEST_F(AnyCoalesceTest, comparesTheCoalescedValue) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc', 'Martina'] "
               "RETURN p.name, coalesce(p.age, p.dob, p.name) = 32",
               {{"Luc", "false"}, {"Martina", "false"}, {"Remy", "true"}});
}

TEST_F(AnyCoalesceTest, filtersOnTheCoalescedValue) {
    expectRows("MATCH (p:Person) WITH p, coalesce(p.age, p.dob) AS v WHERE v IS NOT NULL RETURN p.name",
               {{"Adam"}, {"Luc"}, {"Maxime"}, {"Remy"}});
}

TEST_F(AnyCoalesceTest, countsTheCoalescedValues) {
    expectRows("MATCH (p:Person) RETURN count(coalesce(p.age, p.dob))", {{"4"}});
}

TEST_F(AnyCoalesceTest, coalescesValuesReadOutOfAMap) {
    expectRows("WITH {a: 1, c: 'x'} AS m RETURN coalesce(m.b, m.a), coalesce(m.b, m.c)", {{"1", "x"}});
}

TEST_F(AnyCoalesceTest, coalescesTheResultOfACoalesce) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Maxime', 'Suhas'] "
               "RETURN p.name, coalesce(coalesce(p.age, p.dob), false)",
               {{"Maxime", "24/07"}, {"Remy", "32"}, {"Suhas", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
