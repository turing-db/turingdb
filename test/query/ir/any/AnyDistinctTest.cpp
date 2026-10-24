#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// DISTINCT over values of type any keeps one row per equivalent value: an integer and the
// double equal to it are one value, a null is one value, and values of different types are
// never equal. Remy is node 0 and Luc node 9; only Remy and Adam have an age, 32 each.
class AnyDistinctTest : public CallV3Test {
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

TEST_F(AnyDistinctTest, keepsOneRowPerValue) {
    expectRows("UNWIND [1, 'a', 1, 'a', true, true, null, null] AS x RETURN DISTINCT x",
               {{"1"}, {"a"}, {"null"}, {"true"}});
}

TEST_F(AnyDistinctTest, mergesAnIntegerWithAnEqualDouble) {
    expectRows("UNWIND [1, 1.0, 'x'] AS x WITH DISTINCT x RETURN count(*)", {{"2"}});
    expectRows("UNWIND [1, 1.5, 'x'] AS x WITH DISTINCT x RETURN count(*)", {{"3"}});
}

TEST_F(AnyDistinctTest, keepsABooleanApartFromANumberAndAString) {
    expectRows("UNWIND [true, 1, false, 0, 'true'] AS x WITH DISTINCT x RETURN count(*)", {{"5"}});
}

TEST_F(AnyDistinctTest, comparesStringsWithTheirCase) {
    expectRows("UNWIND ['a', 'A', 'a', 1] AS x RETURN DISTINCT x", {{"1"}, {"A"}, {"a"}});
}

TEST_F(AnyDistinctTest, comparesListsElementWise) {
    expectRows("UNWIND [[1, 2], [1, 2], [2, 1], [1], 'x'] AS x WITH DISTINCT x RETURN count(*)", {{"4"}});
    expectRows("UNWIND [[1], [1.0], 'x'] AS x WITH DISTINCT x RETURN count(*)", {{"2"}});
}

TEST_F(AnyDistinctTest, comparesMapsEntryWise) {
    expectRows("UNWIND [{a: 1}, {a: 1}, {a: 2}, {b: 1}, 1] AS x WITH DISTINCT x RETURN count(*)", {{"4"}});
}

TEST_F(AnyDistinctTest, comparesTemporalValues) {
    expectRows("UNWIND [datetime('2020-01-01T00:00:00Z'), datetime('2020-01-01T00:00:00Z'), "
               "duration({days: 1}), duration({hours: 24}), 'x'] AS x WITH DISTINCT x RETURN count(*)",
               {{"3"}});
}

TEST_F(AnyDistinctTest, keepsNodesByIdentity) {
    expectRows("MATCH (p:Person) WITH CASE WHEN p.name IN ['Remy', 'Luc'] THEN p ELSE 'other' END AS v "
               "RETURN DISTINCT v",
               {{"0"}, {"9"}, {"other"}});
}

TEST_F(AnyDistinctTest, keepsOneRowPerValueOfACase) {
    expectRows("MATCH (p:Person) RETURN DISTINCT CASE WHEN p.age IS NULL THEN p.isFrench ELSE p.age END",
               {{"32"}, {"false"}, {"true"}});
}

TEST_F(AnyDistinctTest, keepsOneRowPerValueOfACoalesce) {
    expectRows("MATCH (p:Person) RETURN DISTINCT coalesce(p.age, p.dob, 'none')",
               {{"24/07"}, {"28/05"}, {"32"}, {"none"}});
}

TEST_F(AnyDistinctTest, comparesEveryColumnOfTheRow) {
    expectRows("UNWIND [[1, 'a'], [1.0, 'a'], [1, 'b'], ['1', 'a']] AS pair "
               "WITH DISTINCT pair[0] AS k, pair[1] AS v RETURN count(*)",
               {{"3"}});
}

TEST_F(AnyDistinctTest, keepsAnAnyColumnBesideATypedOne) {
    expectRows("MATCH (p:Person) RETURN DISTINCT p.isFrench, CASE WHEN p.age IS NULL THEN 'none' ELSE p.age END",
               {{"false", "none"}, {"true", "32"}, {"true", "none"}});
}

TEST_F(AnyDistinctTest, ordersTheDistinctRows) {
    expectOrderedRows("UNWIND [3, 'a', 3, 'b', 'a', null] AS x RETURN DISTINCT x ORDER BY x",
                      {{"a"}, {"b"}, {"3"}, {"null"}});
}

TEST_F(AnyDistinctTest, countsDistinctValuesWithoutNull) {
    expectRows("UNWIND [1, 1.0, 'a', 'a', null, true] AS x RETURN count(DISTINCT x)", {{"3"}});
}

TEST_F(AnyDistinctTest, keepsOneRowPerValueAfterAWith) {
    expectRows("UNWIND ['a', 2, 'a', 2.0, false] AS x WITH DISTINCT x WITH x RETURN count(*)", {{"3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
