#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A value of type any as a grouping key: equivalent values share a group, an integer and the
// double equal to it among them, and every null falls in one group. Remy is node 0; only
// Remy and Adam have an age, 32 each. Remy has 3 interests, Adam, Maxime, Luc, Cyrus and
// Suhas 2 each, Martina and Doruk 1.
class AnyGroupingKeyTest : public CallV3Test {
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

TEST_F(AnyGroupingKeyTest, groupsEqualValuesTogether) {
    expectRows("UNWIND [1, 'a', 1, 'a', 'b', true] AS x RETURN x, count(*)",
               {{"1", "2"}, {"a", "2"}, {"b", "1"}, {"true", "1"}});
}

TEST_F(AnyGroupingKeyTest, groupsAnIntegerWithAnEqualDouble) {
    expectOrderedRows("UNWIND [1, 1.0, 2, 'x'] AS x WITH x, count(*) AS c RETURN c ORDER BY c",
                      {{"1"}, {"1"}, {"2"}});
}

TEST_F(AnyGroupingKeyTest, groupsEveryNullTogether) {
    expectRows("UNWIND [null, 1, null, 'a'] AS x RETURN x, count(*)",
               {{"1", "1"}, {"a", "1"}, {"null", "2"}});
}

TEST_F(AnyGroupingKeyTest, groupsOnACasePerRow) {
    expectRows("MATCH (p:Person) RETURN CASE WHEN p.age IS NULL THEN p.isFrench ELSE p.age END AS v, count(*)",
               {{"32", "2"}, {"false", "4"}, {"true", "2"}});
}

TEST_F(AnyGroupingKeyTest, groupsOnACoalesce) {
    expectRows("MATCH (p:Person) RETURN coalesce(p.age, p.dob, p.hasPhD) AS v, count(*)",
               {{"24/07", "1"}, {"28/05", "1"}, {"32", "2"}, {"false", "3"}, {"true", "1"}});
}

TEST_F(AnyGroupingKeyTest, groupsNodesByIdentity) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "RETURN CASE WHEN p.name = 'Remy' THEN p ELSE p.name END AS v, count(*)",
               {{"0", "3"}, {"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"}, {"Martina", "1"},
                {"Maxime", "2"}, {"Suhas", "2"}});
}

TEST_F(AnyGroupingKeyTest, aggregatesValuesOfTypeAnyPerGroup) {
    expectRows("UNWIND [['a', 1], ['b', 2], ['a', 3], [1, 4]] AS pair "
               "RETURN pair[0] AS k, count(pair[1]), max(pair[1]), min(pair[1])",
               {{"1", "1", "4", "4"}, {"a", "2", "3", "1"}, {"b", "1", "2", "2"}});
}

TEST_F(AnyGroupingKeyTest, groupsOnTwoKeysOfTypeAny) {
    expectOrderedRows("UNWIND [[1, 'a'], [1.0, 'a'], [1, 'b'], ['1', 'a']] AS pair "
                      "WITH pair[0] AS k, pair[1] AS v, count(*) AS c RETURN c ORDER BY c",
                      {{"1"}, {"1"}, {"2"}});
}

TEST_F(AnyGroupingKeyTest, groupsTemporalValues) {
    expectRows("UNWIND [duration({days: 1}), duration({hours: 24}), datetime('2020-01-01T00:00:00Z'), 'x'] AS x "
               "RETURN x, count(*)",
               {{"2020-01-01T00:00:00Z", "1"}, {"PT24H", "2"}, {"x", "1"}});
}

TEST_F(AnyGroupingKeyTest, groupsListsAndMaps) {
    expectRows("UNWIND [[1], [1], {a: 1}, {a: 1}, 'x'] AS x RETURN x, count(*)",
               {{"[1]", "2"}, {"x", "1"}, {"{a: 1}", "2"}});
}

TEST_F(AnyGroupingKeyTest, groupsAnAnyKeyBesideATypedKey) {
    expectRows("MATCH (p:Person) "
               "RETURN p.isFrench AS f, CASE WHEN p.age IS NULL THEN 'none' ELSE p.age END AS v, count(*)",
               {{"false", "none", "4"}, {"true", "32", "2"}, {"true", "none", "2"}});
}

TEST_F(AnyGroupingKeyTest, ordersTheGroups) {
    expectOrderedRows("UNWIND [2, 'b', 2, 'a', null, 'b'] AS x RETURN x, count(*) ORDER BY x",
                      {{"a", "1"}, {"b", "2"}, {"2", "2"}, {"null", "1"}});
}

TEST_F(AnyGroupingKeyTest, filtersTheGroupsAfterAWith) {
    expectRows("UNWIND [1, 'a', 1, 'a', 'b'] AS x WITH x, count(*) AS c WHERE c > 1 RETURN x",
               {{"1"}, {"a"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
