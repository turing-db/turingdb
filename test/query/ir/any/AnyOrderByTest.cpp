#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// ORDER BY over values of type any follows Cypher's order across types:
// MAP < NODE < RELATIONSHIP < LIST < DATETIME < DURATION < STRING < BOOLEAN < NUMBER < NULL,
// and each type's own order within it. Remy is node 0, Adam 1 and Luc 9; only Remy and
// Adam have an age, 32 each.
class AnyOrderByTest : public CallV3Test {
protected:
    // One value of a different type per person, and none for Suhas
    static constexpr std::string_view PER_PERSON_VALUE =
        "CASE p.name WHEN 'Remy' THEN 32 WHEN 'Adam' THEN 2.5 WHEN 'Maxime' THEN true "
        "WHEN 'Luc' THEN 'x' WHEN 'Martina' THEN [1, 2] WHEN 'Cyrus' THEN {a: 1} "
        "WHEN 'Doruk' THEN datetime('2020-01-01T00:00:00Z') END";

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.rows(), expected) << query;
    }

    static std::string perPersonQuery(std::string_view tail) {
        return "MATCH (p:Person) WITH p, " + std::string(PER_PERSON_VALUE) + " AS v " + std::string(tail);
    }
};

TEST_F(AnyOrderByTest, ordersEveryTypeByItsClass) {
    expectOrderedRows("UNWIND [1, 'b', true, null, [1], {a: 1}, 2.5, 'a', false, duration({days: 1}), "
                      "datetime('2020-01-01T00:00:00Z')] AS x RETURN x ORDER BY x",
                      {{"{a: 1}"}, {"[1]"}, {"2020-01-01T00:00:00Z"}, {"PT24H"}, {"a"}, {"b"},
                       {"false"}, {"true"}, {"1"}, {"2.500000"}, {"null"}});
}

TEST_F(AnyOrderByTest, ordersDescendingWithNullFirst) {
    expectOrderedRows("UNWIND [1, 'b', true, null, [1], {a: 1}, 2.5, 'a', false, duration({days: 1}), "
                      "datetime('2020-01-01T00:00:00Z')] AS x RETURN x ORDER BY x DESC",
                      {{"null"}, {"2.500000"}, {"1"}, {"true"}, {"false"}, {"b"}, {"a"},
                       {"PT24H"}, {"2020-01-01T00:00:00Z"}, {"[1]"}, {"{a: 1}"}});
}

TEST_F(AnyOrderByTest, ordersIntegersAndDoublesNumerically) {
    expectOrderedRows("UNWIND [3, 2.5, -1, 10, 0.5, 'z'] AS x RETURN x ORDER BY x",
                      {{"z"}, {"-1"}, {"0.500000"}, {"2.500000"}, {"3"}, {"10"}});
}

TEST_F(AnyOrderByTest, ordersStringsByCodePoint) {
    expectOrderedRows("UNWIND ['b', 'B', 'a', 1, 'ab'] AS x RETURN x ORDER BY x",
                      {{"B"}, {"a"}, {"ab"}, {"b"}, {"1"}});
}

TEST_F(AnyOrderByTest, ordersListsElementWiseWithAPrefixFirst) {
    expectOrderedRows("UNWIND [[1, 2], [1], [2], [1, 'a'], 'x'] AS x RETURN x ORDER BY x",
                      {{"[1]"}, {"[1, a]"}, {"[1, 2]"}, {"[2]"}, {"x"}});
}

TEST_F(AnyOrderByTest, ordersMapsBySizeThenKeysThenValues) {
    expectOrderedRows("UNWIND [{b: 1}, {a: 2}, {a: 1}, {a: 1, b: 1}, 1] AS x RETURN x ORDER BY x",
                      {{"{a: 1}"}, {"{a: 2}"}, {"{b: 1}"}, {"{a: 1, b: 1}"}, {"1"}});
}

TEST_F(AnyOrderByTest, ordersDateTimesBeforeDurations) {
    expectOrderedRows("UNWIND [duration({days: 2}), datetime('2021-01-01T00:00:00Z'), duration({days: 1}), "
                      "datetime('2020-01-01T00:00:00Z'), 'x'] AS x RETURN x ORDER BY x",
                      {{"2020-01-01T00:00:00Z"}, {"2021-01-01T00:00:00Z"}, {"PT24H"}, {"PT48H"}, {"x"}});
}

TEST_F(AnyOrderByTest, ordersNodesByIdentityBeforeScalars) {
    expectOrderedRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc', 'Adam'] "
                      "WITH p, CASE WHEN p.name = 'Adam' THEN 1 ELSE p END AS v RETURN p.name ORDER BY v",
                      {{"Remy"}, {"Luc"}, {"Adam"}});
}

TEST_F(AnyOrderByTest, ordersAValuePerRow) {
    expectOrderedRows(perPersonQuery("RETURN p.name ORDER BY v"),
                      {{"Cyrus"}, {"Martina"}, {"Doruk"}, {"Luc"}, {"Maxime"}, {"Adam"}, {"Remy"}, {"Suhas"}});
}

TEST_F(AnyOrderByTest, ordersAValuePerRowDescending) {
    expectOrderedRows(perPersonQuery("RETURN p.name ORDER BY v DESC"),
                      {{"Suhas"}, {"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Doruk"}, {"Martina"}, {"Cyrus"}});
}

TEST_F(AnyOrderByTest, keepsTheFirstRowsOfTheOrder) {
    expectOrderedRows(perPersonQuery("RETURN p.name ORDER BY v LIMIT 3"),
                      {{"Cyrus"}, {"Martina"}, {"Doruk"}});
    expectOrderedRows(perPersonQuery("RETURN p.name ORDER BY v DESC LIMIT 2"),
                      {{"Suhas"}, {"Remy"}});
}

TEST_F(AnyOrderByTest, skipsTheFirstRowsOfTheOrder) {
    expectOrderedRows(perPersonQuery("RETURN p.name ORDER BY v SKIP 5 LIMIT 2"),
                      {{"Adam"}, {"Remy"}});
}

TEST_F(AnyOrderByTest, ordersACoalesceAndBreaksTiesOnASecondKey) {
    expectOrderedRows("MATCH (p:Person) RETURN p.name ORDER BY coalesce(p.age, p.name), p.name",
                      {{"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}, {"Adam"}, {"Remy"}});
}

TEST_F(AnyOrderByTest, ordersAnElementReadOutOfAMixedList) {
    expectOrderedRows("UNWIND [[3, 'a'], ['b', 1], [true, 0]] AS pair RETURN pair[0] ORDER BY pair[0]",
                      {{"b"}, {"true"}, {"3"}});
}

TEST_F(AnyOrderByTest, breaksTiesBetweenAnIntegerAndAnEqualDouble) {
    expectOrderedRows("UNWIND [[1, 'b'], ['a', 'c'], [1.0, 'a'], ['a', 'b']] AS pair "
                      "RETURN pair[0], pair[1] ORDER BY pair[0], pair[1]",
                      {{"a", "b"}, {"a", "c"}, {"1.000000", "a"}, {"1", "b"}});
}

TEST_F(AnyOrderByTest, ordersAnAnyKeyBesideATypedKey) {
    expectOrderedRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam', 'Luc', 'Maxime'] "
                      "RETURN p.name ORDER BY CASE WHEN p.age IS NULL THEN 'none' ELSE p.age END DESC, p.name",
                      {{"Adam"}, {"Remy"}, {"Luc"}, {"Maxime"}});
}

TEST_F(AnyOrderByTest, ordersByAProjectedAlias) {
    expectOrderedRows("UNWIND [2, 'a', 1] AS x RETURN x AS y ORDER BY y",
                      {{"a"}, {"1"}, {"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
