#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view REMY = "MATCH (p:Person {name: 'Remy'}) ";

constexpr size_t ROWS_OVER_CHUNKS = 200000;

// Row i holds [i], i, i + 1 and null in turn, laid over three chunks
constexpr std::string_view MIXED_ROWS = "UNWIND range(0, 199999) AS i "
                                        "WITH i, CASE i % 4 WHEN 0 THEN [i] WHEN 1 THEN i WHEN 2 THEN i + 1 END AS x ";

constexpr std::array<std::string_view, 4> MIXED_ROW_MEMBERSHIPS {"true", "true", "false", "null"};

}

// Every expected row is Neo4j 5.26's answer to the same query
class IterateACellAsAListOfOneTest : public WriteQueryTest {
protected:
    void expectRowsAcrossChunks(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), expected.size()) << "query: " << query;

        for (size_t row = 0; row < rows.size(); row++) {
            ASSERT_EQ(rows[row], expected[row]) << "query: " << query << "\nrow: " << row;
        }
    }
};

TEST_F(IterateACellAsAListOfOneTest, testsMembershipInEachValueACellHolds) {
    expectRowsInOrder("UNWIND [[1], 1, 2, 'a', {a: 1}, null, 'ab'] AS x RETURN x, 1 IN x, 'ab' IN x",
                      {{"[1]", "true", "false"},
                       {"1", "true", "false"},
                       {"2", "false", "false"},
                       {"a", "false", "false"},
                       {"{a: 1}", "false", "false"},
                       {"null", "null", "null"},
                       {"ab", "false", "true"}});
}

TEST_F(IterateACellAsAListOfOneTest, filtersOnMembershipInEachValueACellHolds) {
    expectRowsInOrder("UNWIND [[1], 1, 2, 'a', {a: 1}, null, 'ab'] AS x WITH x WHERE 1 IN x RETURN x",
                      {{"[1]"}, {"1"}});
    expectRowsInOrder("UNWIND [[1], 1, 2, 'a', {a: 1}, null, 'ab'] AS x WITH x WHERE 'ab' IN x RETURN x",
                      {{"ab"}});
    expectRowsInOrder("UNWIND [[1], 1, 2, 'a', {a: 1}, null, 'ab'] AS x WITH x WHERE NOT 1 IN x RETURN x",
                      {{"2"}, {"a"}, {"{a: 1}"}, {"ab"}});
    expectRows("UNWIND [['Remy'], 'Adam', null, 'Nobody'] AS names MATCH (n:Person) WHERE n.name IN names RETURN n.name",
               {{"Remy"}, {"Adam"}});
    expectRows("MATCH (p:Person) WITH p, CASE WHEN p.name = 'Remy' THEN [p] ELSE p END AS x "
               "MATCH (q:Person) WHERE q IN x RETURN count(q)",
               {{"8"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsTheMembershipOfANull) {
    expectRowsInOrder("UNWIND [[1], [], 1, 'a', {a: 1}, null] AS x RETURN x, null IN x",
                      {{"[1]", "null"},
                       {"[]", "false"},
                       {"1", "null"},
                       {"a", "null"},
                       {"{a: 1}", "null"},
                       {"null", "null"}});
    expectRowsInOrder("UNWIND [[1], [], 1, 'a', {a: 1}, null] AS x WITH x WHERE null IN x RETURN x", {});
    expectRowsInOrder("UNWIND [[1], [], 1, 'a', {a: 1}, null] AS x WITH x WHERE (null IN x) IS NULL RETURN x",
                      {{"[1]"}, {"1"}, {"a"}, {"{a: 1}"}, {"null"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsMembershipInANodeACellHolds) {
    expectRowsInOrder(std::string(REMY) + "UNWIND [p, [p], 1, null] AS x RETURN p IN x, 1 IN x, 'Remy' IN x",
                      {{"true", "false", "false"},
                       {"true", "false", "false"},
                       {"false", "true", "false"},
                       {"null", "null", "null"}});
    expectRowsInOrder(std::string(REMY) + "UNWIND [p, [p], 1, null] AS x WITH p, x WHERE p IN x RETURN x",
                      {{"0"}, {"[0]"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsMembershipOfAContainerInACell) {
    expectRowsInOrder("UNWIND [{a: 1}, [{a: 1}], 1, {a: 2}] AS x RETURN {a: 1} IN x",
                      {{"true"}, {"true"}, {"false"}, {"false"}});
    expectRowsInOrder("UNWIND [1, [[1]], [1], 'a'] AS x RETURN [1] IN x",
                      {{"false"}, {"true"}, {"false"}, {"false"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsMembershipOfACellInACell) {
    expectRowsInOrder("UNWIND [[1], 1, 1.0, 'a', null] AS x RETURN x IN x, 1 IN x",
                      {{"false", "true"},
                       {"true", "true"},
                       {"true", "true"},
                       {"true", "false"},
                       {"null", "null"}});
    expectRowsInOrder("UNWIND [[1], 1] AS x UNWIND [[1], 1, [[1]]] AS y RETURN x IN y",
                      {{"false"}, {"false"}, {"true"}, {"true"}, {"true"}, {"false"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsMembershipInAConstantCell) {
    expectRows("RETURN 1 IN [[1], 1][1], 1 IN [[1], 1][0], 2 IN [[1], 1][1]", {{"true", "true", "false"}});
    expectRows("RETURN 1 IN [[1], 1][5], null IN [[1], 1][5]", {{"null", "null"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsMembershipInACellThatMayBeAbsent) {
    expectRowsInOrder("UNWIND [0, 1, 5, 2] AS i RETURN i, 1 IN [[1], 1, 'a'][i], null IN [[1], 1, []][i]",
                      {{"0", "true", "null"},
                       {"1", "true", "null"},
                       {"5", "null", "null"},
                       {"2", "false", "false"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsMembershipInAnAggregate) {
    expectRows("UNWIND [[1], 1] AS x RETURN 1 IN max(x), 1 IN min(x)", {{"true", "true"}});
    expectRows("UNWIND [[1], 1, 'a', null] AS x WITH x AS k, max(x) AS m RETURN k, 1 IN m, 'a' IN m",
               {{"[1]", "true", "false"},
                {"1", "true", "false"},
                {"a", "false", "true"},
                {"null", "null", "null"}});
    expectRows("UNWIND [[1], 1, 'a', null] AS x WITH x AS k, max(x) AS m WITH k, m WHERE 1 IN m RETURN k",
               {{"[1]"}, {"1"}});
    expectRows("UNWIND [[1], 1] AS x WITH collect(x) AS c RETURN 1 IN head(c), 1 IN last(c)", {{"true", "true"}});
}

TEST_F(IterateACellAsAListOfOneTest, decidesACaseOnMembership) {
    expectRowsInOrder("UNWIND [[1], 1, null] AS x RETURN CASE WHEN 1 IN x THEN 'in' ELSE 'out' END",
                      {{"in"}, {"in"}, {"out"}});
}

TEST_F(IterateACellAsAListOfOneTest, buildsAComprehensionOverEachValueACellHolds) {
    expectRowsInOrder("UNWIND [[1], 1, 'ab', {a: 1}, null] AS x "
                      "RETURN [y IN x | y], [y IN x WHERE y = 1], [y IN x WHERE y = 1 | y + 1], size([y IN x | y])",
                      {{"[1]", "[1]", "[2]", "1"},
                       {"[1]", "[1]", "[2]", "1"},
                       {"[ab]", "[]", "[]", "1"},
                       {"[{a: 1}]", "[]", "[]", "1"},
                       {"null", "null", "null", "null"}});
    expectRowsInOrder(std::string(REMY) + "UNWIND [p, 1] AS x RETURN [y IN x | y = p]", {{"[true]"}, {"[false]"}});
}

TEST_F(IterateACellAsAListOfOneTest, decidesAListPredicateOverEachValueACellHolds) {
    expectRowsInOrder("UNWIND [[1], 1, 2, 'ab', {a: 1}, null, []] AS x "
                      "RETURN all(y IN x WHERE y = 1), any(y IN x WHERE y = 1), none(y IN x WHERE y = 1), "
                      "single(y IN x WHERE y = 1)",
                      {{"true", "true", "false", "true"},
                       {"true", "true", "false", "true"},
                       {"false", "false", "true", "false"},
                       {"false", "false", "true", "false"},
                       {"false", "false", "true", "false"},
                       {"null", "null", "null", "null"},
                       {"true", "false", "true", "false"}});
    expectRowsInOrder(std::string(REMY) + "UNWIND [p, 1] AS x RETURN any(y IN x WHERE y = p), all(y IN x WHERE y = p)",
                      {{"true", "true"}, {"false", "false"}});
    expectRowsInOrder("UNWIND [[1], 1, 2, 'ab', {a: 1}, null, []] AS x WITH x WHERE any(y IN x WHERE y = 1) RETURN x",
                      {{"[1]"}, {"1"}});
}

TEST_F(IterateACellAsAListOfOneTest, reducesEachValueACellHolds) {
    expectRowsInOrder("UNWIND [[1, 2], 1, 'ab', {a: 1}, null] AS x "
                      "RETURN reduce(acc = [], y IN x | acc + [y]), reduce(n = 0, y IN x | n + 1)",
                      {{"[1, 2]", "2"},
                       {"[1]", "1"},
                       {"[ab]", "1"},
                       {"[{a: 1}]", "1"},
                       {"null", "null"}});
}

TEST_F(IterateACellAsAListOfOneTest, unwindsEachValueACellHolds) {
    expectRowsInOrder("UNWIND [[1, 2], 1, 'ab', {a: 1}, null] AS x UNWIND x AS y RETURN x, y",
                      {{"[1, 2]", "1"},
                       {"[1, 2]", "2"},
                       {"1", "1"},
                       {"ab", "ab"},
                       {"{a: 1}", "{a: 1}"}});
    expectRowsInOrder(std::string(REMY) + "UNWIND [p, 1] AS x UNWIND x AS y RETURN y = p", {{"true"}, {"false"}});
}

TEST_F(IterateACellAsAListOfOneTest, iteratesACellThatIsConstantOrMayBeAbsent) {
    expectRows("UNWIND [[1], 1][1] AS y RETURN y", {{"1"}});
    expectRows("RETURN [y IN [[1], 1][1] | y], any(y IN [[1], 1][1] WHERE y = 1), reduce(s = 0, y IN [[1], 1][1] | s + y)",
               {{"[1]", "true", "1"}});
    expectRowsInOrder("UNWIND [0, 1, 5] AS i UNWIND [[1, 2], 3][i] AS y RETURN i, y", {{"0", "1"}, {"0", "2"}, {"1", "3"}});
    expectRowsInOrder("UNWIND [0, 1, 5] AS i "
                      "RETURN i, [y IN [[1, 2], 3][i] | y], all(y IN [[1, 2], 3][i] WHERE y > 0), "
                      "reduce(s = 0, y IN [[1, 2], 3][i] | s + y)",
                      {{"0", "[1, 2]", "true", "3"},
                       {"1", "[3]", "true", "3"},
                       {"5", "null", "null", "null"}});
}

TEST_F(IterateACellAsAListOfOneTest, writesOnceForEachValueACellHolds) {
    applyWrite("UNWIND [[1, 2], 3, 'ab', {a: 1}, null] AS x FOREACH (y IN x | CREATE (:Tag))");

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"5"}});
}

TEST_F(IterateACellAsAListOfOneTest, testsMembershipOverRowsOfSeveralChunks) {
    Rows expected;
    expected.reserve(ROWS_OVER_CHUNKS);

    for (size_t row = 0; row < ROWS_OVER_CHUNKS; row++) {
        expected.push_back({std::to_string(row), std::string(MIXED_ROW_MEMBERSHIPS[row % 4])});
    }

    expectRowsAcrossChunks(std::string(MIXED_ROWS) + "RETURN i, i IN x", expected);

    expectRows(std::string(MIXED_ROWS) + "WITH i, x WHERE i IN x RETURN count(*), min(i), max(i)",
               {{"100000", "0", "199997"}});
}

TEST_F(IterateACellAsAListOfOneTest, iteratesRowsOfSeveralChunks) {
    expectRows(std::string(MIXED_ROWS) + "RETURN i % 4 AS k, size([y IN x | y]) AS s, any(y IN x WHERE y = i) AS a, count(*)",
               {{"0", "1", "true", "50000"},
                {"1", "1", "true", "50000"},
                {"2", "1", "false", "50000"},
                {"3", "null", "null", "50000"}});
}
