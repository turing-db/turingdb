#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// m['k'] reads what m.k reads where every row of m holds a map, a node, an edge or null, so
// there it plans as m.k does. Over any other value the two differ: m.year reads a component
// of a datetime that m['year'] raises on.
class FoldConstantMapKeysTest : public HashJoinQueryTest {
protected:
    void expectPlansAsTheDottedForm(std::string_view bracketQuery, std::string_view dottedQuery) {
        std::string bracketProgram;
        dbProgram(bracketQuery, bracketProgram);

        std::string dottedProgram;
        dbProgram(dottedQuery, dottedProgram);

        EXPECT_FALSE(contains(bracketProgram, "db.dynamic_map_key")) << bracketProgram;
        EXPECT_EQ(bracketProgram, dottedProgram);
    }

    void expectKeepsTheBracketRead(std::string_view query) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, "db.dynamic_map_key")) << program;
    }

    void expectJoins(std::string_view query) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, "db.hash_join")) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectRaisesTheBracketError(std::string_view query) {
        try {
            StringRowSink sink;
            runQuery(query, sink);
        } catch (const TuringException& error) {
            const std::string message = error.what();
            EXPECT_TRUE(contains(message, "Index operator []")) << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query raised nothing: " << query;
    }
};

TEST_F(FoldConstantMapKeysTest, joinsOnAValueReadByABracketKey) {
    const std::string_view query = "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                                   "MATCH (a:Person {name: row.s['t'].name}) RETURN a.name";

    expectJoins(query);
    expectJoinedRows(query, {{"Luc"}, {"Remy"}});
}

TEST_F(FoldConstantMapKeysTest, plansABracketKeyAsTheDottedForm) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                               "MATCH (a:Person {name: row.s['t'].name}) RETURN a.name",
                               "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                               "MATCH (a:Person {name: row.s.t.name}) RETURN a.name");
}

// The WHERE lays the key out over the one row the WITH keeps, and the product with that row
// carries it to the read
TEST_F(FoldConstantMapKeysTest, joinsOnAKeyLaidOutByAWith) {
    const std::string_view bracketQuery = "WITH 't' AS k WHERE k IS NOT NULL "
                                          "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                                          "MATCH (a:Person {name: row.s[k].name}) RETURN a.name";

    expectPlansAsTheDottedForm(bracketQuery,
                               "WITH 't' AS k WHERE k IS NOT NULL "
                               "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                               "MATCH (a:Person {name: row.s.t.name}) RETURN a.name");

    std::string program;
    dbProgram(bracketQuery, program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;

    expectJoinedRows(bracketQuery, {{"Luc"}, {"Remy"}});
}

TEST_F(FoldConstantMapKeysTest, readsAKeyOfAMapReadByABracketKey) {
    const std::string_view bracketQuery = "UNWIND [{s: {t: {u: 1}}}, {s: {t: null}}, {s: null}, null] AS row "
                                          "RETURN row.s['t']['u'] AS v";

    expectPlansAsTheDottedForm(bracketQuery,
                               "UNWIND [{s: {t: {u: 1}}}, {s: {t: null}}, {s: null}, null] AS row "
                               "RETURN row.s.t.u AS v");
    expectSortedRows(bracketQuery, {{"1"}, {"null"}, {"null"}, {"null"}});
}

TEST_F(FoldConstantMapKeysTest, readsAKeyOfAMapCarriedPastASort) {
    const std::string_view bracketQuery = "UNWIND [{s: {t: {u: 2}}}, {s: {t: {u: 1}}}] AS row "
                                          "WITH row.s['t'] AS t ORDER BY t['u'] RETURN t['u'] AS v";

    expectPlansAsTheDottedForm(bracketQuery,
                               "UNWIND [{s: {t: {u: 2}}}, {s: {t: {u: 1}}}] AS row "
                               "WITH row.s.t AS t ORDER BY t.u RETURN t.u AS v");
    expectRows(bracketQuery, {{"1"}, {"2"}});
}

TEST_F(FoldConstantMapKeysTest, readsAPropertyOfANodeHeldByAMap) {
    const std::string_view bracketQuery = "MATCH (n:Person {name: 'Remy'}) WITH {p: n} AS m "
                                          "UNWIND [m] AS row RETURN row.p['name'] AS v";

    expectPlansAsTheDottedForm(bracketQuery,
                               "MATCH (n:Person {name: 'Remy'}) WITH {p: n} AS m "
                               "UNWIND [m] AS row RETURN row.p.name AS v");
    expectRows(bracketQuery, {{"Remy"}});
}

TEST_F(FoldConstantMapKeysTest, keepsAKeyEachRowHolds) {
    const std::string_view query = "UNWIND ['t', 'u'] AS k UNWIND [{t: 1, u: 2}] AS row RETURN row[k]";

    expectKeepsTheBracketRead(query);
    expectSortedRows(query, {{"1"}, {"2"}});
}

// A CASE with no ELSE gives the rows no branch takes a null key, which reads no value
TEST_F(FoldConstantMapKeysTest, keepsAKeyOnlySomeRowsHold) {
    const std::string_view query = "UNWIND [1, 2] AS x WITH x, CASE WHEN x = 1 THEN 't' END AS k "
                                   "UNWIND [{t: 5}] AS row RETURN x, row[k]";

    expectKeepsTheBracketRead(query);
    expectSortedRows(query, {{"1", "5"}, {"2", "null"}});
}

// The key carries the rows the WHERE keeps, which a read of the constant map alone would not
TEST_F(FoldConstantMapKeysTest, keepsTheRowsALaidOutKeyCarries) {
    expectRows("WITH 't' AS k WHERE k IS NULL RETURN {t: 1}[k]", {});
    expectRows("WITH 't' AS k WHERE k IS NOT NULL RETURN {t: 1}[k]", {{"1"}});
}

TEST_F(FoldConstantMapKeysTest, raisesOnADatetimeWhoseComponentTheDottedFormReads) {
    const std::string_view query = "UNWIND [{year: 1}, datetime('2020-01-01T00:00:00Z')] AS row RETURN row['year']";

    expectKeepsTheBracketRead(query);
    expectRaisesTheBracketError(query);
}

TEST_F(FoldConstantMapKeysTest, raisesOnAValueWithNoKeys) {
    const std::string_view query = "UNWIND [{t: 1}, 5] AS row RETURN row['t']";

    expectKeepsTheBracketRead(query);
    expectRaisesTheBracketError(query);
}
