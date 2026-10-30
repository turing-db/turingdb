#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A list holds paths as it holds any other value. Remy is node 0 and has three INTERESTED_IN
// paths, (0), [2], (2) to Computers, (0), [3], (3) to Eighties and (0), [1], (6) to Ghosts,
// and one KNOWS_WELL path, (0), [0], (1) to Adam.
class ListOfPathsTest : public CallV3Test {
protected:
    static constexpr std::string_view REMYS_INTERESTS =
        "MATCH p=(:Person {name: 'Remy'})-[:INTERESTED_IN]->(i:Interest) ";

    static constexpr std::string_view REMY_KNOWS_ADAM =
        "MATCH p=(:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) ";

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << query;
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.rows(), expected) << query;
    }
};

TEST_F(ListOfPathsTest, collectsThePathsInTheOrderTheyCome) {
    std::string query(REMYS_INTERESTS);
    query += "WITH p ORDER BY i.name RETURN collect(p)";

    expectRows(query, {{"[(0), [2], (2), (0), [3], (3), (0), [1], (6)]"}});
}

TEST_F(ListOfPathsTest, dropsTheNullPathsAnOptionalMatchLeft) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH p=(n)-[:KNOWS_WELL]->(:Person) RETURN n.name, size(collect(p))",
               {
                   {"Remy", "1"},
                   {"Adam", "1"},
                   {"Maxime", "0"},
                   {"Luc", "0"},
                   {"Martina", "0"},
                   {"Suhas", "0"},
                   {"Cyrus", "0"},
                   {"Doruk", "0"},
               });
}

TEST_F(ListOfPathsTest, collectsEachDistinctPathOnce) {
    std::string query(REMYS_INTERESTS);
    query += "UNWIND [1, 2] AS copy RETURN size(collect(p)), size(collect(DISTINCT p))";

    expectRows(query, {{"6", "3"}});
}

TEST_F(ListOfPathsTest, unwindsTheCollectedPaths) {
    std::string query(REMYS_INTERESTS);
    query += "WITH collect(p) AS paths UNWIND paths AS x RETURN length(x), nodes(x)[1], relationships(x)[0]";

    expectRows(query, {{"1", "2", "2"}, {"1", "3", "3"}, {"1", "6", "1"}});
}

TEST_F(ListOfPathsTest, readsThePathsAListHoldsByPosition) {
    std::string query(REMYS_INTERESTS);
    query += "WITH p ORDER BY i.name WITH collect(p) AS paths "
             "RETURN nodes(paths[0])[1], nodes(head(paths))[1], nodes(last(paths))[1], size(paths[1..])";

    expectRows(query, {{"2", "2", "6", "2"}});
}

TEST_F(ListOfPathsTest, comparesAListedPathWithAPathEitherWayRound) {
    std::string query(REMYS_INTERESTS);
    query += "WITH p ORDER BY i.name WITH collect(p) AS paths "
             "MATCH q=(:Person {name: 'Remy'})-[:INTERESTED_IN]->(j:Interest) "
             "RETURN j.name, q = paths[0], paths[0] = q, q <> paths[0], q IN paths, q IN paths[1..]";

    expectRows(query, {
        {"Computers", "true", "true", "false", "true", "false"},
        {"Eighties", "false", "false", "true", "true", "true"},
        {"Ghosts", "false", "false", "true", "true", "true"},
    });
}

TEST_F(ListOfPathsTest, comparesListsOfPaths) {
    std::string query(REMY_KNOWS_ADAM);
    query += "MATCH q=(:Person {name: 'Adam'})-[:KNOWS_WELL]->(:Person) "
             "RETURN [p] = [p], [p] = [q], [p, q] = [p] + q";

    expectRows(query, {{"true", "false", "true"}});
}

TEST_F(ListOfPathsTest, ordersListsOfPathsByTheirIDs) {
    std::string query(REMYS_INTERESTS);
    query += "RETURN [p] AS l ORDER BY l";

    expectOrderedRows(query, {{"[(0), [1], (6)]"}, {"[(0), [2], (2)]"}, {"[(0), [3], (3)]"}});
}

TEST_F(ListOfPathsTest, groupsByAListOfPaths) {
    std::string query(REMYS_INTERESTS);
    query += "UNWIND [1, 2] AS copy RETURN [p] AS l, count(*)";

    expectRows(query, {
        {"[(0), [1], (6)]", "2"},
        {"[(0), [2], (2)]", "2"},
        {"[(0), [3], (3)]", "2"},
    });
}

TEST_F(ListOfPathsTest, appendsAPathToAList) {
    std::string query(REMY_KNOWS_ADAM);
    query += "RETURN size([p] + p), size(p + [p]), length(([p] + p)[1])";

    expectRows(query, {{"2", "2", "1"}});
}

TEST_F(ListOfPathsTest, selectsAPathInACase) {
    std::string query(REMY_KNOWS_ADAM);
    query += "RETURN nodes(CASE WHEN length(p) = 1 THEN p END), nodes(CASE WHEN length(p) > 1 THEN p END)";

    expectRows(query, {{"[0, 1]", "null"}});
}

TEST_F(ListOfPathsTest, readsThePathATaggedCellHolds) {
    std::string query(REMY_KNOWS_ADAM);
    query += "WITH [1, p] AS mixed RETURN length(mixed[1]), nodes(mixed[1]), relationships(mixed[1])";

    expectRows(query, {{"1", "[0, 1]", "[0]"}});
}

TEST_F(ListOfPathsTest, rejectsAPathFunctionOverATaggedCellHoldingNoPath) {
    runQueryExpectingError("WITH [1, 'a'] AS mixed RETURN nodes(mixed[1])",
                           "nodes(), relationships() and length() read a path");
}
