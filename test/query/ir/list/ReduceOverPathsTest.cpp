#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// reduce and allReduce over the paths of each person: one INTERESTED_IN hop, or a KNOWS_WELL
// hop and then one. Remy and Adam know each other, so their path lengths are 1, 1, 1, 2, 2
// and 1, 1, 2, 2, 2, and everyone else's are all 1.
class ReduceOverPathsTest : public CallV3Test {
protected:
    static constexpr std::string_view PATHS_OF_EACH_PERSON =
        "MATCH p=(n:Person)-[:KNOWS_WELL*0..1]->(:Person)-[:INTERESTED_IN]->(:Interest) ";

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << query;
    }
};

TEST_F(ReduceOverPathsTest, findsTheLongestPathOfEachPerson) {
    std::string query(PATHS_OF_EACH_PERSON);
    query += "WITH n, collect(length(p)) AS lengths ";
    query += "RETURN n.name, reduce(longest = 0, l IN lengths | CASE WHEN l > longest THEN l ELSE longest END)";

    expectRows(query, {
        {"Remy", "2"},
        {"Adam", "2"},
        {"Maxime", "1"},
        {"Luc", "1"},
        {"Martina", "1"},
        {"Suhas", "1"},
        {"Cyrus", "1"},
        {"Doruk", "1"},
    });
}

TEST_F(ReduceOverPathsTest, sumsThePathLengthsOfEachPerson) {
    std::string query(PATHS_OF_EACH_PERSON);
    query += "WITH n, collect(length(p)) AS lengths ";
    query += "RETURN n.name, reduce(total = 0, l IN lengths | total + l)";

    expectRows(query, {
        {"Remy", "7"},
        {"Adam", "8"},
        {"Maxime", "2"},
        {"Luc", "2"},
        {"Martina", "1"},
        {"Suhas", "2"},
        {"Cyrus", "2"},
        {"Doruk", "1"},
    });
}

TEST_F(ReduceOverPathsTest, findsTheLongestPathOfAll) {
    std::string query(PATHS_OF_EACH_PERSON);
    query += "WITH collect(length(p)) AS lengths ";
    query += "RETURN size(lengths), reduce(longest = 0, l IN lengths | CASE WHEN l > longest THEN l ELSE longest END)";

    expectRows(query, {{"20", "2"}});
}

TEST_F(ReduceOverPathsTest, decidesWhetherEveryPathSoFarIsOneHop) {
    std::string query(PATHS_OF_EACH_PERSON);
    query += "WITH n, collect(length(p)) AS lengths ";
    query += "RETURN n.name, allReduce(longest = 0, l IN lengths | CASE WHEN l > longest THEN l ELSE longest END, longest <= 1)";

    expectRows(query, {
        {"Remy", "false"},
        {"Adam", "false"},
        {"Maxime", "true"},
        {"Luc", "true"},
        {"Martina", "true"},
        {"Suhas", "true"},
        {"Cyrus", "true"},
        {"Doruk", "true"},
    });
}

TEST_F(ReduceOverPathsTest, decidesWhetherTheRunningTotalStaysUnderABound) {
    std::string query(PATHS_OF_EACH_PERSON);
    query += "WITH n, collect(length(p)) AS lengths ";
    query += "RETURN n.name, allReduce(total = 0, l IN lengths | total + l, total <= 4)";

    expectRows(query, {
        {"Remy", "false"},
        {"Adam", "false"},
        {"Maxime", "true"},
        {"Luc", "true"},
        {"Martina", "true"},
        {"Suhas", "true"},
        {"Cyrus", "true"},
        {"Doruk", "true"},
    });
}

TEST_F(ReduceOverPathsTest, foldsNoLength) {
    expectRows("MATCH p=(:Person {name: 'Remy'})-[:INTERESTED_IN]->(:Interest) "
               "WITH [l IN collect(length(p)) WHERE l > 1] AS lengths "
               "RETURN reduce(longest = 0, l IN lengths | CASE WHEN l > longest THEN l ELSE longest END), "
               "allReduce(longest = 0, l IN lengths | CASE WHEN l > longest THEN l ELSE longest END, longest <= 1)",
               {{"0", "true"}});
}

// The folds above over the paths themselves.
// Disabled: collect() has no overload for a path.
TEST_F(ReduceOverPathsTest, DISABLED_findsTheLongestOfTheCollectedPaths) {
    std::string query(PATHS_OF_EACH_PERSON);
    query += "WITH n, collect(p) AS paths ";
    query += "RETURN n.name, reduce(longest = 0, p IN paths | CASE WHEN length(p) > longest THEN length(p) ELSE longest END), "
             "allReduce(total = 0, p IN paths | total + length(p), nodes(p)[0] = n AND total <= 4)";

    expectRows(query, {
        {"Remy", "2", "false"},
        {"Adam", "2", "false"},
        {"Maxime", "1", "true"},
        {"Luc", "1", "true"},
        {"Martina", "1", "true"},
        {"Suhas", "1", "true"},
        {"Cyrus", "1", "true"},
        {"Doruk", "1", "true"},
    });
}

// Remy has two longest paths, so only what they share is read off the one kept.
// Disabled: collect() has no overload for a path.
TEST_F(ReduceOverPathsTest, DISABLED_keepsTheLongestPathAsTheAccumulator) {
    std::string query(PATHS_OF_EACH_PERSON);
    query += "WITH n, collect(p) AS paths ";
    query += "WITH n, reduce(longest = null, p IN paths | "
             "CASE WHEN longest IS NULL OR length(p) > length(longest) THEN p ELSE longest END) AS longest ";
    query += "RETURN n.name, length(longest), nodes(longest)[0].name, nodes(longest)[-2].name";

    expectRows(query, {
        {"Remy", "2", "Remy", "Adam"},
        {"Adam", "2", "Adam", "Remy"},
        {"Maxime", "1", "Maxime", "Maxime"},
        {"Luc", "1", "Luc", "Luc"},
        {"Martina", "1", "Martina", "Martina"},
        {"Suhas", "1", "Suhas", "Suhas"},
        {"Cyrus", "1", "Cyrus", "Cyrus"},
        {"Doruk", "1", "Doruk", "Doruk"},
    });
}

// Disabled: collect() has no overload for a path.
TEST_F(ReduceOverPathsTest, DISABLED_collectsThePathsOfAPerson) {
    expectRows("MATCH p=(:Person {name: 'Remy'})-[:INTERESTED_IN]->(:Interest) RETURN [x IN collect(p) | length(x)]",
               {{"[1, 1, 1]"}});
}

// Disabled: lowering cannot read a path column into a list.
TEST_F(ReduceOverPathsTest, DISABLED_foldsAListOfOnePath) {
    expectRows("MATCH p=(:Person {name: 'Remy'})-[:INTERESTED_IN]->(:Interest) RETURN reduce(total = 0, x IN [p] | total + length(x))",
               {{"1"}, {"1"}, {"1"}});
}

TEST_F(ReduceOverPathsTest, DISABLED_foldsAListLiteralOfPaths) {
    expectRows("MATCH p=(r:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person)-[:INTERESTED_IN]->(:Interest {name: 'Bio'}), "
               "q=(r)-[:INTERESTED_IN]->(:Interest {name: 'Ghosts'}) "
               "RETURN reduce(longest = 0, x IN [p, q] | CASE WHEN length(x) > longest THEN length(x) ELSE longest END), "
               "reduce(total = 0, x IN [p, q] | total + length(x))",
               {{"2", "3"}});
}

// Disabled: a COLLECT subquery that returns a path trips the codegen assertion 'readsAPath'.
TEST_F(ReduceOverPathsTest, DISABLED_collectsThePathsOfACollectSubquery) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN [x IN COLLECT { MATCH q=(n)-[:INTERESTED_IN]->(:Interest) RETURN q } | length(x)]",
               {{"[1, 1, 1]"}});
}

TEST_F(ReduceOverPathsTest, DISABLED_foldsThePathsOfACollectSubquery) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN reduce(total = 0, x IN COLLECT { "
               "MATCH q=(n)-[:KNOWS_WELL*0..1]->(:Person)-[:INTERESTED_IN]->(:Interest) RETURN q "
               "} | total + length(x))",
               {{"7"}});
}

// Disabled: the parser takes no path name in a pattern comprehension.
TEST_F(ReduceOverPathsTest, DISABLED_collectsThePathsOfAPatternComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN [x IN [q = (n)-[:INTERESTED_IN]->(:Interest) | q] | length(x)]",
               {{"[1, 1, 1]"}});
}

TEST_F(ReduceOverPathsTest, DISABLED_foldsThePathsOfAPatternComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN reduce(total = 0, x IN [q = (n)-[:KNOWS_WELL*0..1]->(:Person)-[:INTERESTED_IN]->(:Interest) | q] "
               "| total + length(x))",
               {{"7"}});
}

TEST_F(ReduceOverPathsTest, DISABLED_decidesOverThePathsOfAPatternComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN allReduce(total = 0, x IN [q = (n)-[:INTERESTED_IN]->(:Interest) | q] | total + length(x), total < 3)",
               {{"false"}});
}
