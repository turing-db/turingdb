#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A named path groups, dedups and counts distinct by its value: the sequence of nodes and
// relationships it runs through
class PathGroupingKeysTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(PathGroupingKeysTest, groupsOnAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN p, count(*)",
               {{"(0), [0], (1)", "1"}, {"(1), [4], (0)", "1"}});
}

TEST_F(PathGroupingKeysTest, groupsTheRowsOfOnePathTogether) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN p, count(x)",
               {{"(0), [0], (1)", "8"}, {"(1), [4], (0)", "8"}});
}

TEST_F(PathGroupingKeysTest, groupsOnAPathBesideAnotherKey) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN n.name, p, count(*)",
               {{"Remy", "(0), [0], (1)", "1"}, {"Adam", "(1), [4], (0)", "1"}});
}

TEST_F(PathGroupingKeysTest, groupsOnAWalk) {
    expectRows("MATCH p = (n:Person {name: 'Maxime'})-[*]->(m) MATCH (x:Person) RETURN p, count(x)",
               {{"(8), [8], (4)", "8"}, {"(8), [9], (7)", "8"}});
}

TEST_F(PathGroupingKeysTest, groupsOnAPathAWithPublished) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) WITH p, count(x) AS c RETURN length(p), c",
               {{"1", "8"}, {"1", "8"}});
}

TEST_F(PathGroupingKeysTest, readsTheGroupedPathAfterAWith) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) WITH p, count(x) AS c RETURN nodes(p), c",
               {{"0, 1", "8"}, {"1, 0", "8"}});
}

TEST_F(PathGroupingKeysTest, collectsBesideAPathKey) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN p, size(collect(x.name))",
               {{"(0), [0], (1)", "8"}, {"(1), [4], (0)", "8"}});
}

TEST_F(PathGroupingKeysTest, groupsTheMissedPathsAsOneNull) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN p, count(n)",
               {{"(0), [0], (1)", "1"}, {"(1), [4], (0)", "1"}, {"null", "6"}});
}

TEST_F(PathGroupingKeysTest, dedupsAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN DISTINCT p",
               {{"(0), [0], (1)"}, {"(1), [4], (0)"}});
}

TEST_F(PathGroupingKeysTest, dedupsAWalk) {
    expectRows("MATCH p = (n:Person {name: 'Maxime'})-[*]->(m) MATCH (x:Person) RETURN DISTINCT p",
               {{"(8), [8], (4)"}, {"(8), [9], (7)"}});
}

TEST_F(PathGroupingKeysTest, tellsApartTheTwoDirectionsOfAnEdge) {
    expectRows("MATCH p = (n:Person)-[e]-(m:Person) RETURN DISTINCT p",
               {{"(0), [0], (1)"}, {"(1), [0], (0)"}, {"(1), [4], (0)"}, {"(0), [4], (1)"}});
}

TEST_F(PathGroupingKeysTest, countsDistinctPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN count(DISTINCT p)", {{"2"}});
}

TEST_F(PathGroupingKeysTest, countsDistinctPathsPerGroup) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) WHERE x.name IN ['Remy', 'Luc'] RETURN x.name, count(DISTINCT p)",
               {{"Remy", "2"}, {"Luc", "2"}});
}

TEST_F(PathGroupingKeysTest, countsNoMissedPathAsDistinct) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN count(DISTINCT p)", {{"2"}});
}

TEST_F(PathGroupingKeysTest, groupsOnAnAliasedPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN p AS q, count(*)",
               {{"(0), [0], (1)", "1"}, {"(1), [4], (0)", "1"}});
}

TEST_F(PathGroupingKeysTest, readsTheLengthOfAGroupedWalk) {
    expectRows("MATCH p = (n:Person {name: 'Adam'})-[*1..2]->(m) WITH p, count(*) AS c WHERE length(p) = 1 RETURN p, c",
               {{"(1), [4], (0)", "1"}, {"(1), [5], (4)", "1"}, {"(1), [6], (5)", "1"}});
}

TEST_F(PathGroupingKeysTest, ordersTheGroupsByTheLengthOfTheirPath) {
    expectRows("MATCH p = (n:Person {name: 'Adam'})-[*1..2]->(m) RETURN p, count(*) ORDER BY length(p) DESC LIMIT 4",
               {{"(1), [4], (0), [0], (1)", "1"},
                {"(1), [4], (0), [1], (6)", "1"},
                {"(1), [4], (0), [2], (2)", "1"},
                {"(1), [4], (0), [3], (3)", "1"}});
}
