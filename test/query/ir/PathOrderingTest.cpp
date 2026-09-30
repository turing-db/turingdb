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

// A path orders as the list of its alternating nodes and relationships, each by its ID, and
// a null path after every other: that is the order ORDER BY, min and max read
class PathOrderingTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << query;
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(PathOrderingTest, ordersByAPath) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Remy', 'Adam'] RETURN p ORDER BY p LIMIT 5",
                      {{"(0), [0], (1)"}, {"(0), [1], (6)"}, {"(0), [2], (2)"}, {"(0), [3], (3)"}, {"(1), [4], (0)"}});
}

TEST_F(PathOrderingTest, ordersByAPathDescending) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Remy', 'Adam'] RETURN p ORDER BY p DESC LIMIT 2",
                      {{"(1), [6], (5)"}, {"(1), [5], (4)"}});
}

TEST_F(PathOrderingTest, ordersAPathBeforeTheWalksItStarts) {
    expectOrderedRows("MATCH p = (n:Person {name: 'Adam'})-[*1..2]->(m) RETURN p ORDER BY p LIMIT 3",
                      {{"(1), [4], (0)"}, {"(1), [4], (0), [0], (1)"}, {"(1), [4], (0), [1], (6)"}});
}

TEST_F(PathOrderingTest, ordersTheMissedPathsLast) {
    expectOrderedRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam', 'Luc'] OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN p ORDER BY p",
                      {{"(0), [0], (1)"}, {"(1), [4], (0)"}, {"null"}});
}

TEST_F(PathOrderingTest, ordersTheGroupsByTheirPath) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN p, count(x) ORDER BY p DESC",
                      {{"(1), [4], (0)", "8"}, {"(0), [0], (1)", "8"}});
}

TEST_F(PathOrderingTest, reducesThePathsToTheirExtremes) {
    expectRows("MATCH p = (n:Person)-[e]->(m) RETURN min(p), max(p)",
               {{"(0), [0], (1)", "(17), [17], (13)"}});
}

TEST_F(PathOrderingTest, reducesTheWalksToTheirExtremes) {
    expectRows("MATCH p = (n:Person {name: 'Adam'})-[*1..2]->(m) RETURN min(p), max(p)",
               {{"(1), [4], (0)", "(1), [6], (5)"}});
}

TEST_F(PathOrderingTest, reducesEachGroupsPathsToTheirExtremes) {
    expectRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Remy', 'Adam'] RETURN n.name, min(p), max(p)",
               {{"Remy", "(0), [0], (1)", "(0), [3], (3)"}, {"Adam", "(1), [4], (0)", "(1), [6], (5)"}});
}

TEST_F(PathOrderingTest, reducesNoMissedPath) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam', 'Luc'] OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN n.name, max(p)",
               {{"Remy", "(0), [0], (1)"}, {"Adam", "(1), [4], (0)"}, {"Luc", "null"}});
}

TEST_F(PathOrderingTest, reducesOnlyMissedPathsToNull) {
    expectRows("MATCH (n:Person {name: 'Luc'}) OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN min(p)", {{"null"}});
}

TEST_F(PathOrderingTest, reducesTheDistinctPathsAsTheirAll) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN min(DISTINCT p)", {{"(0), [0], (1)"}});
}

TEST_F(PathOrderingTest, reducesAPathBesideACollect) {
    expectRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Remy', 'Adam'] RETURN n.name, size(collect(m.name)), max(p)",
               {{"Remy", "4", "(0), [3], (3)"}, {"Adam", "3", "(1), [6], (5)"}});
}

TEST_F(PathOrderingTest, readsAReducedPathAfterAWith) {
    expectRows("MATCH p = (n:Person)-[e]->(m) WITH max(p) AS q RETURN length(q), nodes(q)", {{"1", "17, 13"}});
}

TEST_F(PathOrderingTest, readsEachGroupsReducedPathAfterAWith) {
    expectRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Remy', 'Adam'] WITH n, max(p) AS q RETURN n.name, nodes(q)",
               {{"Remy", "0, 3"}, {"Adam", "1, 5"}});
}

TEST_F(PathOrderingTest, readsAReducedPathBesideACollectAfterAWith) {
    expectRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Remy', 'Adam'] WITH n, collect(m.name) AS names, min(p) AS q RETURN n.name, size(names), relationships(q)",
               {{"Remy", "4", "0"}, {"Adam", "3", "4"}});
}
