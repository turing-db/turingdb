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

// collect(p) gathers paths into a list whose elements are paths, which UNWIND hands back as
// paths. The sink reads a path element as <(node), [edge], ...>
class PathCollectTest : public CallV3Test {
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

TEST_F(PathCollectTest, collectsPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN collect(p)",
               {{"<(0), [0], (1)>, <(1), [4], (0)>"}});
}

TEST_F(PathCollectTest, collectsEachGroupsPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Maxime', 'Luc'] RETURN n.name, collect(p)",
               {{"Maxime", "<(8), [8], (4)>, <(8), [9], (7)>"}, {"Luc", "<(9), [10], (10)>, <(9), [11], (2)>"}});
}

TEST_F(PathCollectTest, collectsWalks) {
    expectRows("MATCH p = (n:Person {name: 'Maxime'})-[*]->(m) RETURN collect(p)",
               {{"<(8), [8], (4)>, <(8), [9], (7)>"}});
}

TEST_F(PathCollectTest, collectsNoMissedPath) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN n.name, collect(p)",
               {{"Remy", "<(0), [0], (1)>"}, {"Luc", ""}});
}

TEST_F(PathCollectTest, collectsNoPathOfAnEmptyMatch) {
    expectRows("MATCH p = (n:Person {name: 'Nobody'})-[e]->(m) RETURN size(collect(p))", {{"0"}});
}

TEST_F(PathCollectTest, collectsDistinctPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN size(collect(DISTINCT p)), size(collect(p))",
               {{"2", "16"}});
}

TEST_F(PathCollectTest, collectsEachGroupsDistinctPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) WHERE x.name IN ['Remy', 'Luc'] RETURN x.name, collect(DISTINCT p)",
               {{"Remy", "<(0), [0], (1)>, <(1), [4], (0)>"}, {"Luc", "<(0), [0], (1)>, <(1), [4], (0)>"}});
}

TEST_F(PathCollectTest, collectsPathsBesideOtherAggregates) {
    expectRows("MATCH p = (n:Person)-[e]->(m) WHERE n.name IN ['Maxime', 'Luc'] RETURN n.name, collect(p), count(*), max(p)",
               {{"Maxime", "<(8), [8], (4)>, <(8), [9], (7)>", "2", "(8), [9], (7)"},
                {"Luc", "<(9), [10], (10)>, <(9), [11], (2)>", "2", "(9), [11], (2)"}});
}

TEST_F(PathCollectTest, unwindsCollectedPathsAsPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths UNWIND paths AS q RETURN q, length(q), nodes(q)",
               {{"(0), [0], (1)", "1", "0, 1"}, {"(1), [4], (0)", "1", "1, 0"}});
}

TEST_F(PathCollectTest, unwindsEachGroupsCollectedWalks) {
    expectRows("MATCH p = (n:Person {name: 'Adam'})-[*1..2]->(m) WITH m, collect(p) AS paths UNWIND paths AS q RETURN m.name, relationships(q)",
               {{"Remy", "4"},
                {"Bio", "5"},
                {"Cooking", "6"},
                {"Adam", "4, 0"},
                {"Ghosts", "4, 1"},
                {"Computers", "4, 2"},
                {"Eighties", "4, 3"}});
}

TEST_F(PathCollectTest, dedupsCollectedLists) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) WITH x, collect(p) AS paths RETURN DISTINCT paths",
               {{"<(0), [0], (1)>, <(1), [4], (0)>"}});
}

TEST_F(PathCollectTest, readsTheCollectedPathsBackAsPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN length(head(collect(p))), nodes(last(collect(p))), relationships(collect(p)[1])",
               {{"1", "1, 0", "4"}});
}

TEST_F(PathCollectTest, readsNoPathPastTheEndOfTheList) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths RETURN paths[5], length(paths[-5])", {{"null", "null"}});
}

TEST_F(PathCollectTest, readsNoPathOutOfAnEmptyList) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] OPTIONAL MATCH p = (n)-[e]->(m:Person) WITH n, collect(p) AS paths RETURN n.name, length(head(paths))",
               {{"Remy", "1"}, {"Luc", "null"}});
}

TEST_F(PathCollectTest, readsTheCollectedPathsInAComprehension) {
    expectRows("MATCH p = (n:Person {name: 'Adam'})-[*1..2]->(m) WITH collect(p) AS paths RETURN [q IN paths WHERE length(q) = 2 | last(nodes(q))], all(q IN paths WHERE nodes(q)[0] = 1)",
               {{"1, 6, 2, 3", "true"}});
}

TEST_F(PathCollectTest, listsAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN [p, 1]",
               {{"<(0), [0], (1)>, 1"}, {"<(1), [4], (0)>, 1"}});
}

TEST_F(PathCollectTest, listsAWalk) {
    expectRows("MATCH p = (n:Person {name: 'Maxime'})-[*]->(m) RETURN [p]",
               {{"<(8), [8], (4)>"}, {"<(8), [9], (7)>"}});
}

TEST_F(PathCollectTest, unwindsAListedPathAsAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) UNWIND [p] AS q RETURN length(q), relationships(q)",
               {{"1", "0"}, {"1", "4"}});
}

TEST_F(PathCollectTest, appendsAPathToCollectedPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths MATCH q = (a:Person {name: 'Remy'})-[x]->(b:Person) RETURN paths + q",
               {{"<(0), [0], (1)>, <(1), [4], (0)>, <(0), [0], (1)>"}});
}

TEST_F(PathCollectTest, storesNoPath) {
    runWriteExpectingError("MATCH p = (n:Person {name: 'Remy'})-[e]->(m:Person) WITH n, collect(p) AS paths SET n.paths = paths",
                           "a path is not a property value");
}

TEST_F(PathCollectTest, findsAPathInCollectedPaths) {
    expectRows("MATCH p = (n:Person {name: 'Remy'})-[e]->(m:Person) WITH collect(p) AS paths MATCH q = (a:Person)-[x]->(b:Person) RETURN q, q IN paths",
               {{"(0), [0], (1)", "true"}, {"(1), [4], (0)", "false"}});
}

TEST_F(PathCollectTest, findsAnUnwoundPathInItsList) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths UNWIND paths AS q RETURN q IN paths",
               {{"true"}, {"true"}});
}

TEST_F(PathCollectTest, findsAMissedPathInNoList) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths MATCH (a:Person {name: 'Luc'}) OPTIONAL MATCH q = (a)-[x]->(b:Person) RETURN q IN paths",
               {{"null"}});
}

TEST_F(PathCollectTest, findsAPathInAListReadOutOfAList) {
    expectRows("MATCH p = (n:Person {name: 'Remy'})-[e]->(m:Person) WITH [collect(p), 1] AS nested MATCH q = (a:Person)-[x]->(b:Person) RETURN q, q IN nested[0]",
               {{"(0), [0], (1)", "true"}, {"(1), [4], (0)", "false"}});
}

TEST_F(PathCollectTest, listsAMissedPathAsNull) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN n.name, [p]",
               {{"Remy", "<(0), [0], (1)>"}, {"Luc", "null"}});
}

TEST_F(PathCollectTest, unwindsAListedMissedPathAsANullPath) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] OPTIONAL MATCH p = (n)-[e]->(m:Person) UNWIND [p] AS q RETURN n.name, q, length(q)",
               {{"Remy", "(0), [0], (1)", "1"}, {"Luc", "null", "null"}});
}

TEST_F(PathCollectTest, collectsPathsBesideOtherTypes) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) UNWIND [p, 1] AS x RETURN collect(x)",
               {{"<(0), [0], (1)>, 1, <(1), [4], (0)>, 1"}});
}

TEST_F(PathCollectTest, dedupsPathsBesideOtherTypes) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) UNWIND [p, 1] AS x RETURN DISTINCT x",
               {{"<(0), [0], (1)>"}, {"<(1), [4], (0)>"}, {"1"}});
}

TEST_F(PathCollectTest, ordersPathsBeforeNumbers) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m:Person) UNWIND [p, 1] AS x RETURN x ORDER BY x",
                      {{"<(0), [0], (1)>"}, {"<(1), [4], (0)>"}, {"1"}, {"1"}});
}

TEST_F(PathCollectTest, addsNoNumberToAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) UNWIND [p, 1] AS x RETURN x + 1",
               {{"null"}, {"null"}, {"2"}, {"2"}});
}

TEST_F(PathCollectTest, ordersListsOfPaths) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m:Person) WITH n, collect(p) AS paths RETURN n.name, paths ORDER BY paths DESC",
                      {{"Adam", "<(1), [4], (0)>"}, {"Remy", "<(0), [0], (1)>"}});
}

TEST_F(PathCollectTest, prependsAPathToCollectedPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths MATCH q = (a:Person {name: 'Remy'})-[x]->(b:Person) RETURN q + paths",
               {{"<(0), [0], (1)>, <(0), [0], (1)>, <(1), [4], (0)>"}});
}

TEST_F(PathCollectTest, listsAPathReadOutOfAListBesideOtherTypes) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths RETURN [1, 'a', paths[0], null]",
               {{"1, a, <(0), [0], (1)>, null"}});
}

TEST_F(PathCollectTest, mapsAListOfOnePathOntoItself) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN [x IN [p] | x]",
               {{"<(0), [0], (1)>"}, {"<(1), [4], (0)>"}});
}
