#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A map literal may hold a path as one of its values. The sink renders a path value as
// <(node), [edge], ...>
class PathMapTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(PathMapTest, mapsAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN {route: p, who: n.name}",
               {{"{route: <(0), [0], (1)>, who: Remy}"}, {"{route: <(1), [4], (0)>, who: Adam}"}});
}

TEST_F(PathMapTest, mapsAWalk) {
    expectRows("MATCH p = (n:Person {name: 'Maxime'})-[*]->(m) RETURN {route: p}",
               {{"{route: <(8), [8], (4)>}"}, {"{route: <(8), [9], (7)>}"}});
}

TEST_F(PathMapTest, mapsAMissedPathAsNull) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN n.name, {route: p}",
               {{"Luc", "{route: null}"}, {"Remy", "{route: <(0), [0], (1)>}"}});
}

TEST_F(PathMapTest, mapsAPathReadOutOfAList) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH collect(p) AS paths RETURN {first: paths[0], count: size(paths)}",
               {{"{count: 2, first: <(0), [0], (1)>}"}});
}

TEST_F(PathMapTest, nestsAMapOfAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN {trip: {route: p}}, [{route: p}]",
               {{"{trip: {route: <(0), [0], (1)>}}", "[{route: <(0), [0], (1)>}]"},
                {"{trip: {route: <(1), [4], (0)>}}", "[{route: <(1), [4], (0)>}]"}});
}

TEST_F(PathMapTest, dedupsMapsOfPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person), (x:Person) RETURN DISTINCT {route: p}",
               {{"{route: <(0), [0], (1)>}"}, {"{route: <(1), [4], (0)>}"}});
}

TEST_F(PathMapTest, comparesMapsOfPaths) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) MATCH q = (a:Person {name: 'Remy'})-[x]->(b:Person) RETURN n.name, {route: p} = {route: q}",
               {{"Adam", "false"}, {"Remy", "true"}});
}

TEST_F(PathMapTest, storesNoMapOfPaths) {
    runWriteExpectingError("MATCH p = (n:Person {name: 'Remy'})-[e]->(m:Person) SET n.trip = {route: p}",
                           "a path is not a property value");
}
