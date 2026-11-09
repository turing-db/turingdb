#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyCellJoinsAnyValueTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void readRemyAdamEdgeID(std::string& edgeID) {
        RowSink sink;
        runQuery("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN id(e)", sink);

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), 1);

        edgeID = rows[0][0];
    }
};

TEST_F(AnyCellJoinsAnyValueTest, appendsAMapToTheListACellHolds) {
    expectRows("RETURN [[1], 'a'][0] + {k: 1} AS a, {k: 1} + [[1], 'a'][0] AS b", {{"[1, {k: 1}]", "[{k: 1}, 1]"}});
}

TEST_F(AnyCellJoinsAnyValueTest, appendsANodeToTheListACellHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [[1], 'a'][0] + n AS a, n + [[1], 'a'][0] AS b",
               {{"[1, 0]", "[0, 1]"}});
}

TEST_F(AnyCellJoinsAnyValueTest, appendsAnEdgeToTheListACellHolds) {
    std::string edgeID;
    readRemyAdamEdgeID(edgeID);

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "RETURN [[1], 'a'][0] + e AS a, e + [[1], 'a'][0] AS b",
               {{"[1, " + edgeID + "]", "[" + edgeID + ", 1]"}});
}

TEST_F(AnyCellJoinsAnyValueTest, appendsAPathToTheListACellHolds) {
    std::string edgeID;
    readRemyAdamEdgeID(edgeID);

    const std::string path = "(0), [" + edgeID + "], (1)";
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "RETURN [[1], 'a'][0] + p AS a, p + [[1], 'a'][0] AS b",
               {{"[1, " + path + "]", "[" + path + ", 1]"}});
}

TEST_F(AnyCellJoinsAnyValueTest, appendsAPointToTheListACellHolds) {
    expectRows("RETURN [[1], 'a'][0] + point({x: 1, y: 2}) AS a, point({x: 1, y: 2}) + [[1], 'a'][0] AS b",
               {{"[1, point({x: 1.0, y: 2.0, crs: 'cartesian'})]", "[point({x: 1.0, y: 2.0, crs: 'cartesian'}), 1]"}});
}

TEST_F(AnyCellJoinsAnyValueTest, appendsAnEmbeddingToTheListACellHolds) {
    expectRows("RETURN size([[1], 'a'][0] + (1, 5)) AS a, size((1, 5) + [[1, 2], 'a'][0]) AS b", {{"2", "3"}});
}

TEST_F(AnyCellJoinsAnyValueTest, appendsAValueToTheListAMapValueHolds) {
    expectRows("WITH {l: [1]} AS m RETURN m.l + {k: 2}", {{"[1, {k: 2}]"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, {l: [1]} AS m RETURN n + m.l", {{"[0, 1]"}});
}

TEST_F(AnyCellJoinsAnyValueTest, appendsAValueToTheListEachRowHolds) {
    expectRows("UNWIND [[[1], 'a'], [[2, 3], 'b']] AS xs RETURN xs[0] + {k: 1}", {{"[1, {k: 1}]"}, {"[2, 3, {k: 1}]"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [[[1], 'a'], [[2, 3], 'b']] AS xs RETURN xs[0] + n",
               {{"[1, 0]"}, {"[2, 3, 0]"}});
}

TEST_F(AnyCellJoinsAnyValueTest, answersNullForANullCell) {
    expectRows("RETURN [null, 1, 'a'][0] + {k: 1} AS a, {k: 1} + [null, 1, 'a'][0] AS b", {{"null", "null"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [null, 1, 'a'][0] + n AS a, n + [null, 1, 'a'][0] AS b",
               {{"null", "null"}});
}

TEST_F(AnyCellJoinsAnyValueTest, answersNullForAnEntityAnOptionalMatchMissed) {
    expectRows("OPTIONAL MATCH (n:Person {name: 'Nobody'}) RETURN [[1], 'a'][0] + n AS a, n + [[1], 'a'][0] AS b",
               {{"null", "null"}});
    expectRows("OPTIONAL MATCH (:Person {name: 'Nobody'})-[e]->() RETURN [[1], 'a'][0] + e", {{"null"}});
    expectRows("OPTIONAL MATCH p = (:Person {name: 'Nobody'})-->() RETURN [[1], 'a'][0] + p", {{"null"}});
}

TEST_F(AnyCellJoinsAnyValueTest, raisesWhereTheCellHoldsNoList) {
    runQueryExpectingError("RETURN [[1], 'a'][1] + {k: 1}", "'String' and 'Map'");
    runQueryExpectingError("RETURN {k: 1} + [[1], 'a'][1]", "'Map' and 'String'");
    runQueryExpectingError("RETURN [1, 'a'][0] + {k: 1}", "'Integer' and 'Map'");
    runQueryExpectingError("RETURN [{k: 1}, 'a'][0] + {k: 2}", "'Map' and 'Map'");
    runQueryExpectingError("MATCH (n:Person {name: 'Remy'}) RETURN [1, 'a'][0] + n", "'Integer' and 'NodePattern'");
    runQueryExpectingError("MATCH (n:Person {name: 'Remy'}) RETURN n + ['a', 1][0]", "'NodePattern' and 'String'");
    runQueryExpectingError("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN [true, 'a'][0] + e",
                           "'Bool' and 'EdgePattern'");
    runQueryExpectingError("RETURN [1.5, 'a'][0] + point({x: 1, y: 2})", "'Double' and 'Point'");
    runQueryExpectingError("RETURN ['a', 1][0] + (1, 5)", "'String' and 'Embedding'");
    runQueryExpectingError("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN p + [1, 'a'][0]",
                           "'GraphPath' and 'Integer'");
}

TEST_F(AnyCellJoinsAnyValueTest, raisesOnTheRowWhoseCellHoldsNoList) {
    runQueryExpectingError("UNWIND [[[1], 'a'], ['b', [2]]] AS xs RETURN xs[0] + {k: 1}", "'String' and 'Map'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
