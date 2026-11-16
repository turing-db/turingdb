#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyCellAppendsAnyValueTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void readSingleValue(std::string_view query, std::string& value) {
        RowSink sink;
        runQuery(query, sink);

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), 1) << query;
        ASSERT_EQ(rows[0].size(), 1) << query;

        value = rows[0][0];
    }

    void expectTypeError(std::string_view query, std::string_view left, std::string_view right) {
        const std::string reason = "for '+': '" + std::string(left) + "' and '" + std::string(right) + "'";
        runQueryExpectingError(query, reason);
    }
};

TEST_F(AnyCellAppendsAnyValueTest, appendsAMapToTheListACellHolds) {
    expectRows("RETURN [[1], 'a'][0] + {k: 1} AS a, {k: 1} + [[1], 'a'][0] AS b", {{"[1, {k: 1}]", "[{k: 1}, 1]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsANodeToTheListACellHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [[1], 'a'][0] + n AS a, n + [[1], 'a'][0] AS b",
               {{"[1, 0]", "[0, 1]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsAnEdgeToTheListACellHolds) {
    std::string edge;
    readSingleValue("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN id(e)", edge);

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "RETURN [[1], 'a'][0] + e AS a, e + [[1], 'a'][0] AS b",
               {{"[1, " + edge + "]", "[" + edge + ", 1]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsAPathToTheListACellHolds) {
    std::string edge;
    readSingleValue("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN id(e)", edge);

    const std::string path = "(0), [" + edge + "], (1)";
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "RETURN [[1], 'a'][0] + p AS a, p + [[1], 'a'][0] AS b",
               {{"[1, " + path + "]", "[" + path + ", 1]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsAPointToTheListACellHolds) {
    expectRows("RETURN [[1], 'a'][0] + point({x: 1, y: 2}) AS a, point({x: 1, y: 2}) + [[1], 'a'][0] AS b",
               {{"[1, point({x: 1.0, y: 2.0, crs: 'cartesian'})]", "[point({x: 1.0, y: 2.0, crs: 'cartesian'}), 1]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsAnEmbeddingToTheListACellHolds) {
    expectRows("WITH [[1], 'a'][0] + (1.0, 0.0) AS a, (1.0, 0.0) + [[1, 2], 'a'][0] AS b "
               "RETURN size(a), cosine_similarity(a[1], (1.0, 0.0)), size(b), cosine_similarity(b[0], (1.0, 0.0))",
               {{"2", "1.000000", "3", "1.000000"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsALabelIDToTheListACellHolds) {
    std::string labelList;
    readSingleValue("CALL db.labels() YIELD id, label WITH id, label WHERE label = 'Person' RETURN [id]", labelList);
    ASSERT_GE(labelList.size(), 2);

    const std::string label = labelList.substr(1, labelList.size() - 2);
    expectRows("CALL db.labels() YIELD id, label WITH id, label WHERE label = 'Person' "
               "RETURN [[1], 'a'][0] + id AS a, id + [[1], 'a'][0] AS b",
               {{"[1, " + label + "]", "[" + label + ", 1]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsAValueToTheListAMapValueHolds) {
    expectRows("WITH {l: [1]} AS m RETURN m.l + {k: 2}", {{"[1, {k: 2}]"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, {l: [1]} AS m RETURN n + m.l", {{"[0, 1]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, appendsAValueToTheListEachRowHolds) {
    expectRows("UNWIND [[[1], 'a'], [[2, 3], 'b']] AS xs RETURN xs[0] + {k: 1}", {{"[1, {k: 1}]"}, {"[2, 3, {k: 1}]"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [[[1], 'a'], [[2, 3], 'b']] AS xs RETURN xs[0] + n",
               {{"[1, 0]"}, {"[2, 3, 0]"}});
}

TEST_F(AnyCellAppendsAnyValueTest, answersNullForACellHoldingNull) {
    expectRows("RETURN [null, 1, 'a'][0] + {k: 1} AS a, {k: 1} + [null, 1, 'a'][0] AS b", {{"null", "null"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [null, 1, 'a'][0] + n AS a, n + [null, 1, 'a'][0] AS b",
               {{"null", "null"}});
}

TEST_F(AnyCellAppendsAnyValueTest, answersNullForAnEntityAnOptionalMatchMissed) {
    expectRows("OPTIONAL MATCH (n:Person {name: 'Nobody'}) RETURN [[1], 'a'][0] + n AS a, n + [[1], 'a'][0] AS b",
               {{"null", "null"}});
    expectRows("OPTIONAL MATCH (:Person {name: 'Nobody'})-[e]->() RETURN [[1], 'a'][0] + e", {{"null"}});
    expectRows("OPTIONAL MATCH p = (:Person {name: 'Nobody'})-->() RETURN [[1], 'a'][0] + p", {{"null"}});
}

TEST_F(AnyCellAppendsAnyValueTest, raisesWhereTheCellHoldsNoList) {
    expectTypeError("RETURN [[1], 'a'][1] + {k: 1}", "String", "Map");
    expectTypeError("RETURN {k: 1} + [[1], 'a'][1]", "Map", "String");
    expectTypeError("RETURN [1, 'a'][0] + {k: 1}", "Integer", "Map");
    expectTypeError("RETURN [{k: 1}, 'a'][0] + {k: 2}", "Map", "Map");
    expectTypeError("MATCH (n:Person {name: 'Remy'}) RETURN [1, 'a'][0] + n", "Integer", "NodePattern");
    expectTypeError("MATCH (n:Person {name: 'Remy'}) RETURN n + ['a', 1][0]", "NodePattern", "String");
    expectTypeError("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN [true, 'a'][0] + e",
                    "Bool",
                    "EdgePattern");
    expectTypeError("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN p + [1, 'a'][0]",
                    "GraphPath",
                    "Integer");
    expectTypeError("RETURN [1.5, 'a'][0] + point({x: 1, y: 2})", "Double", "Point");
    expectTypeError("RETURN [1, 'a'][0] + (1.0, 0.0)", "Integer", "Embedding");
}

TEST_F(AnyCellAppendsAnyValueTest, raisesOnTheRowWhoseCellHoldsNoList) {
    expectTypeError("UNWIND [[[1], 'a'], ['b', [2]]] AS xs RETURN xs[0] + {k: 1}", "String", "Map");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
