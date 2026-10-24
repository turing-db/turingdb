#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// = and <> take any two values: two that can never be equal answer false and true, and a null
// on either side answers null, in a binary comparison and in a CASE alike. A list or map cell
// holds a value of its own type, a node or an edge among them, so it is compared row by row.
// On simpledb only Remy and Adam know a person well.
class EqualityAcrossTypesTest : public CallV3Test {
};

TEST_F(EqualityAcrossTypesTest, comparesANodeWithValuesOfOtherTypes) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) "
             "RETURN n = 'a', n <> 'a', n = [1], n = {k: 1}, n = true",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"false", "true", "false", "false", "false"}}));
}

TEST_F(EqualityAcrossTypesTest, comparesANodeWithAnEdge) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'})-[e:KNOWS_WELL]->() RETURN n = e, e <> n", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"false", "true"}}));
}

TEST_F(EqualityAcrossTypesTest, answersNullForANodeAnOptionalMatchMissed) {
    StringRowSink sink;
    runQuery("MATCH (a:Person) WHERE a.name IN ['Remy', 'Luc'] OPTIONAL MATCH (a)-[:KNOWS_WELL]->(b) "
             "RETURN a.name AS name, b = 'x' ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Luc", "null"},
        {"Remy", "false"},
    }));
}

TEST_F(EqualityAcrossTypesTest, comparesAMapValueWithANode) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) WITH n, {k: n, j: 'x'} AS m "
             "RETURN m.k = n, m.j = n, m.k <> n, n = m.k",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"true", "false", "false", "true"}}));
}

TEST_F(EqualityAcrossTypesTest, comparesTwoPaths) {
    StringRowSink sink;
    runQuery("MATCH p = (a:Person)-[:KNOWS_WELL]->(:Person), q = (c:Person)-[:KNOWS_WELL]->(:Person) "
             "RETURN a.name AS first, c.name AS second, p = q, p <> q, p = 1 ORDER BY first, second",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "Adam", "true", "false", "false"},
        {"Adam", "Remy", "false", "true", "false"},
        {"Remy", "Adam", "false", "true", "false"},
        {"Remy", "Remy", "true", "false", "false"},
    }));
}

TEST_F(EqualityAcrossTypesTest, answersNullForAPathAnOptionalMatchMissed) {
    StringRowSink sink;
    runQuery("MATCH (a:Person) WHERE a.name IN ['Remy', 'Luc'] OPTIONAL MATCH p = (a)-[:KNOWS_WELL]->(b) "
             "RETURN a.name AS name, p = p ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Luc", "null"},
        {"Remy", "true"},
    }));
}

TEST_F(EqualityAcrossTypesTest, comparesACaseSubjectWithANode) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) WITH n, {k: n, j: 'x'} AS m "
             "RETURN CASE 'x' WHEN n THEN 'node' ELSE 'other' END, "
             "CASE m.k WHEN n THEN 'node' ELSE 'other' END, "
             "CASE n WHEN m.j THEN 'text' WHEN m.k THEN 'node' ELSE 'other' END",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"other", "node", "node"}}));
}

TEST_F(EqualityAcrossTypesTest, comparesTwoWalks) {
    StringRowSink sink;
    runQuery("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->+(:Person) RETURN DISTINCT p = p, p = 'a'", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"true", "false"}}));
}

TEST_F(EqualityAcrossTypesTest, comparesACellWithAnEmbedding) {
    StringRowSink sink;
    runQuery("WITH {k: (1.0, 2.0), j: 'x'} AS m "
             "RETURN m.k = (1.0, 2.0), m.k = (1.0, 3.0), m.j = (1.0, 2.0), "
             "CASE m.k WHEN (1.0, 2.0) THEN 'same' ELSE 'other' END",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"true", "false", "false", "same"}}));

    StringRowSink listSink;
    runQuery("UNWIND [(1.0, 2.0), 'a'] AS x RETURN x = (1.0, 2.0)", listSink);

    EXPECT_EQ(listSink.getRows(), (Rows {{"true"}, {"false"}}));
}
