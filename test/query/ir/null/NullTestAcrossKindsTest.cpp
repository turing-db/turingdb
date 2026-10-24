#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// IS NULL and IS NOT NULL over every kind of column a value can come in, each of which keeps
// its null its own way: a node or an edge in the invalid ID, a path as the path with no
// node, a walk in its invalid handle, a property in an absent value, a list or map cell in
// its tag. On simpledb only Remy and Adam know a person well, and only they carry an age.
class NullTestAcrossKindsTest : public CallV3Test {
};

TEST_F(NullTestAcrossKindsTest, testsANodeAndAnEdgeAnOptionalMatchMissed) {
    StringRowSink sink;
    runQuery("MATCH (a:Person) OPTIONAL MATCH (a)-[e:KNOWS_WELL]->(b) "
             "RETURN a.name AS name, b IS NULL, e IS NOT NULL ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "false", "true"},
        {"Cyrus", "true", "false"},
        {"Doruk", "true", "false"},
        {"Luc", "true", "false"},
        {"Martina", "true", "false"},
        {"Maxime", "true", "false"},
        {"Remy", "false", "true"},
        {"Suhas", "true", "false"},
    }));
}

TEST_F(NullTestAcrossKindsTest, keepsTheRowsAnOptionalMatchMissed) {
    StringRowSink sink;
    runQuery("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(b) WITH a, b WHERE b IS NULL RETURN count(*)", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"6"}}));
}

TEST_F(NullTestAcrossKindsTest, testsANamedPath) {
    StringRowSink sink;
    runQuery("MATCH (a:Person) OPTIONAL MATCH p = (a)-[:KNOWS_WELL]->(b) "
             "RETURN a.name AS name, p IS NULL ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "false"},
        {"Cyrus", "true"},
        {"Doruk", "true"},
        {"Luc", "true"},
        {"Martina", "true"},
        {"Maxime", "true"},
        {"Remy", "false"},
        {"Suhas", "true"},
    }));
}

TEST_F(NullTestAcrossKindsTest, testsAWalk) {
    StringRowSink sink;
    runQuery("MATCH (a:Person) OPTIONAL MATCH p = (a)-[:KNOWS_WELL]->+(b) "
             "RETURN DISTINCT a.name AS name, p IS NOT NULL ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "true"},
        {"Cyrus", "false"},
        {"Doruk", "false"},
        {"Luc", "false"},
        {"Martina", "false"},
        {"Maxime", "false"},
        {"Remy", "true"},
        {"Suhas", "false"},
    }));
}

TEST_F(NullTestAcrossKindsTest, testsAProperty) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) RETURN n.name AS name, n.age IS NULL ORDER BY name", sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "false"},
        {"Cyrus", "true"},
        {"Doruk", "true"},
        {"Luc", "true"},
        {"Martina", "true"},
        {"Maxime", "true"},
        {"Remy", "false"},
        {"Suhas", "true"},
    }));
}

TEST_F(NullTestAcrossKindsTest, testsAListCell) {
    StringRowSink sink;
    runQuery("UNWIND [1, null, 'a'] AS x RETURN x IS NULL, x IS NOT NULL", sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"false", "true"},
        {"true", "false"},
        {"false", "true"},
    }));
}

TEST_F(NullTestAcrossKindsTest, testsAMapValue) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) WITH {k: n.missing, j: n.age} AS m RETURN m.k IS NULL, m.j IS NULL", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"true", "false"}}));
}

TEST_F(NullTestAcrossKindsTest, testsAComparisonAListAndAConstant) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) "
             "RETURN (n.age = 32) IS NULL, (n.age > 1) IS NOT NULL, [n.name] IS NULL, 1 IS NULL, 'a' IS NOT NULL",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"false", "true", "false", "false", "true"}}));
}

TEST_F(NullTestAcrossKindsTest, takesACaseBranchOnANull) {
    StringRowSink sink;
    runQuery("UNWIND [1, null] AS x RETURN CASE x WHEN IS NULL THEN 'missing' ELSE 'present' END", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"present"}, {"missing"}}));
}
