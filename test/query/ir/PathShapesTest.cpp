#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// length(), nodes() and relationships() over the shapes a named path takes, and the list
// operators over what they read
class PathShapesTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, Rows expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(PathShapesTest, readsAPathOfOneNode) {
    expectRows("MATCH p = (n:Person {name: 'Remy'}) RETURN length(p), nodes(p), relationships(p)", {{"0", "[0]", "[]"}});
}

TEST_F(PathShapesTest, readsAVariableLengthRelationshipWrittenInBrackets) {
    expectRows("MATCH p = (n:Person)-[*1..2]->(m:Person) RETURN length(p)", {{"1"}, {"1"}, {"2"}, {"2"}, {"2"}});
}

TEST_F(PathShapesTest, readsTheSizesOfItsLists) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN size(nodes(p)), size(relationships(p))", {{"2", "1"}, {"2", "1"}});
}

TEST_F(PathShapesTest, readsTheLastNodeFromTheEnd) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN nodes(p)[-1]", {{"0"}, {"1"}});
}

TEST_F(PathShapesTest, slicesTheNodesOfAWalk) {
    expectRows("MATCH p = (n:Person)-[e]->+(m:Person) WHERE length(p) = 2 RETURN nodes(p)[1..]", {{"[0, 1]"}, {"[1, 0]"}, {"[6, 0]"}});
}

TEST_F(PathShapesTest, unwindsNoNodeOfANullPath) {
    expectRows("UNWIND nodes(null) AS x RETURN x", {});
}

TEST_F(PathShapesTest, stillReadsTheLengthOfAString) {
    expectRows("RETURN length('abc')", {{"3"}});
}
