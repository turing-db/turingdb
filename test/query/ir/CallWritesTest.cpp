#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallWritesTest : public CallV3Test {
};

TEST_F(CallWritesTest, createsANodePerYieldedRow) {
    runWrite("CALL db.edgeTypes() YIELD edgeType CREATE (m:Marker)");

    StringRowSink sink;
    runQuery("MATCH (m:Marker) RETURN count(m)", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The SET reads the matched node carried past the call, once per yielded row.
TEST_F(CallWritesTest, setsACarriedVariable) {
    runWrite("MATCH (n {name: 'Remy'}) CALL db.edgeTypes() YIELD edgeType SET n.age = 50");

    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) RETURN n.age", sink);

    const std::vector<StringRowSink::Row> expected {{"50"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallWritesTest, setsAReMatchedYieldedNode) {
    runWrite("CALL db.getNodes([1]) YIELD id AS a MATCH (a) SET a.age = 41");

    StringRowSink sink;
    runQuery("MATCH (n {name: 'Adam'}) RETURN n.age", sink);

    const std::vector<StringRowSink::Row> expected {{"41"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The pattern binds to the very edge the call yielded, so that one edge goes and the other
// seventeen stay.
TEST_F(CallWritesTest, deletesAReMatchedYieldedEdge) {
    runWrite("CALL db.getEdges([0]) YIELD id AS e MATCH ()-[e]->() DELETE e");

    StringRowSink sink;
    runQuery("MATCH ()-[e]->() RETURN count(e)", sink);

    const std::vector<StringRowSink::Row> expected {{"17"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallWritesTest, refusesToDeleteAConnectedYieldedNodeWithoutDetach) {
    runWriteExpectingError("CALL db.getNodes([17]) YIELD id AS a MATCH (a) DELETE a", "DETACH DELETE");
}
