#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class CreateEdgePropertyTest : public CallV3Test {
};

// CREATE (a)-[e:KNOWS {since: 1999}]->(b) RETURN e.since: the graph carries no property
// called 'since' until this query commits, so the read resolves against the type the CREATE
// is about to write rather than against the schema it starts from.
TEST_F(CreateEdgePropertyTest, readsAPropertyTheCreateWroteOnAnEdge) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Eva'})-[e:KNOWS {since: 1999}]->(b:Person {name: 'Fay'}) RETURN e.since",
             sink);

    const std::vector<StringRowSink::Row> expected {{"1999"}};
    EXPECT_EQ(sink.getRows(), expected);

    StringRowSink committed;
    runQuery("MATCH (:Person {name: 'Eva'})-[e:KNOWS]->(:Person {name: 'Fay'}) RETURN e.since", committed);
    EXPECT_EQ(committed.getRows(), expected);
}

// The same read one part below the cut, where the edge is named by a provisional ID and its
// value comes out of the write buffer.
TEST_F(CreateEdgePropertyTest, readsANewEdgePropertyBelowAPartCut) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Gus'})-[e:KNOWS {since: 2001}]->(b:Person {name: 'Hal'}) "
             "WITH e RETURN type(e), e.since",
             sink);

    const std::vector<StringRowSink::Row> expected {{"KNOWS", "2001"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A property the graph already carries on its edges is read the same way, from the value the
// CREATE wrote rather than from whatever the edge ID collides with.
TEST_F(CreateEdgePropertyTest, readsAnExistingPropertyTheCreateWroteOnAnEdge) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ida'})-[e:KNOWS {name: 'Ida -> Jon'}]->(b:Person {name: 'Jon'}) "
             "RETURN e.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ida -> Jon"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A name no property carries and no CREATE writes reads null, on an edge as on a node.
TEST_F(CreateEdgePropertyTest, readsNullForAPropertyNothingWrites) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Kit'})-[e:KNOWS {since: 2003}]->(b:Person {name: 'Lou'}) "
             "RETURN e.untilWhen",
             sink);

    const std::vector<StringRowSink::Row> expected {{"null"}};
    EXPECT_EQ(sink.getRows(), expected);
}
