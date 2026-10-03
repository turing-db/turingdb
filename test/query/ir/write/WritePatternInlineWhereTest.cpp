#include <gtest/gtest.h>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A WHERE written inside a pattern filters the rows the pattern matches, which a CREATE and
// a MERGE pattern do not do. The parser reads one wherever a node or an edge is written, so
// a write clause must name it as the error it is rather than drop the predicate it holds.
class WritePatternInlineWhereTest : public CallV3Test {
};

TEST_F(WritePatternInlineWhereTest, createRejectsItOnANode) {
    runWriteExpectingError("CREATE (n:Person {name: 'Ada'} WHERE n.age > 1)", "WHERE");
}

TEST_F(WritePatternInlineWhereTest, createRejectsItOnAnEdge) {
    runWriteExpectingError("MATCH (a:Person), (b:Person) CREATE (a)-[e:KNOWS WHERE e.weight = 1]->(b)", "WHERE");
}

TEST_F(WritePatternInlineWhereTest, mergeRejectsItOnANode) {
    runWriteExpectingError("MERGE (n:Person {name: 'Ada'} WHERE n.age > 1)", "WHERE");
}

TEST_F(WritePatternInlineWhereTest, mergeRejectsItOnAnEdge) {
    runWriteExpectingError("MATCH (a:Person), (b:Person) MERGE (a)-[e:KNOWS WHERE e.weight = 1]->(b)", "WHERE");
}

TEST_F(WritePatternInlineWhereTest, matchStillReadsItAsAPredicate) {
    StringRowSink sink;
    runQuery("MATCH (n:Person WHERE n.name = 'Remy') RETURN n.name", sink);

    ASSERT_EQ(sink.getRows().size(), 1u);
    EXPECT_EQ(sink.getRows().front().front(), "Remy");
}
