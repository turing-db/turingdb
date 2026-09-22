#include <gtest/gtest.h>

#include "CallV3Test.h"

using namespace turing::test;

// An index covers every node or edge carrying the property, so a pattern that constrains
// which ones it covers is refused. A WHERE written inside the pattern is one more such
// constraint, and the parser reads one wherever a node or an edge is written.
class IndexPatternInlineWhereTest : public CallV3Test {
};

TEST_F(IndexPatternInlineWhereTest, aNodeIndexRejectsIt) {
    runQueryExpectingError("CREATE INDEX myindex FOR (n WHERE n.age > 1) ON n.age",
                           "WHERE is not allowed in an index pattern");
}

TEST_F(IndexPatternInlineWhereTest, anEdgeIndexRejectsIt) {
    runQueryExpectingError("CREATE INDEX myindex FOR [e WHERE e.duration > 1] ON e.duration",
                           "WHERE is not allowed in an index pattern");
}

TEST_F(IndexPatternInlineWhereTest, anUnconstrainedNodeIndexIsStillAccepted) {
    runWrite("CREATE INDEX myindex FOR (n) ON n.age");
}
