#include <gtest/gtest.h>

#include <string>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// Naming both ends of a repetition the same is how a pattern spells a self-loop: each hop
// leaves and lands on one node, and chaining the repetitions keeps the whole walk there. The
// name binds the one node per hop, as any other name inside a quantified pattern binds a
// list of one entity per hop.
class QuantifiedSelfLoopTest : public CallV3Test {
protected:
    size_t rowCount(const std::string& query) {
        StringRowSink sink;
        runQuery(query, sink);

        return sink.getRows().size();
    }
};

TEST_F(QuantifiedSelfLoopTest, matchesNothingWhereTheGraphHasNoSelfLoop) {
    ASSERT_EQ(rowCount("MATCH (x)-[:KNOWS_WELL]->(x) RETURN x.name"), 0u);
    ASSERT_GT(rowCount("MATCH (n)((a)-[e:KNOWS_WELL]->(b)){1,3}(m) RETURN m.name"), 0u);

    EXPECT_EQ(rowCount("MATCH (n)((a)-[e:KNOWS_WELL]->(a)){1,3}(m) RETURN m.name"), 0u);
}

TEST_F(QuantifiedSelfLoopTest, matchesTheSelfLoopTheQueryWrote) {
    runWrite("MATCH (r:Person {name: 'Remy'}) CREATE (r)-[:KNOWS_WELL {name: 'Remy -> Remy'}]->(r)");

    ASSERT_EQ(rowCount("MATCH (x)-[:KNOWS_WELL]->(x) RETURN x.name"), 1u);

    StringRowSink sink;
    runQuery("MATCH (n)((a)-[e:KNOWS_WELL]->(a)){1}(m) RETURN m.name", sink);

    ASSERT_EQ(sink.getRows().size(), 1u);
    EXPECT_EQ(sink.getRows().front().front(), "Remy");
}
