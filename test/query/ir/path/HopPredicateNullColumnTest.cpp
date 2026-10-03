#include <gtest/gtest.h>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A predicate over a property no edge of the graph carries compares to null, which keeps no
// row. A quantified pattern evaluates its predicate per hop instead of per matched edge, but
// the column it yields is the same one a plain WHERE builds, so the two must agree.
class HopPredicateNullColumnTest : public CallV3Test {
protected:
    size_t rowCount(std::string_view query) {
        StringRowSink sink;
        runQuery(query, sink);

        return sink.getRows().size();
    }
};

TEST_F(HopPredicateNullColumnTest, anAbsentPropertyKeepsNoHop) {
    EXPECT_EQ(rowCount("MATCH (n)-[e:KNOWS_WELL*1..3 WHERE e.since > 0]->(m) RETURN m.name"), 0u);
}

TEST_F(HopPredicateNullColumnTest, anAbsentPropertyKeepsNoHopInTheParenthesisedForm) {
    EXPECT_EQ(rowCount("MATCH (n)((a)-[e:KNOWS_WELL]->(b) WHERE e.since > 0){1,3}(m) RETURN m.name"), 0u);
}

TEST_F(HopPredicateNullColumnTest, aPlainHopAgreesOnTheAbsentProperty) {
    EXPECT_EQ(rowCount("MATCH (n)-[e:KNOWS_WELL]->(m) WHERE e.since > 0 RETURN m.name"), 0u);
}

TEST_F(HopPredicateNullColumnTest, aPresentPropertyStillKeepsItsHops) {
    EXPECT_EQ(rowCount("MATCH (n)-[e:KNOWS_WELL*1..3 WHERE e.duration > 0]->(m) RETURN m.name"),
              rowCount("MATCH (n)-[e:KNOWS_WELL*1..3]->(m) RETURN m.name"));
}
