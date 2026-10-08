#include <gtest/gtest.h>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class QuantifiedBodyEdgeNameTest : public CallV3Test {
};

TEST_F(QuantifiedBodyEdgeNameTest, aBodyCannotNameTwoRelationshipsAlike) {
    runQueryExpectingError("MATCH (n)((a)-[e:KNOWS_WELL]->(b)-[e:KNOWS_WELL]->(c)){1,1}(m) RETURN e",
                           "Re-using the same edge variable");
}

TEST_F(QuantifiedBodyEdgeNameTest, aBodyCannotNameTwoRelationshipsAlikeAcrossTypes) {
    runQueryExpectingError("MATCH (n)((a)-[e:KNOWS_WELL]->(b)-[e:INTERESTED_IN]->(c)){1,2}(m) RETURN m.name",
                           "Re-using the same edge variable");
}

TEST_F(QuantifiedBodyEdgeNameTest, aBodyNamesEachRelationshipOnce) {
    StringRowSink sink;
    runQuery("MATCH (n)((a)-[e:KNOWS_WELL]->(b)-[f:INTERESTED_IN]->(c)){1,1}(m) RETURN m.name", sink);

    EXPECT_FALSE(sink.getRows().empty());
}

TEST_F(QuantifiedBodyEdgeNameTest, aLaterPatternCannotTakeABodyRelationshipGroup) {
    runQueryExpectingError("MATCH (n)((a)-[e:KNOWS_WELL]->(b)-[f:INTERESTED_IN]->(c)){1,1}(m), (u)-[e]->(v) RETURN e",
                           "already bound");
}

TEST_F(QuantifiedBodyEdgeNameTest, aLaterMatchCannotTakeABodyRelationshipGroup) {
    runQueryExpectingError("MATCH (n)((a)-[e:KNOWS_WELL]->(b)-[f:INTERESTED_IN]->(c)){1,1}(m) MATCH (u)-[e]->(v) RETURN e",
                           "already bound");
}
