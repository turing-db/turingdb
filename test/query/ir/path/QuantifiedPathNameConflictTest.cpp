#include <gtest/gtest.h>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A node named inside a quantified pattern binds the list of nodes the repetitions matched,
// which is a variable the pattern introduces. A name the query already bound cannot also be
// that list, so the pattern naming one is a conflict: taking it silently drops the outer
// binding and turns what reads as a correlation into a cross product.
class QuantifiedPathNameConflictTest : public CallV3Test {
protected:
    size_t rowCount(std::string_view query) {
        StringRowSink sink;
        runQuery(query, sink);

        return sink.getRows().size();
    }
};

TEST_F(QuantifiedPathNameConflictTest, aFreshNameGroupsTheRepetitions) {
    EXPECT_EQ(rowCount("MATCH (n)((a)-[e:KNOWS_WELL]->(b)){1,3}(m) RETURN m.name"), 7u);
}

TEST_F(QuantifiedPathNameConflictTest, theStartOfARepetitionCannotTakeABoundName) {
    runQueryExpectingError("MATCH (x:Person) MATCH (n)((x)-[e:KNOWS_WELL]->(b)){1,3}(m) RETURN m.name",
                           "already bound");
}

TEST_F(QuantifiedPathNameConflictTest, theEndOfARepetitionCannotTakeABoundName) {
    runQueryExpectingError("MATCH (x:Person) MATCH (n)((a)-[e:KNOWS_WELL]->(x)){1,3}(m) RETURN m.name",
                           "already bound");
}

TEST_F(QuantifiedPathNameConflictTest, aLaterPatternCannotTakeAGroupName) {
    runQueryExpectingError("MATCH (n)((a)-[e:KNOWS_WELL]->(b)){1,3}(m), (b:Person) RETURN m.name",
                           "already");
}

TEST_F(QuantifiedPathNameConflictTest, aLaterMatchCannotTakeAGroupName) {
    runQueryExpectingError("MATCH (n)((a)-[e:KNOWS_WELL]->(b)){1,3}(m) MATCH (b)-[:KNOWS_WELL]->(z) RETURN z.name",
                           "already");
}
