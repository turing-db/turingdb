#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// OPTIONAL pads the input rows a body yields nothing for. A unit body yields nothing for
// every row and leaves the rows as they are, so there is no row to pad and the keyword
// does nothing: the query answers as it would without it
class CallSubqueryOptionalUnitTest : public WriteQueryTest {
};

TEST_F(CallSubqueryOptionalUnitTest, optionalOverAUnitBodyPassesEveryRowThrough) {
    expectWriteRows("MATCH (p:Person) OPTIONAL CALL (p) { CREATE (:Audit) } RETURN count(p)", {{"8"}});

    expectRows("MATCH (a:Audit) RETURN count(a)", {{"8"}});
}

// Six of the eight Persons have no KNOWS_WELL edge - the rows OPTIONAL would pad if a unit
// body had rows to yield - and all eight come through
TEST_F(CallSubqueryOptionalUnitTest, keepsTheRowsOfABodyThatMatchedNothing) {
    expectWriteRows("MATCH (p:Person) "
                    "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) CREATE (:Friendship) } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (f:Friendship) RETURN count(f)", {{"2"}});
}

TEST_F(CallSubqueryOptionalUnitTest, answersAsTheSameQueryWithoutOptional) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) CREATE (:Friendship) } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (f:Friendship) RETURN count(f)", {{"2"}});
}
