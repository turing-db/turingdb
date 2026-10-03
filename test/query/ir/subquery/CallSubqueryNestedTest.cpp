#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A subquery inside a subquery: the inner body runs once per row the outer one runs for,
// and the rows in flight come out of both as they went in
class CallSubqueryNestedTest : public WriteQueryTest {
};

TEST_F(CallSubqueryNestedTest, aNestedUnitBodyWritesOncePerRowInFlight) {
    expectWriteRows("MATCH (p:Person) CALL (p) { CALL () { CREATE (:Audit) } } RETURN count(p)", {{"8"}});

    expectRows("MATCH (a:Audit) RETURN count(a)", {{"8"}});
}

// The inner body walks the interests of the Person the outer body was given, so the writes
// count the 15 edges rather than the 8 rows
TEST_F(CallSubqueryNestedTest, aNestedBodyReadsTheImportOfTheOuterOne) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) CREATE (i)-[:LIKED_BY]->(p) } } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (:Interest)-[:LIKED_BY]->(:Person) RETURN count(*)", {{"15"}});
}
