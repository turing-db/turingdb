#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class CaseEntityInequalityTest : public WriteQueryTest {
};

TEST_F(CaseEntityInequalityTest, takesTheBranchForAnotherNode) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b) RETURN a.name, CASE a WHEN <> b THEN 'other' ELSE 'self' END",
               {{"Adam", "other"}, {"Remy", "other"}});
}

TEST_F(CaseEntityInequalityTest, skipsTheBranchForTheSameNode) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b) RETURN a.name, CASE a WHEN <> a THEN 'other' ELSE 'self' END",
               {{"Adam", "self"}, {"Remy", "self"}});
}

TEST_F(CaseEntityInequalityTest, takesTheBranchForAnotherID) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b) RETURN a.name, CASE a WHEN <> 0 THEN 'other' ELSE 'self' END",
               {{"Adam", "other"}, {"Remy", "self"}});
}
