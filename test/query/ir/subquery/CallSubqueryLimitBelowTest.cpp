#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A unit subquery hands the rows it was given straight on, so a cut below it stops the
// walk that feeds it, as it does for a write standing in the same place
class CallSubqueryLimitBelowTest : public WriteQueryTest {
};

// The 8 Persons against themselves are 64 pairs; the cut takes 5, and the body wrote for
// those 5 alone
TEST_F(CallSubqueryLimitBelowTest, aCutBelowAUnitBodyStopsTheWalkThatFeedsIt) {
    expectWriteRows("MATCH (a:Person), (b:Person) CALL (a) { CREATE (:Audit) } RETURN 1 LIMIT 5",
                    {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}});

    expectRows("MATCH (a:Audit) RETURN count(a)", {{"5"}});
}

TEST_F(CallSubqueryLimitBelowTest, answersAsTheSameWriteWithoutASubquery) {
    expectWriteRows("MATCH (a:Person), (b:Person) CREATE (:Audit) RETURN 1 LIMIT 5",
                    {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}});

    expectRows("MATCH (a:Audit) RETURN count(a)", {{"5"}});
}
