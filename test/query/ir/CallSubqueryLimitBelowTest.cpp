#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A unit subquery hands the rows it was given straight on, and a cut below it keeps some
// of them: the body has written for every one all the same, as a write standing in the
// same place has
class CallSubqueryLimitBelowTest : public WriteQueryTest {
};

// The 8 Persons against themselves are 64 pairs; the cut takes 5, and the body wrote for
// all 64
TEST_F(CallSubqueryLimitBelowTest, writesForEveryRowACutBelowAUnitBodyDrops) {
    expectWriteRows("MATCH (a:Person), (b:Person) CALL (a) { CREATE (:Audit) } RETURN 1 LIMIT 5",
                    {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}});

    expectRows("MATCH (a:Audit) RETURN count(a)", {{"64"}});
}

TEST_F(CallSubqueryLimitBelowTest, answersAsTheSameWriteWithoutASubquery) {
    expectWriteRows("MATCH (a:Person), (b:Person) CREATE (:Audit) RETURN 1 LIMIT 5",
                    {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}});

    expectRows("MATCH (a:Audit) RETURN count(a)", {{"64"}});
}
