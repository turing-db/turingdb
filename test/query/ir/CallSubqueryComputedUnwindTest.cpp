#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CALL body runs once per incoming row, so a list it computes from nothing of the outer
// row is unwound for every one of them
class CallSubqueryComputedUnwindTest : public WriteQueryTest {
};

TEST_F(CallSubqueryComputedUnwindTest, unwindsARangeForEveryOuterRow) {
    expectWriteRows("UNWIND [1, 2, 3] AS i CALL () { UNWIND range(1, 2) AS j RETURN j } RETURN count(*)", {{"6"}});
}

TEST_F(CallSubqueryComputedUnwindTest, createsForEveryOuterRowAndUnwoundValue) {
    applyWrite("MATCH (p:Person) CALL () { UNWIND range(1, 2) AS j CREATE (:CB) }");

    expectRows("MATCH (c:CB) RETURN count(c)", {{"16"}});
}

TEST_F(CallSubqueryComputedUnwindTest, limitsTheUnwoundRowsOfEachRun) {
    applyWrite("UNWIND range(1, 3) AS i CALL () { UNWIND range(1, 2) AS j WITH j LIMIT 1 CREATE (:CB) }");

    expectRows("MATCH (c:CB) RETURN count(c)", {{"3"}});
}

TEST_F(CallSubqueryComputedUnwindTest, returnsTheLimitedRowsOfEachRun) {
    expectWriteRows("MATCH (p:Person) CALL () { UNWIND range(1, 2) AS j WITH j LIMIT 1 RETURN j } RETURN count(*)", {{"8"}});
}

TEST_F(CallSubqueryComputedUnwindTest, mergesALiteralPatternOncePerOuterRow) {
    expectWriteRows("UNWIND [1, 2, 3] AS o CALL () { MERGE (m:T {k: 1 + 1}) RETURN m.k AS k } RETURN o, k",
                    {{"1", "2"}, {"2", "2"}, {"3", "2"}});

    expectRows("MATCH (m:T) RETURN count(m)", {{"1"}});
}

TEST_F(CallSubqueryComputedUnwindTest, mergesALiteralPathOncePerOuterRow) {
    applyWrite("UNWIND [1, 2] AS o CALL () { MERGE (a:Z3)-[:R2]->(b:Z4) RETURN a } RETURN o");

    expectRows("MATCH (a:Z3)-[:R2]->(b:Z4) RETURN count(*)", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
