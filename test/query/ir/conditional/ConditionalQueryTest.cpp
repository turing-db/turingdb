#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A standalone WHEN ... THEN ... ELSE query: its result is the rows of the first branch
// whose predicate is true, or of the ELSE branch when none is
class ConditionalQueryTest : public WriteQueryTest {
};

TEST_F(ConditionalQueryTest, returnsTheRowsOfTheFirstBranchTaken) {
    expectRows("WHEN false THEN RETURN 1 AS x "
               "WHEN true THEN RETURN 2 AS x "
               "WHEN true THEN RETURN 3 AS x "
               "ELSE RETURN 3 AS x",
               {{"2"}});
}

TEST_F(ConditionalQueryTest, matchesInsideABranch) {
    expectRows("WHEN true THEN { MATCH (p:Person) WHERE p.isFrench RETURN p.name AS name } "
               "ELSE { MATCH (p:Person) RETURN p.name AS name }",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

TEST_F(ConditionalQueryTest, runsTheElseBranch) {
    expectRows("WHEN false THEN { RETURN 0 AS c } ELSE { MATCH (p:Person) RETURN count(p) AS c }",
               {{"8"}});
}

TEST_F(ConditionalQueryTest, returnsNoRowWhenNoBranchIsTaken) {
    expectRows("WHEN false THEN RETURN 1 AS x", {});
    expectRows("WHEN false THEN { MATCH (p:Person) RETURN count(p) AS c }", {});
}

TEST_F(ConditionalQueryTest, keepsTheColumnsInTheirReturnOrder) {
    expectRowsInOrder("WHEN true THEN RETURN 1 AS b, 2 AS a ELSE RETURN 3 AS b, 4 AS a",
                      {{"1", "2"}});
}

TEST_F(ConditionalQueryTest, keepsTheOrderOfTheBranchTaken) {
    expectRowsInOrder("WHEN true THEN { MATCH (p:Person) RETURN p.name AS name ORDER BY name LIMIT 3 }",
                      {{"Adam"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(ConditionalQueryTest, unwindsInsideABranch) {
    expectRows("WHEN 1 > 2 THEN { RETURN 0 AS x } ELSE { UNWIND [1, 2, 3] AS x RETURN x }",
               {{"1"}, {"2"}, {"3"}});
}

TEST_F(ConditionalQueryTest, takesANullPredicateAsFalse) {
    expectRows("WHEN null THEN RETURN 1 AS x ELSE RETURN 2 AS x", {{"2"}});
}

TEST_F(ConditionalQueryTest, explainsAConditional) {
    RowSink sink;
    const QueryStatus status = runQuery("EXPLAIN WHEN true THEN RETURN 1 AS x", &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();
}

TEST_F(ConditionalQueryTest, rejectsAPredicateReadingAVariable) {
    expectError("WHEN p.isFrench THEN RETURN 1 AS x", "Variable 'p' not found");
}

TEST_F(ConditionalQueryTest, rejectsBranchesReturningDifferentColumns) {
    expectError("WHEN true THEN RETURN 2 AS x ELSE RETURN 3 AS y",
                "All branches of a conditional query must return the same column names");

    expectError("WHEN true THEN RETURN 2 AS x, 3 AS y ELSE RETURN 3 AS x",
                "All branches of a conditional query must return the same number of columns");
}
