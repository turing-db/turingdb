#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WHEN computes a predicate only for the rows no earlier branch was taken for, and a
// branch only for the rows it is taken for: what a row does not reach divides by no zero
class ConditionalLazyBranchTest : public WriteQueryTest {
};

TEST_F(ConditionalLazyBranchTest, computesNoPredicatePastTheBranchTaken) {
    expectRows("UNWIND [0, 5] AS k "
               "CALL (k) { WHEN k = 0 THEN RETURN 0 AS r WHEN 10 / k > 1 THEN RETURN 1 AS r ELSE RETURN 2 AS r } "
               "RETURN k, r",
               {{"0", "0"}, {"5", "1"}});
}

TEST_F(ConditionalLazyBranchTest, computesNoConstantPredicatePastTheBranchTaken) {
    expectRows("WITH 0 AS d MATCH (p:Person) "
               "CALL (d) { WHEN d = 0 THEN RETURN 0 AS r WHEN 10 / d > 1 THEN RETURN 1 AS r } "
               "RETURN r, count(*)",
               {{"0", "8"}});

    expectRows("WHEN true THEN RETURN 1 AS x WHEN 10 / 0 > 1 THEN RETURN 2 AS x", {{"1"}});
}

TEST_F(ConditionalLazyBranchTest, computesNothingOfABranchNotTaken) {
    expectRows("UNWIND [0, 5] AS k "
               "CALL (k) { WHEN k = 0 THEN RETURN 0 AS r ELSE RETURN 10 / k AS r } "
               "RETURN k, r",
               {{"0", "0"}, {"5", "2"}});
}

TEST_F(ConditionalLazyBranchTest, computesNoConstantOfABranchNotTaken) {
    expectRows("WITH 0 AS d MATCH (p:Person) "
               "CALL (d) { WHEN d = 0 THEN RETURN 0 AS r ELSE RETURN 10 / d AS r } "
               "RETURN r, count(*)",
               {{"0", "8"}});

    expectRows("WHEN true THEN RETURN 1 AS x ELSE RETURN 10 / 0 AS x", {{"1"}});
    expectRows("WHEN false THEN RETURN 10 / 0 AS x ELSE RETURN 1 AS x", {{"1"}});
}

TEST_F(ConditionalLazyBranchTest, computesTheConstantsOfABranchTakenOverItsRows) {
    expectRows("WITH 2 AS d MATCH (p:Person) "
               "CALL (p, d) { WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN 10 / d AS r } } "
               "RETURN r, count(*)",
               {{"5", "9"}});
}

TEST_F(ConditionalLazyBranchTest, computesNoConstantBesideTheRowsOfABranchNotTaken) {
    expectRows("WITH 0 AS d MATCH (p:Person) "
               "CALL (p, d) { "
               "WHEN d = 0 THEN RETURN 0 AS r "
               "ELSE { MATCH (p)-[:INTERESTED_IN]->(i) RETURN (10 / d) * 2 AS r } "
               "} "
               "RETURN r, count(*)",
               {{"0", "8"}});

    expectRows("WITH 2 AS d MATCH (p:Person) "
               "CALL (p, d) { WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN (10 / d) * 2 AS r } } "
               "RETURN r, count(*)",
               {{"10", "9"}});
}

TEST_F(ConditionalLazyBranchTest, computesNoConstantOfANestedBranchNotTaken) {
    expectRows("WITH 0 AS d MATCH (p:Person) "
               "CALL (p, d) { "
               "WHEN p.isFrench THEN { CALL (d) { WHEN d = 0 THEN RETURN 0 AS r ELSE RETURN 10 / d AS r } RETURN r } "
               "ELSE RETURN 1 AS r "
               "} "
               "RETURN r, count(*)",
               {{"0", "4"}, {"1", "4"}});
}

TEST_F(ConditionalLazyBranchTest, readsAConstantOfABranchInANestedBranch) {
    expectRows("WITH 0 AS d MATCH (p:Person) "
               "CALL (p, d) { "
               "WHEN p.isFrench THEN { WITH d + 10 AS c CALL (c) { WHEN c > 1 THEN RETURN c + 1 AS r } RETURN r } "
               "ELSE RETURN 1 AS r "
               "} "
               "RETURN r, count(*)",
               {{"11", "4"}, {"1", "4"}});
}
