#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class CaseLazyBranchTest : public WriteQueryTest {
};

TEST_F(CaseLazyBranchTest, skipsTheElseOfAGuardedRow) {
    expectRows("UNWIND [0, 2] AS t RETURN CASE t WHEN 0 THEN 0 ELSE 10 / t END", {{"0"}, {"5"}});
}

TEST_F(CaseLazyBranchTest, skipsTheThenOfAnUnguardedRow) {
    expectRows("UNWIND [0, 2] AS t RETURN CASE WHEN t <> 0 THEN 10 / t ELSE -1 END", {{"-1"}, {"5"}});
    expectRows("UNWIND [0, 3] AS t RETURN CASE WHEN t = 0 THEN 0 ELSE 7 % t END", {{"0"}, {"1"}});
}

TEST_F(CaseLazyBranchTest, testsALaterConditionOnlyOnTheRowsReachingIt) {
    expectRows("UNWIND [0, 2] AS t RETURN CASE WHEN t = 0 THEN 'zero' WHEN 10 / t > 1 THEN 'big' END",
               {{"zero"}, {"big"}});
}

TEST_F(CaseLazyBranchTest, keepsTheRowOrder) {
    expectRowsInOrder("UNWIND [2, 0, 5, 0, 1] AS t RETURN CASE WHEN t = 0 THEN -1 WHEN t = 5 THEN 50 ELSE 10 / t END",
                      {{"5"}, {"-1"}, {"50"}, {"-1"}, {"10"}});
}

TEST_F(CaseLazyBranchTest, guardsADivisionByAnAggregate) {
    expectRows("MATCH (n:Person) WITH n.name AS name, count(n.age) AS aged "
               "RETURN name, CASE aged WHEN 0 THEN 0 ELSE 10 / aged END",
               {{"Remy", "10"}, {"Adam", "10"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(CaseLazyBranchTest, guardsInsideAListComprehension) {
    expectRows("RETURN [t IN [0, 2] | CASE WHEN t = 0 THEN 0 ELSE 10 / t END]", {{"[0, 5]"}});
}

TEST_F(CaseLazyBranchTest, guardsAConstantDivisor) {
    expectRows("WITH 0 AS t RETURN CASE WHEN t = 0 THEN 0 ELSE 10 / t END", {{"0"}});
}

TEST_F(CaseLazyBranchTest, guardsWithNoRowsInFlight) {
    expectRows("RETURN CASE WHEN false THEN 10 / 0 ELSE 1 END", {{"1"}});
}

TEST_F(CaseLazyBranchTest, guardsANestedCase) {
    expectRows("UNWIND [0, 1, 2] AS t "
               "RETURN CASE WHEN t = 0 THEN 0 ELSE CASE WHEN t = 1 THEN -1 ELSE 10 / (t - 1) END END",
               {{"0"}, {"-1"}, {"10"}});
}

TEST_F(CaseLazyBranchTest, guardsAnAggregateArgument) {
    expectRows("UNWIND [0, 2, 5] AS t RETURN sum(CASE WHEN t = 0 THEN 0 ELSE 10 / t END)", {{"7"}});
}

TEST_F(CaseLazyBranchTest, guardsAPredicate) {
    expectRows("UNWIND [0, 2, 5] AS t WITH t WHERE CASE WHEN t = 0 THEN false ELSE 10 / t > 2 END RETURN t",
               {{"2"}});
}

TEST_F(CaseLazyBranchTest, carriesTheResultThroughAnExpansion) {
    expectRows("MATCH (n:Person) WITH n, CASE WHEN n.age IS NULL THEN 0 ELSE 64 / n.age END AS s "
               "MATCH (n)-[:INTERESTED_IN]->(i) RETURN n.name, s, i.name",
               {{"Remy", "2", "Ghosts"}, {"Remy", "2", "Computers"}, {"Remy", "2", "Eighties"},
                {"Adam", "2", "Bio"}, {"Adam", "2", "Cooking"},
                {"Maxime", "0", "Bio"}, {"Maxime", "0", "Padel"},
                {"Luc", "0", "Animals"}, {"Luc", "0", "Computers"},
                {"Martina", "0", "Cooking"},
                {"Cyrus", "0", "Gym"}, {"Cyrus", "0", "Travel"},
                {"Doruk", "0", "Gym"},
                {"Suhas", "0", "Gym"}, {"Suhas", "0", "JiuJitsu"}});
}

TEST_F(CaseLazyBranchTest, stillDividesByZeroOnARowTakingTheBranch) {
    expectError("UNWIND [0, 2] AS t RETURN CASE WHEN t = 2 THEN 10 / t ELSE 10 / t END", "divide by zero");
}
