#include <gtest/gtest.h>

#include <stddef.h>

#include <string>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// coalesce computes an argument only over the rows every earlier argument left null, so a
// later argument that would raise on a row is never computed for it
class CoalesceLazyArgumentTest : public WriteQueryTest {
};

TEST_F(CoalesceLazyArgumentTest, skipsTheArgumentsAfterALeadingLiteral) {
    expectRows("RETURN coalesce(1, 1 / 0)", {{"1"}});
    expectRows("RETURN coalesce(null, 1, 1 / 0)", {{"1"}});
}

TEST_F(CoalesceLazyArgumentTest, computesTheSecondArgumentOnlyWhereTheFirstIsNull) {
    expectRows("UNWIND [0, 2] AS t RETURN coalesce(CASE WHEN t = 0 THEN 0 END, 10 / t)", {{"0"}, {"5"}});
}

TEST_F(CoalesceLazyArgumentTest, computesTheThirdArgumentOnlyWhereBothEarlierAreNull) {
    expectRows("UNWIND [0, 1, 2] AS t "
               "RETURN coalesce(CASE WHEN t = 0 THEN -1 END, CASE WHEN t = 1 THEN -2 END, 10 / (t * (t - 1)))",
               {{"-1"}, {"-2"}, {"5"}});
}

TEST_F(CoalesceLazyArgumentTest, skipsTheLastArgumentWhereTheSecondHasAValue) {
    expectRows("UNWIND [0, 2] AS t RETURN coalesce(CASE WHEN t = 0 THEN 0 END, 10 / t, 1 / 0)", {{"0"}, {"5"}});
}

// Remy and Adam are the two people with an age, and the only ones with a four-letter name
TEST_F(CoalesceLazyArgumentTest, computesTheFallbackOnlyForThePeopleWithNoAge) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.age, 10 / (size(n.name) - 4))",
               {{"Remy", "32"}, {"Adam", "32"}, {"Maxime", "5"}, {"Luc", "-10"},
                {"Martina", "3"}, {"Suhas", "10"}, {"Cyrus", "10"}, {"Doruk", "10"}});
}

TEST_F(CoalesceLazyArgumentTest, keepsTheRowOrder) {
    expectRowsInOrder("UNWIND [2, 0, 5, 0, 1] AS t RETURN coalesce(CASE WHEN t = 0 THEN -1 END, 10 / t)",
                      {{"5"}, {"-1"}, {"2"}, {"-1"}, {"10"}});
}

TEST_F(CoalesceLazyArgumentTest, guardsAConstantDivisor) {
    expectRows("WITH 0 AS t RETURN coalesce(CASE WHEN t = 0 THEN 0 END, 10 / t)", {{"0"}});
}

TEST_F(CoalesceLazyArgumentTest, guardsInsideAListComprehension) {
    expectRows("RETURN [t IN [0, 2] | coalesce(CASE WHEN t = 0 THEN 0 END, 10 / t)]", {{"[0, 5]"}});
}

TEST_F(CoalesceLazyArgumentTest, guardsAnAggregateArgument) {
    expectRows("UNWIND [0, 2, 5] AS t RETURN sum(coalesce(CASE WHEN t = 0 THEN 0 END, 10 / t))", {{"7"}});
}

TEST_F(CoalesceLazyArgumentTest, guardsAPredicate) {
    expectRows("UNWIND [0, 2, 5] AS t WITH t WHERE coalesce(CASE WHEN t = 0 THEN false END, 10 / t > 2) RETURN t",
               {{"2"}});
}

TEST_F(CoalesceLazyArgumentTest, skipsTheFallbackOfAReduction) {
    expectRows("MATCH (n:Person) RETURN coalesce(max(n.age), 1 / 0)", {{"32"}});
}

// Remy, Adam, Luc and Martina hold a PhD; Remy and Adam are the only people with an age
TEST_F(CoalesceLazyArgumentTest, guardsAGroupedReduction) {
    expectRows("MATCH (n:Person) RETURN n.hasPhD, coalesce(max(n.age), 10 / (count(n.age) - 2))",
               {{"true", "32"}, {"false", "-5"}});
}

TEST_F(CoalesceLazyArgumentTest, computesALongArgumentListLazily) {
    constexpr size_t GUARD_COUNT = 5000;

    std::string query = "UNWIND [0, 2] AS t RETURN coalesce(";
    for (size_t guardIndex = 0; guardIndex < GUARD_COUNT; guardIndex++) {
        query += "CASE WHEN t = 0 THEN -1 END, ";
    }
    query += "10 / t)";

    expectRows(query, {{"-1"}, {"5"}});
}

TEST_F(CoalesceLazyArgumentTest, stillDividesByZeroOnARowReachingTheArgument) {
    expectError("UNWIND [0, 2] AS t RETURN coalesce(CASE WHEN t = 2 THEN 1 END, 10 / t)", "divide by zero");
}

TEST_F(CoalesceLazyArgumentTest, stillDividesByZeroInAnUnreadCoalesce) {
    expectError("UNWIND [0, 2] AS t WITH t, coalesce(CASE WHEN t = 2 THEN 1 END, 10 / t) AS q RETURN count(t)",
                "divide by zero");
}
