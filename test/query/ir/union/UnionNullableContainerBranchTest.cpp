#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A column of lists or maps holds a null wherever one branch can answer null, whether that
// branch is the null literal or a CASE with no ELSE. Of simpledb's 8 people, Remy, Adam,
// Maxime and Luc are French.
class UnionNullableContainerBranchTest : public WriteQueryTest {
};

TEST_F(UnionNullableContainerBranchTest, holdsANullableListBesideAList) {
    const Rows expected {{"[1]"}, {"[1]"}, {"[1]"}, {"[1]"},
                         {"null"}, {"null"}, {"null"}, {"null"},
                         {"[2]"}};

    expectRows("MATCH (n:Person) RETURN CASE WHEN n.isFrench THEN [1] END AS l UNION ALL RETURN [2] AS l",
               expected);
    expectRows("RETURN [2] AS l UNION ALL MATCH (n:Person) RETURN CASE WHEN n.isFrench THEN [1] END AS l",
               expected);
}

TEST_F(UnionNullableContainerBranchTest, holdsANullableListBesideAListOfOtherElements) {
    expectRows("MATCH (n:Person) RETURN CASE WHEN n.isFrench THEN ['a'] END AS l UNION ALL RETURN [2] AS l",
               {{"[a]"}, {"[a]"}, {"[a]"}, {"[a]"},
                {"null"}, {"null"}, {"null"}, {"null"},
                {"[2]"}});
}

TEST_F(UnionNullableContainerBranchTest, holdsANullableMapBesideAMap) {
    expectRows("MATCH (n:Person) RETURN CASE WHEN n.isFrench THEN {a: 1} END AS m UNION ALL RETURN {b: 2} AS m",
               {{"{a: 1}"}, {"{a: 1}"}, {"{a: 1}"}, {"{a: 1}"},
                {"null"}, {"null"}, {"null"}, {"null"},
                {"{b: 2}"}});
}

TEST_F(UnionNullableContainerBranchTest, holdsANullableListBesideAListInAWhen) {
    expectRows("MATCH (n:Person) "
               "CALL (n) { "
               "  WHEN n.isFrench THEN RETURN CASE WHEN n.name = 'Remy' THEN [1] END AS l "
               "  ELSE RETURN [2] AS l "
               "} "
               "RETURN n.name, l",
               {{"Remy", "[1]"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "[2]"},
                {"Suhas", "[2]"},
                {"Cyrus", "[2]"},
                {"Doruk", "[2]"}});
}
