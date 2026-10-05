#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WHEN beside OPTIONAL MATCH, CALL and EXISTS: in a branch, around the CALL holding the
// WHEN, and in its predicates
class ConditionalCompositionTest : public WriteQueryTest {
};

// Remy and Adam know each other well, Maxime and Luc know no one well
TEST_F(ConditionalCompositionTest, optionallyMatchesInABranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench THEN { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name AS f } "
               "ELSE { RETURN 'n/a' AS f } "
               "} "
               "RETURN p.name, f",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"},
                {"Martina", "n/a"}, {"Suhas", "n/a"}, {"Cyrus", "n/a"}, {"Doruk", "n/a"}});
}

TEST_F(ConditionalCompositionTest, decidesOnAnOptionallyMatchedNode) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { WHEN f IS NULL THEN RETURN 'alone' AS s ELSE RETURN f.name AS s } "
               "RETURN p.name, s",
               {{"Remy", "Adam"}, {"Adam", "Remy"},
                {"Maxime", "alone"}, {"Luc", "alone"}, {"Martina", "alone"},
                {"Suhas", "alone"}, {"Cyrus", "alone"}, {"Doruk", "alone"}});
}

TEST_F(ConditionalCompositionTest, optionallyMatchesFromWhatABranchReturns) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN RETURN p AS q } "
               "OPTIONAL MATCH (q)-[:KNOWS_WELL]->(f) "
               "RETURN q.name, f.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"}});
}

TEST_F(ConditionalCompositionTest, optionallyMatchesInAStandaloneConditional) {
    expectRows("WHEN true THEN { "
               "MATCH (p:Person) WHERE p.isFrench "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name AS n, f.name AS f "
               "}",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"}});
}

TEST_F(ConditionalCompositionTest, callsASubqueryInABranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench THEN { CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS n } RETURN n } "
               "ELSE { RETURN 0 AS n } "
               "} "
               "RETURN p.name, n",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

// Remy has 3 interests, Martina and Doruk 1, the 5 others 2
TEST_F(ConditionalCompositionTest, decidesOnWhatAnEarlierCallReturns) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS n } "
               "CALL (n) { WHEN n > 2 THEN RETURN 'many' AS k WHEN n > 1 THEN RETURN 'some' AS k ELSE RETURN 'one' AS k } "
               "RETURN k, count(*)",
               {{"many", "1"}, {"some", "5"}, {"one", "2"}});
}

TEST_F(ConditionalCompositionTest, padsAnOptionalCallInABranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench THEN { "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name AS f } "
               "RETURN f "
               "} "
               "} "
               "RETURN p.name, f",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"}});
}

TEST_F(ConditionalCompositionTest, returnsAnExistsFromABranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN RETURN EXISTS { (p)-[:KNOWS_WELL]->() } AS social } "
               "RETURN p.name, social",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"}});
}

// The interests of a person with a PhD that someone else shares
TEST_F(ConditionalCompositionTest, filtersOnAnExistsInABranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.hasPhD THEN { "
               "MATCH (p)-[:INTERESTED_IN]->(i) WHERE EXISTS { (i)<-[:INTERESTED_IN]-(o) WHERE o <> p } "
               "RETURN i.name AS i "
               "} "
               "} "
               "RETURN p.name, i",
               {{"Remy", "Computers"}, {"Adam", "Bio"}, {"Adam", "Cooking"},
                {"Luc", "Computers"}, {"Martina", "Cooking"}});
}

TEST_F(ConditionalCompositionTest, decidesOnANegatedExists) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench AND NOT EXISTS { (p)-[:KNOWS_WELL]->() } THEN RETURN 'lonely' AS s "
               "ELSE RETURN 'other' AS s "
               "} "
               "RETURN p.name, s",
               {{"Maxime", "lonely"}, {"Luc", "lonely"},
                {"Remy", "other"}, {"Adam", "other"}, {"Martina", "other"},
                {"Suhas", "other"}, {"Cyrus", "other"}, {"Doruk", "other"}});
}

TEST_F(ConditionalCompositionTest, decidesAStandaloneConditionalOnAnExists) {
    expectRows("WHEN EXISTS { MATCH (p:Person) WHERE p.age > 40 } THEN RETURN 'old' AS x "
               "ELSE RETURN 'young' AS x",
               {{"young"}});

    expectRows("WHEN EXISTS { (:Person)-[:KNOWS_WELL]->(:Person) } THEN RETURN 'social' AS x "
               "ELSE RETURN 'alone' AS x",
               {{"social"}});
}
