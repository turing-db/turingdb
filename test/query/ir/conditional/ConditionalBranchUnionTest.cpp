#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WHEN branch that is a UNION: the branch taken returns the rows its union combines
class ConditionalBranchUnionTest : public WriteQueryTest {
};

TEST_F(ConditionalBranchUnionTest, returnsTheUnionOfTheBranchTaken) {
    expectRows("WHEN true THEN { RETURN 1 AS x UNION RETURN 2 AS x } ELSE { RETURN 3 AS x }", {{"1"}, {"2"}});
    expectRows("WHEN false THEN { RETURN 1 AS x UNION RETURN 2 AS x } ELSE { RETURN 3 AS x }", {{"3"}});
}

TEST_F(ConditionalBranchUnionTest, dedupsUnderUnionAndKeepsEveryRowUnderUnionAll) {
    expectRows("WHEN true THEN { RETURN 1 AS x UNION RETURN 1 AS x }", {{"1"}});
    expectRows("WHEN true THEN { RETURN 1 AS x UNION ALL RETURN 1 AS x }", {{"1"}, {"1"}});
}

TEST_F(ConditionalBranchUnionTest, keepsTheColumnsInTheirReturnOrder) {
    expectRowsInOrder("WHEN true THEN { RETURN 1 AS b, 2 AS a UNION ALL RETURN 3 AS b, 4 AS a } "
                      "ELSE { RETURN 5 AS b, 6 AS a }",
                      {{"1", "2"}, {"3", "4"}});
}

// What the French are interested in and who they know well
TEST_F(ConditionalBranchUnionTest, readsWhatACallImportsInEachSide) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "WHEN p.isFrench THEN { "
               "MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS thing "
               "UNION "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name AS thing "
               "} "
               "ELSE { RETURN 'none' AS thing } "
               "} "
               "RETURN p.name, thing",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"}, {"Remy", "Adam"},
                {"Adam", "Bio"}, {"Adam", "Cooking"}, {"Adam", "Remy"},
                {"Maxime", "Bio"}, {"Maxime", "Padel"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "none"}, {"Suhas", "none"}, {"Cyrus", "none"}, {"Doruk", "none"}});
}

TEST_F(ConditionalBranchUnionTest, unionsAConditionalInABranch) {
    expectRows("WHEN true THEN { { WHEN false THEN RETURN 1 AS x ELSE RETURN 2 AS x } UNION RETURN 3 AS x }",
               {{"2"}, {"3"}});
}

TEST_F(ConditionalBranchUnionTest, writesInEachSideOfTheBranchTaken) {
    expectWriteRows("WHEN true THEN { "
                    "CREATE (n:Person {name: 'Kai'}) RETURN n.name AS name "
                    "UNION "
                    "MERGE (n:Person {name: 'Nia'}) RETURN n.name AS name "
                    "}",
                    {{"Kai"}, {"Nia"}});

    expectRows("MATCH (p:Person) WHERE p.name IN ['Kai', 'Nia'] RETURN p.name", {{"Kai"}, {"Nia"}});
}

TEST_F(ConditionalBranchUnionTest, writesNothingInABranchNotTaken) {
    expectWriteRows("WHEN false THEN { "
                    "CREATE (:Person {name: 'Kai'}) RETURN 1 AS x "
                    "UNION "
                    "MERGE (:Person {name: 'Nia'}) RETURN 2 AS x "
                    "} "
                    "ELSE { RETURN 3 AS x }",
                    {{"3"}});

    expectRows("MATCH (p:Person) WHERE p.name IN ['Kai', 'Nia'] RETURN count(p)", {{"0"}});
}

TEST_F(ConditionalBranchUnionTest, returnsAnEntityTheSidesCreatedOrMatched) {
    expectWriteRows("CALL { "
                    "WHEN true THEN { "
                    "CREATE (n:Person {name: 'Kai'}) RETURN n "
                    "UNION "
                    "MATCH (n:Person {name: 'Remy'}) RETURN n "
                    "} "
                    "} "
                    "SET n.age = 9 RETURN n.name, n.age",
                    {{"Kai", "9"}, {"Remy", "9"}});

    expectRows("MATCH (p:Person) WHERE p.age = 9 RETURN p.name", {{"Kai"}, {"Remy"}});
}

TEST_F(ConditionalBranchUnionTest, rejectsAColumnWithoutAName) {
    expectError("WHEN true THEN { RETURN 1 + 1 UNION RETURN 2 AS x }",
                "A UNION in a WHEN branch must name each column it returns with AS");
}

TEST_F(ConditionalBranchUnionTest, rejectsSidesReturningDifferentColumns) {
    expectError("WHEN true THEN { RETURN 1 AS x UNION RETURN 2 AS y }",
                "All sub-queries of a UNION must return the same column names");
}

TEST_F(ConditionalBranchUnionTest, rejectsBranchesReturningDifferentColumns) {
    expectError("WHEN true THEN { RETURN 1 AS x UNION RETURN 2 AS x } ELSE { RETURN 3 AS y }",
                "All branches of a conditional query must return the same column names");
}
