#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// `{ WHEN ... } UNION { WHEN ... }`: each side returns the rows of the branch it takes, and
// the union combines them as it combines any two queries
class ConditionalUnionTest : public WriteQueryTest {
};

TEST_F(ConditionalUnionTest, unionsTheBranchEachSideTakes) {
    expectRows("{ "
               "WHEN true THEN RETURN 1 AS x "
               "WHEN false THEN RETURN 2 AS x "
               "ELSE RETURN 3 AS x "
               "} "
               "UNION "
               "{ "
               "WHEN false THEN RETURN 4 AS x "
               "WHEN false THEN RETURN 5 AS x "
               "ELSE RETURN 6 AS x "
               "}",
               {{"1"}, {"6"}});
}

TEST_F(ConditionalUnionTest, dedupsUnderUnionAndKeepsEveryRowUnderUnionAll) {
    expectRows("{ WHEN true THEN RETURN 1 AS x } UNION { WHEN true THEN RETURN 1 AS x }", {{"1"}});
    expectRows("{ WHEN true THEN RETURN 1 AS x } UNION ALL { WHEN true THEN RETURN 1 AS x }", {{"1"}, {"1"}});
}

TEST_F(ConditionalUnionTest, unionsAConditionalWithAPlainQuery) {
    expectRows("{ WHEN false THEN RETURN 1 AS x ELSE RETURN 2 AS x } UNION RETURN 3 AS x", {{"2"}, {"3"}});
    expectRows("RETURN 3 AS x UNION { WHEN true THEN RETURN 1 AS x }", {{"3"}, {"1"}});
    expectRows("RETURN 3 AS x UNION { WHEN false THEN RETURN 1 AS x } UNION RETURN 4 AS x", {{"3"}, {"4"}});
}

TEST_F(ConditionalUnionTest, keepsTheColumnsInTheirReturnOrder) {
    expectRowsInOrder("{ WHEN true THEN RETURN 1 AS b, 2 AS a } UNION ALL { WHEN true THEN RETURN 3 AS b, 4 AS a }",
                      {{"1", "2"}, {"3", "4"}});
}

// The French and the PhDs: Remy, Adam and Luc are both
TEST_F(ConditionalUnionTest, matchesInTheBranchesOfEachSide) {
    expectRows("{ WHEN true THEN { MATCH (p:Person) WHERE p.isFrench RETURN p.name AS name } } "
               "UNION "
               "{ WHEN false THEN { RETURN 'x' AS name } ELSE { MATCH (p:Person) WHERE p.hasPhD RETURN p.name AS name } }",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}});
}

TEST_F(ConditionalUnionTest, decidesOnWhatACallImports) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "{ WHEN p.isFrench THEN RETURN 'fr' AS tag ELSE RETURN 'other' AS tag } "
               "UNION "
               "{ WHEN p.hasPhD THEN RETURN 'phd' AS tag ELSE RETURN 'none' AS tag } "
               "} "
               "RETURN tag, count(*)",
               {{"fr", "4"}, {"other", "4"}, {"phd", "4"}, {"none", "4"}});
}

TEST_F(ConditionalUnionTest, dedupsTheRowsOfEachInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "{ WHEN p.isFrench THEN RETURN 'x' AS tag } "
               "UNION "
               "{ WHEN p.hasPhD THEN RETURN 'x' AS tag } "
               "} "
               "RETURN tag, count(*)",
               {{"x", "5"}});
}

TEST_F(ConditionalUnionTest, unionsInACallWithoutAScopeClause) {
    expectRows("MATCH (p:Person) "
               "CALL { { WHEN true THEN RETURN 1 AS x } UNION { WHEN true THEN RETURN 2 AS x } } "
               "RETURN x, count(*)",
               {{"1", "8"}, {"2", "8"}});
}

TEST_F(ConditionalUnionTest, writesInTheBranchesOfEachSide) {
    expectWriteRows("{ WHEN true THEN { CREATE (n:Person {name: 'Kai'}) RETURN n.name AS name } } "
                    "UNION "
                    "{ WHEN true THEN { MERGE (n:Person {name: 'Nia'}) RETURN n.name AS name } }",
                    {{"Kai"}, {"Nia"}});

    expectRows("MATCH (p:Person) WHERE p.name IN ['Kai', 'Nia'] RETURN p.name", {{"Kai"}, {"Nia"}});
}

TEST_F(ConditionalUnionTest, returnsAnEntityTheSidesCreatedOrMatched) {
    expectWriteRows("CALL { "
                    "{ WHEN true THEN { CREATE (n:Person {name: 'Kai'}) RETURN n } } "
                    "UNION "
                    "{ WHEN true THEN { MATCH (n:Person {name: 'Remy'}) RETURN n } } "
                    "} "
                    "SET n.age = 9 RETURN n.name, n.age",
                    {{"Kai", "9"}, {"Remy", "9"}});

    expectRows("MATCH (p:Person) WHERE p.age = 9 RETURN p.name", {{"Kai"}, {"Remy"}});
}

TEST_F(ConditionalUnionTest, rejectsSidesReturningDifferentColumns) {
    expectError("{ WHEN true THEN RETURN 1 AS x } UNION { WHEN true THEN RETURN 1 AS y }",
                "All sub-queries of a UNION must return the same column names");
}

TEST_F(ConditionalUnionTest, rejectsAColumnWithoutAName) {
    expectError("{ WHEN true THEN RETURN 1 + 1 } UNION { WHEN true THEN RETURN 2 AS x }",
                "A WHEN combined by UNION must name each column it returns with AS");
}

TEST_F(ConditionalUnionTest, rejectsAPredicateReadingWhatNoScopeClauseImports) {
    expectError("MATCH (p:Person) CALL { { WHEN p.isFrench THEN RETURN 1 AS x } UNION { WHEN true THEN RETURN 2 AS x } } RETURN x",
                "Variable 'p' not found");
}

TEST_F(ConditionalUnionTest, rejectsAConditionalInAnExistsOrCountBody) {
    expectError("MATCH (p:Person) RETURN EXISTS { { WHEN true THEN RETURN 1 AS x } UNION RETURN 2 AS x }",
                "Not implemented: WHEN in an EXISTS body");
    expectError("MATCH (p:Person) RETURN COUNT { RETURN 2 AS x UNION { WHEN true THEN RETURN 1 AS x } }",
                "Not implemented: WHEN in a COUNT body");
}
