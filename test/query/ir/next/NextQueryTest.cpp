#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Queries chained with NEXT: each segment reads the whole table the segment before it
// returned, under the names it returned and nothing else
class NextQueryTest : public WriteQueryTest {
};

TEST_F(NextQueryTest, readsWhatTheSegmentBeforeReturned) {
    expectRows("MATCH (p:Person) WHERE p.isFrench RETURN p.name AS name NEXT RETURN name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

TEST_F(NextQueryTest, rejectsAVariableTheSegmentBeforeDidNotReturn) {
    expectError("MATCH (p:Person) RETURN p.name AS name NEXT RETURN p", "Variable 'p' not found");
}

TEST_F(NextQueryTest, rejectsAnUnaliasedExpressionBeforeNext) {
    expectError("MATCH (p:Person) RETURN p.name NEXT RETURN 1 AS one",
                "Expression in a RETURN followed by NEXT must be aliased (use AS)");
}

TEST_F(NextQueryTest, chainsThreeSegments) {
    expectRows("UNWIND [1, 2, 3] AS x RETURN x NEXT RETURN x * 10 AS y NEXT RETURN sum(y) AS total",
               {{"60"}});
}

// 15 INTERESTED_IN edges, 3 of them to Gym and 2 each to Bio, Computers and Cooking
TEST_F(NextQueryTest, aggregatesOverEveryRowOfTheTable) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) RETURN p, i NEXT RETURN count(*) AS n",
               {{"15"}});

    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) RETURN i.name AS interest "
                      "NEXT RETURN interest, count(*) AS n ORDER BY n DESC, interest LIMIT 3",
                      {{"Gym", "3"}, {"Bio", "2"}, {"Computers", "2"}});
}

TEST_F(NextQueryTest, matchesFromAnEntityTheSegmentBeforeReturned) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' RETURN p AS person "
               "NEXT MATCH (person)-[:INTERESTED_IN]->(i:Interest) RETURN i.name AS name",
               {{"Ghosts"}, {"Computers"}, {"Eighties"}});
}

TEST_F(NextQueryTest, passesEveryVariableOfAReturnStar) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' WITH p.name AS name, p.age AS age RETURN * "
               "NEXT RETURN name, age",
               {{"Remy", "32"}});
}

// The French are Remy, Adam, Maxime and Luc; the PhDs Remy, Adam, Luc and Martina
TEST_F(NextQueryTest, readsTheWholeResultOfAUnion) {
    expectRows("MATCH (p:Person) WHERE p.isFrench RETURN p.name AS name "
               "UNION MATCH (p:Person) WHERE p.hasPhD RETURN p.name AS name "
               "NEXT RETURN count(*) AS n",
               {{"5"}});

    expectRows("MATCH (p:Person) WHERE p.isFrench RETURN p.name AS name "
               "UNION ALL MATCH (p:Person) WHERE p.hasPhD RETURN p.name AS name "
               "NEXT RETURN count(*) AS n",
               {{"8"}});
}

TEST_F(NextQueryTest, runsEveryBranchOfAUnionOverTheWholeTable) {
    expectRows("MATCH (p:Person) RETURN p.name AS name, p.isFrench AS french "
               "NEXT RETURN count(*) AS n "
               "UNION ALL WITH name, french WHERE french RETURN count(name) AS n",
               {{"8"}, {"4"}});
}

TEST_F(NextQueryTest, dedupsAUnionAcrossTheWholeTable) {
    expectRows("UNWIND [1, 1, 2] AS x RETURN x NEXT RETURN x UNION RETURN x + 1 AS x",
               {{"1"}, {"2"}, {"3"}});
}

TEST_F(NextQueryTest, cutsOneBranchOfAUnionWithoutCuttingTheOthers) {
    expectRows("MATCH (p:Person) RETURN p.name AS name "
               "NEXT RETURN name ORDER BY name LIMIT 2 "
               "UNION ALL RETURN name ORDER BY name DESC LIMIT 1",
               {{"Adam"}, {"Cyrus"}, {"Suhas"}});

    expectRows("MATCH (p:Person) RETURN p.name AS name "
               "NEXT RETURN name LIMIT 2 UNION ALL RETURN name "
               "NEXT RETURN count(*) AS n",
               {{"10"}});
}

TEST_F(NextQueryTest, feedsAUnionIntoAnotherUnion) {
    expectRows("UNWIND [1, 2] AS x RETURN x UNION ALL UNWIND [3] AS x RETURN x "
               "NEXT RETURN sum(x) AS x UNION ALL RETURN max(x) AS x "
               "NEXT RETURN sum(x) AS total",
               {{"9"}});
}

// Remy, Adam, Luc and Martina have a PhD
TEST_F(NextQueryTest, decidesAWhenPerRow) {
    expectRows("MATCH (p:Person) RETURN p.name AS name, p.hasPhD AS phd "
               "NEXT WHEN phd THEN RETURN name, 'doctor' AS title ELSE RETURN name, 'student' AS title",
               {{"Remy", "doctor"}, {"Adam", "doctor"}, {"Luc", "doctor"}, {"Martina", "doctor"},
                {"Maxime", "student"}, {"Suhas", "student"}, {"Cyrus", "student"}, {"Doruk", "student"}});
}

TEST_F(NextQueryTest, rebindsANameAWhenReturnsAgain) {
    expectRows("MATCH (p:Person) RETURN p AS person "
               "NEXT WHEN person.isFrench THEN RETURN person.name AS person ELSE RETURN 'other' AS person",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"other"}, {"other"}, {"other"}, {"other"}});
}

TEST_F(NextQueryTest, aggregatesWhatAWhenReturned) {
    expectRows("MATCH (p:Person) RETURN p.hasPhD AS phd "
               "NEXT WHEN phd THEN RETURN 'doctor' AS title ELSE RETURN 'student' AS title "
               "NEXT RETURN title, count(*) AS n",
               {{"doctor", "4"}, {"student", "4"}});
}

TEST_F(NextQueryTest, startsFromAStandaloneWhen) {
    expectRows("WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x NEXT RETURN x + 1 AS y", {{"2"}});
}

TEST_F(NextQueryTest, groupsPerRowInsideACall) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN i NEXT RETURN count(i) AS n } "
               "RETURN p.name AS name, n",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(NextQueryTest, countsWhatAChainedBodyReturns) {
    expectRows("MATCH (p:Person) WHERE p.isFrench "
               "RETURN p.name AS name, "
               "COUNT { MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN i NEXT RETURN i } AS n",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"}});
}

TEST_F(NextQueryTest, chainsInsideAUnionOperand) {
    expectRows("{ MATCH (p:Person) RETURN p.name AS name NEXT RETURN name ORDER BY name LIMIT 1 } "
               "UNION ALL "
               "{ MATCH (i:Interest) RETURN i.name AS name NEXT RETURN name ORDER BY name LIMIT 1 }",
               {{"Adam"}, {"Animals"}});
}

TEST_F(NextQueryTest, chainsInsideAWhenBranch) {
    expectRows("WHEN true THEN { MATCH (p:Person) RETURN p.age AS age NEXT RETURN max(age) AS a } "
               "ELSE { RETURN 0 AS a }",
               {{"32"}});
}

TEST_F(NextQueryTest, keepsNextAVariableName) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' RETURN p.name AS next", {{"Remy"}});
}
