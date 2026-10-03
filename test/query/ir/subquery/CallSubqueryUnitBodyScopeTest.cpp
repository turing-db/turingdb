#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A unit subquery - a body ending on an updating clause - runs once per row in flight, so
// an aggregate or a cut in its body covers the rows of one input row, as it does in a body
// ending on RETURN
class CallSubqueryUnitBodyScopeTest : public WriteQueryTest {
};

// One Stat per Person, holding that Person's own interest count: Remy 3, Martina and
// Doruk 1, the five others 2
TEST_F(CallSubqueryUnitBodyScopeTest, aggregatesOverOneInputRowAtATime) {
    applyWrite("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c CREATE (:Stat {n: c}) }");

    expectRows("MATCH (s:Stat) RETURN count(s)", {{"8"}});
    expectRows("MATCH (s:Stat) RETURN s.n",
               {{"1"}, {"1"}, {"2"}, {"2"}, {"2"}, {"2"}, {"2"}, {"3"}});
}

// The first interest by name of each Person, which is what the same body ending on RETURN
// yields: Computers ahead of Eighties and Ghosts for Remy, Gym ahead of JiuJitsu for Suhas
TEST_F(CallSubqueryUnitBodyScopeTest, cutsTheRowsOfOneInputRowAtATime) {
    applyWrite("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH i ORDER BY i.name LIMIT 1 "
               "CREATE (p)-[:TOP_INTEREST]->(i) }");

    expectRows("MATCH (p:Person)-[:TOP_INTEREST]->(i) RETURN p.name, i.name",
               {{"Remy", "Computers"},
                {"Adam", "Bio"},
                {"Maxime", "Bio"},
                {"Luc", "Animals"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Cyrus", "Gym"},
                {"Doruk", "Gym"}});
}
