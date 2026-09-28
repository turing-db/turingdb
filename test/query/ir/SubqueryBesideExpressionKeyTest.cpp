#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Grouped on p.name, count(*) is each person's interests: Remy 3, Martina and Doruk 1, the
// other five 2. The graph holds 8 people and 10 interests.
class SubqueryBesideExpressionKeyTest : public WriteQueryTest {
};

TEST_F(SubqueryBesideExpressionKeyTest, countAddsToTheCountOfEachGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) RETURN p.name, count(*) + COUNT { MATCH (x:Person) } AS c",
               {{"Remy", "11"}, {"Adam", "10"}, {"Maxime", "10"}, {"Luc", "10"},
                {"Martina", "9"}, {"Suhas", "10"}, {"Cyrus", "10"}, {"Doruk", "9"}});
}

TEST_F(SubqueryBesideExpressionKeyTest, countRunPerRowAddsToTheCountOfEachGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, count(*) + COUNT { MATCH (x:Person) RETURN x LIMIT 2 } AS c",
               {{"Remy", "5"}, {"Adam", "4"}, {"Maxime", "4"}, {"Luc", "4"},
                {"Martina", "3"}, {"Suhas", "4"}, {"Cyrus", "4"}, {"Doruk", "3"}});
}

TEST_F(SubqueryBesideExpressionKeyTest, existsJoinsTheComparisonOfEachGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, count(*) > 1 AND EXISTS { MATCH (x:Interest) } AS e",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "true"}, {"Luc", "true"},
                {"Martina", "false"}, {"Suhas", "true"}, {"Cyrus", "true"}, {"Doruk", "false"}});
}

TEST_F(SubqueryBesideExpressionKeyTest, countInAWithAddsToTheCountOfEachGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH p.name AS name, count(*) + COUNT { MATCH (x:Person) } AS c "
               "RETURN name, c",
               {{"Remy", "11"}, {"Adam", "10"}, {"Maxime", "10"}, {"Luc", "10"},
                {"Martina", "9"}, {"Suhas", "10"}, {"Cyrus", "10"}, {"Doruk", "9"}});
}

TEST_F(SubqueryBesideExpressionKeyTest, ordersTheGroupsOnACountBesideTheirAggregate) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "RETURN p.name AS name, count(*) AS c "
                      "ORDER BY COUNT { MATCH (x:Interest) } + c DESC, name",
                      {{"Remy", "3"}, {"Adam", "2"}, {"Cyrus", "2"}, {"Luc", "2"},
                       {"Maxime", "2"}, {"Suhas", "2"}, {"Doruk", "1"}, {"Martina", "1"}});
}

TEST_F(SubqueryBesideExpressionKeyTest, countAddsToAKeylessAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*) + COUNT { MATCH (i:Interest) } AS c", {{"18"}});
}
