#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Interests of each person: Remy 3, Martina and Doruk 1, the other five 2. Of the people,
// only Remy and Adam have a KNOWS_WELL out-edge.
class SubqueryOrderedOnAliasTest : public WriteQueryTest {
};

TEST_F(SubqueryOrderedOnAliasTest, ordersOnACountReadingAnAlias) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name AS n "
                      "ORDER BY COUNT { MATCH (y:Person)-[:INTERESTED_IN]->() WHERE y.name = n } DESC, n",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Luc"},
                       {"Maxime"}, {"Suhas"}, {"Doruk"}, {"Martina"}});
}

TEST_F(SubqueryOrderedOnAliasTest, ordersAWithOnACountReadingAnAlias) {
    expectRowsInOrder("MATCH (p:Person) WITH p.name AS n "
                      "ORDER BY COUNT { MATCH (y:Person)-[:INTERESTED_IN]->() WHERE y.name = n } DESC, n LIMIT 3 "
                      "RETURN n",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}});
}

TEST_F(SubqueryOrderedOnAliasTest, ordersOnAnExistsReadingAnAlias) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name AS n "
                      "ORDER BY EXISTS { MATCH (y:Person)-[:KNOWS_WELL]->() WHERE y.name = n } DESC, n",
                      {{"Adam"}, {"Remy"}, {"Cyrus"}, {"Doruk"},
                       {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}

TEST_F(SubqueryOrderedOnAliasTest, ordersTheGroupsOnACountReadingAKeyAlias) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) RETURN p.name AS n, count(*) AS c "
                      "ORDER BY COUNT { MATCH (y:Person)-[:KNOWS_WELL]->() WHERE y.name = n } DESC, n",
                      {{"Adam", "2"}, {"Remy", "3"}, {"Cyrus", "2"}, {"Doruk", "1"},
                       {"Luc", "2"}, {"Martina", "1"}, {"Maxime", "2"}, {"Suhas", "2"}});
}
