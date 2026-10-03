#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Behind an aggregate or a DISTINCT, the ORDER BY reads the projection's columns and no
// other variable, so a body naming one of the others declares it. INTERESTED_IN edges of
// each person: Remy 3, Adam 2, Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2, Doruk 1.
class SubqueryOrderedAfterGroupingTest : public WriteQueryTest {
};

TEST_F(SubqueryOrderedAfterGroupingTest, ordersTheGroupsOnACountDeclaringAConsumedName) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name AS n, count(*) AS k "
                      "ORDER BY COUNT { MATCH (p:Person)-[:INTERESTED_IN]->() WHERE p.name = n } DESC, n",
                      {{"Remy", "1"}, {"Adam", "1"}, {"Cyrus", "1"}, {"Luc", "1"},
                       {"Maxime", "1"}, {"Suhas", "1"}, {"Doruk", "1"}, {"Martina", "1"}});
}

TEST_F(SubqueryOrderedAfterGroupingTest, ordersTheGroupsOnAnExistsDeclaringAConsumedName) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name AS n, count(*) AS k "
                      "ORDER BY EXISTS { MATCH (p:Person)-[:KNOWS_WELL]->() WHERE p.name = n } DESC, n",
                      {{"Adam", "1"}, {"Remy", "1"}, {"Cyrus", "1"}, {"Doruk", "1"},
                       {"Luc", "1"}, {"Martina", "1"}, {"Maxime", "1"}, {"Suhas", "1"}});
}

TEST_F(SubqueryOrderedAfterGroupingTest, ordersTheGroupsOnACountOverAConsumedNameAlone) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.age AS a, count(*) AS k "
                      "ORDER BY COUNT { MATCH (p:Interest) } DESC, k",
                      {{"32", "2"}, {"null", "6"}});
}

TEST_F(SubqueryOrderedAfterGroupingTest, ordersDistinctRowsOnACountDeclaringAConsumedName) {
    expectRowsInOrder("MATCH (p:Person) RETURN DISTINCT p.name AS n "
                      "ORDER BY COUNT { MATCH (p:Person)-[:INTERESTED_IN]->() WHERE p.name = n } DESC, n",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Luc"},
                       {"Maxime"}, {"Suhas"}, {"Doruk"}, {"Martina"}});
}
