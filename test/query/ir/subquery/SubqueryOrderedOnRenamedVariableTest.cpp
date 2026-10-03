#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A variable renamed by the projection it is ordered in. Out-edges of each person: Remy 4,
// Adam 3, Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2, Doruk 1. Node IDs: Remy 0, Adam 1,
// Maxime 8, Luc 9, Martina 11, Suhas 12, Cyrus 15, Doruk 17.
class SubqueryOrderedOnRenamedVariableTest : public WriteQueryTest {
};

TEST_F(SubqueryOrderedOnRenamedVariableTest, ordersOnACountReadingTheRenamedVariable) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name AS n, p AS q "
                      "ORDER BY COUNT { (q)-->() } DESC, n",
                      {{"Remy", "0"}, {"Adam", "1"}, {"Cyrus", "15"}, {"Luc", "9"},
                       {"Maxime", "8"}, {"Suhas", "12"}, {"Doruk", "17"}, {"Martina", "11"}});
}

TEST_F(SubqueryOrderedOnRenamedVariableTest, ordersOnAnExistsReadingTheRenamedVariable) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name AS n, p AS q "
                      "ORDER BY EXISTS { (q)-[:KNOWS_WELL]->() } DESC, n",
                      {{"Adam", "1"}, {"Remy", "0"}, {"Cyrus", "15"}, {"Doruk", "17"},
                       {"Luc", "9"}, {"Martina", "11"}, {"Maxime", "8"}, {"Suhas", "12"}});
}

TEST_F(SubqueryOrderedOnRenamedVariableTest, ordersAWithOnACountReadingTheRenamedVariable) {
    expectRowsInOrder("MATCH (p:Person) WITH p AS q "
                      "ORDER BY COUNT { (q)-->() } DESC, q.name LIMIT 3 "
                      "RETURN q.name",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}});
}
