#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An alias spelling the variable it was computed from. Out-edges of each person: Remy 4,
// Adam 3, Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2, Doruk 1.
class SubqueryOrderedOnShadowingAliasTest : public WriteQueryTest {
};

TEST_F(SubqueryOrderedOnShadowingAliasTest, ordersOnACountReadingTheAlias) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name AS p "
                      "ORDER BY COUNT { MATCH (y:Person)-->() WHERE y.name = p } DESC, p",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Luc"},
                       {"Maxime"}, {"Suhas"}, {"Doruk"}, {"Martina"}});
}

TEST_F(SubqueryOrderedOnShadowingAliasTest, ordersAWithOnACountReadingTheAlias) {
    expectRowsInOrder("MATCH (p:Person) WITH p.name AS p "
                      "ORDER BY COUNT { MATCH (y:Person)-->() WHERE y.name = p } DESC, p LIMIT 3 "
                      "RETURN p",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}});
}

TEST_F(SubqueryOrderedOnShadowingAliasTest, anItemBesideTheAliasCountsForTheVariable) {
    expectRows("MATCH (p:Person) RETURN p.name AS p, COUNT { (p)-->() } AS c",
               {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}
