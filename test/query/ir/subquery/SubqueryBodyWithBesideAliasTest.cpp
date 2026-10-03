#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Out-edges of each person: Remy 4, Adam 3, Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2,
// Doruk 1. Of the people, only Remy and Adam have a KNOWS_WELL out-edge.
class SubqueryBodyWithBesideAliasTest : public WriteQueryTest {
};

TEST_F(SubqueryBodyWithBesideAliasTest, countsThroughAWithBesideAnAliasedItem) {
    expectRows("MATCH (n:Person) RETURN n.name AS nm, COUNT { WITH 1 AS one MATCH (n)-->(x) } AS d",
               {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(SubqueryBodyWithBesideAliasTest, existsThroughAWithBesideAnAliasedItem) {
    expectRows("MATCH (n:Person) RETURN n.name AS nm, EXISTS { WITH 1 AS one MATCH (n)-[:KNOWS_WELL]->() } AS d",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}});
}

// In-degree of each out-neighbour, summed: Remy reaches Adam 1, Ghosts 1, Computers 2 and
// Eighties 1; Suhas reaches Gym 3 and JiuJitsu 1.
TEST_F(SubqueryBodyWithBesideAliasTest, countsThroughAWithBesideAnAggregate) {
    expectRows("MATCH (n:Person) "
               "WITH n, count(*) AS c, COUNT { MATCH (n)-->(x) WITH x MATCH (x)<--(y) } AS d "
               "RETURN n.name, c, d",
               {{"Remy", "1", "5"}, {"Adam", "1", "6"}, {"Maxime", "1", "3"}, {"Luc", "1", "3"},
                {"Martina", "1", "2"}, {"Suhas", "1", "4"}, {"Cyrus", "1", "4"}, {"Doruk", "1", "3"}});
}
