#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Out-edges of each person: Remy 4, Adam 3, Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2,
// Doruk 1, so 17 in all. Of the 8 people, only Remy and Adam have a KNOWS_WELL out-edge.
class SubqueryInAggregateArgumentTest : public WriteQueryTest {
};

TEST_F(SubqueryInAggregateArgumentTest, sumsACountAfterAnotherAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*) AS c, sum(COUNT { (p)-->() }) AS m", {{"8", "17"}});
}

TEST_F(SubqueryInAggregateArgumentTest, takesTheLargestCountAfterAnotherAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*) AS c, max(COUNT { (p)-->() }) AS m", {{"8", "4"}});
}

TEST_F(SubqueryInAggregateArgumentTest, sumsAPerRowCountAfterAnotherAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*) AS c, sum(COUNT { MATCH (p)-->(x) RETURN x LIMIT 1 }) AS m",
               {{"8", "8"}});
}

TEST_F(SubqueryInAggregateArgumentTest, countsAnExistsAfterAnotherAggregate) {
    expectRows("MATCH (p:Person) RETURN count(p.name) AS c, count(EXISTS { (p)-[:KNOWS_WELL]->() }) AS e",
               {{"8", "8"}});

    expectRows("MATCH (p:Person) "
               "RETURN count(*) AS c, sum(CASE WHEN EXISTS { (p)-[:KNOWS_WELL]->() } THEN 1 ELSE 0 END) AS e",
               {{"8", "2"}});
}
