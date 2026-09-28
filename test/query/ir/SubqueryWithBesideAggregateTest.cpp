#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A body holding a WITH, beside an aggregate that consumes the variables the body imports.
// The graph holds 8 people and 10 interests. Grouped on p.name, count(*) is each person's
// interests: Remy 3, Martina and Doruk 1, the other five 2.
class SubqueryWithBesideAggregateTest : public WriteQueryTest {
};

TEST_F(SubqueryWithBesideAggregateTest, addsACountThroughAWithToAKeylessAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*) + COUNT { MATCH (q:Interest) WITH q RETURN q }", {{"18"}});
}

TEST_F(SubqueryWithBesideAggregateTest, addsACountReturningEverythingToAKeylessAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*) + COUNT { MATCH (q:Interest) RETURN * }", {{"18"}});
}

TEST_F(SubqueryWithBesideAggregateTest, addsACountThroughAWithToTheCountOfEachGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, count(*) + COUNT { MATCH (q:Person) WITH q RETURN q } AS c",
               {{"Remy", "11"}, {"Adam", "10"}, {"Maxime", "10"}, {"Luc", "10"},
                {"Martina", "9"}, {"Suhas", "10"}, {"Cyrus", "10"}, {"Doruk", "9"}});
}

TEST_F(SubqueryWithBesideAggregateTest, existsThroughAWithBesideAKeylessAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*) > 0 AND EXISTS { MATCH (q:Interest) WITH q RETURN q }",
               {{"true"}});
}
