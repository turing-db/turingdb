#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CASE beside an aggregate reads the grouping key and the aggregate of the groups its branch
// takes. The 8 nodes no Person is INTERESTED_IN form one group whose key is null.
class CaseBesideAnAggregateTest : public WriteQueryTest {
};

TEST_F(CaseBesideAnAggregateTest, readsTheGroupingKeyInABranch) {
    expectRows("MATCH (f) OPTIONAL MATCH (f)<-[:INTERESTED_IN]-(p:Person) "
               "WITH p, CASE WHEN p IS NULL THEN -1 ELSE count(f) * 100 + size(p.name) END AS q RETURN p.name, q",
               {{"Remy", "304"}, {"Adam", "204"}, {"Maxime", "206"}, {"Luc", "203"}, {"Martina", "107"},
                {"Suhas", "205"}, {"Cyrus", "205"}, {"Doruk", "105"}, {"null", "-1"}});
}
