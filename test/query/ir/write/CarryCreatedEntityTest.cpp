#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A hop carries a column the query created alongside the node it walks from. Remy knows
// Adam well and is INTERESTED_IN 3 interests.
class CarryCreatedEntityTest : public WriteQueryTest {
};

TEST_F(CarryCreatedEntityTest, carriesACreatedNodeThroughAHop) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) "
                    "CREATE (p)-[:KNOWS_WELL]->(m:Person {name: 'Z'}) "
                    "WITH p, m "
                    "MATCH (p)-[:KNOWS_WELL]->(x) "
                    "RETURN m.name, x.name",
                    {{"Z", "Adam"}, {"Z", "Z"}});
}

TEST_F(CarryCreatedEntityTest, carriesACreatedEdgeThroughAHop) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) "
                    "CREATE (p)-[e:KNOWS_WELL {since: 2020}]->(:Person) "
                    "WITH p, e "
                    "MATCH (p)-[:INTERESTED_IN]->(i) "
                    "RETURN e.since, count(i)",
                    {{"2020", "3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
