#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class NullEntityAgainstIDTest : public WriteQueryTest {
};

TEST_F(NullEntityAgainstIDTest, comparesNullForAnUnmatchedEdge) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) RETURN p.name, r = 1000, r <> 1000",
               {
                   {"Adam", "false", "true"}, {"Cyrus", "null", "null"}, {"Doruk", "null", "null"},
                   {"Luc", "null", "null"}, {"Martina", "null", "null"}, {"Maxime", "null", "null"},
                   {"Remy", "false", "true"}, {"Suhas", "null", "null"},
               });
}

TEST_F(NullEntityAgainstIDTest, comparesNullForAnUnmatchedNode) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, 1000 <> f",
               {
                   {"Adam", "true"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
                   {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "true"}, {"Suhas", "null"},
               });
}

TEST_F(NullEntityAgainstIDTest, filtersOutAnUnmatchedEdge) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) WITH p, r WHERE r <> 1000 RETURN p.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(NullEntityAgainstIDTest, takesNoCaseBranchForAnUnmatchedEdge) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "RETURN p.name, CASE r WHEN <> 1000 THEN 'other' ELSE 'none' END",
               {
                   {"Adam", "other"}, {"Cyrus", "none"}, {"Doruk", "none"}, {"Luc", "none"},
                   {"Martina", "none"}, {"Maxime", "none"}, {"Remy", "other"}, {"Suhas", "none"},
               });
}
