#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A map projection of a null is null, and computes none of its items. No Person is
// INTERESTED_IN another, so the optional match binds m to null for each of the 8 people.
class MapProjectionOverNullTest : public WriteQueryTest {
};

TEST_F(MapProjectionOverNullTest, computesNoItemOfAnUnmatchedNode) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:INTERESTED_IN]->(m:Person) RETURN n.name, m {v: 1 / 0}",
               {{"Remy", "null"}, {"Adam", "null"}, {"Maxime", "null"}, {"Luc", "null"},
                {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(MapProjectionOverNullTest, convertsNoPropertyOfAnUnmatchedNode) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:INTERESTED_IN]->(m:Person) "
               "RETURN n.name, m {.name, v: toInteger([[1], m.name][0])}",
               {{"Remy", "null"}, {"Adam", "null"}, {"Maxime", "null"}, {"Luc", "null"},
                {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(MapProjectionOverNullTest, computesNoItemOfANull) {
    expectRows("UNWIND [null, {a: 1}] AS x WITH x WHERE x IS NULL RETURN x {v: 1 / 0}", {{"null"}});
}

TEST_F(MapProjectionOverNullTest, computesTheItemsOfANode) {
    expectError("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n {.name, v: 1 / 0}", "Attempted to divide by zero");
}

TEST_F(MapProjectionOverNullTest, countsAnItemOverAGroupingKey) {
    expectRows("MATCH (n:Person)-->(f) WITH n, n {.name, c: count(f)} AS m RETURN m.name, m.c",
               {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

// The 8 nodes no Person is INTERESTED_IN form one group whose key is null
TEST_F(MapProjectionOverNullTest, countsAnItemOverANullGroupingKey) {
    expectRows("MATCH (f) OPTIONAL MATCH (f)<-[:INTERESTED_IN]-(p:Person) WITH p, p {.name, c: count(f)} AS q "
               "RETURN q.name, q.c",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"}, {"Martina", "1"},
                {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"null", "null"}});
}
