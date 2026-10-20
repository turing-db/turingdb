#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "HashJoinQueryTest.h"

using namespace turing::test;

// A node or an edge equals the integer naming its ID. No ID is negative, so a negative
// integer equals no node and no edge.
class EntityNegativeIntegerTest : public HashJoinQueryTest {
};

TEST_F(EntityNegativeIntegerTest, equalsNoNode) {
    expectCount("MATCH (n) WHERE n = -1 RETURN count(*)", 0);
    expectCount("MATCH (n) WHERE -1 = n RETURN count(*)", 0);
}

TEST_F(EntityNegativeIntegerTest, differsFromEveryNode) {
    expectCount("MATCH (n) WHERE n <> -1 RETURN count(*)", 18);
    expectCount("MATCH (n) WHERE -1 <> n RETURN count(*)", 18);
}

TEST_F(EntityNegativeIntegerTest, equalsNoEdge) {
    expectCount("MATCH ()-[e]->() WHERE e = -100 RETURN count(*)", 0);
    expectCount("MATCH ()-[e]->() WHERE e <> -100 RETURN count(*)", 18);
}

// Remy and Adam give -32, the six people without an age null
TEST_F(EntityNegativeIntegerTest, comparesASignedColumn) {
    expectCount("MATCH (a:Person) WHERE a = -a.age RETURN count(*)", 0);
    expectCount("MATCH (a:Person) WHERE a <> -a.age RETURN count(*)", 2);
}

TEST_F(EntityNegativeIntegerTest, fetchesNoNodeForANegativeID) {
    const std::string_view query = "UNWIND range(-2, 1) AS i MATCH (a) WHERE a = i RETURN a.name";

    std::string program;
    dbProgram(query, program);
    EXPECT_TRUE(contains(program, "db.fetch_nodes")) << program;

    expectRows(query, {{"Remy"}, {"Adam"}});
}

// The first WHERE keeps the six people without an age, whose comparison is null. Remy's and
// Adam's -32 is only on the rows it dropped.
TEST_F(EntityNegativeIntegerTest, comparesAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE (a = toInteger('-' + toString(a.age))) IS NULL RETURN count(*)",
                6);
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE (a <> toInteger('-' + toString(a.age))) IS NULL RETURN count(*)",
                6);
}
