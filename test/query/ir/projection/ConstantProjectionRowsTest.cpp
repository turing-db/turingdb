#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A RETURN projecting constants alone emits one row per row the MATCH binds, as count(*)
// tallies them, however many of those rows share both ends of a variable-length pattern
class ConstantProjectionRowsTest : public CallV3Test {
protected:
    void expectOneRowPerMatch(std::string_view match, std::string_view projection, size_t expectedCount) {
        StringRowSink counted;
        runQuery(std::string(match) + " RETURN count(*)", counted);
        ASSERT_EQ(counted.getRows().size(), 1);
        EXPECT_EQ(counted.getRows().front().front(), std::to_string(expectedCount)) << match;

        StringRowSink sink;
        runQuery(std::string(match) + " " + std::string(projection), sink);
        EXPECT_EQ(sink.getRows().size(), expectedCount) << match << " " << projection;
    }
};

TEST_F(ConstantProjectionRowsTest, returnsOneRowPerPath) {
    expectOneRowPerMatch("MATCH (a:Person)-[e*1..2]->(b)", "RETURN 1", 25);
    expectOneRowPerMatch("MATCH (a:Person)-[*1..2]->(b)", "RETURN null", 25);
    expectOneRowPerMatch("MATCH (a:Person)-[e*1..2]->(b)", "RETURN [1] AS l", 25);
    expectOneRowPerMatch("MATCH (:Person)-[*1..2]->()", "RETURN 1", 25);
}

TEST_F(ConstantProjectionRowsTest, returnsBothPathsSharingTheirEnds) {
    expectOneRowPerMatch("MATCH (a:Person {name: 'Remy'})-[*2..2]->(b)", "RETURN 1", 4);
    expectOneRowPerMatch("MATCH p = (a:Person {name: 'Remy'})-[*2..2]->(b)",
                         "RETURN toStringOrNull(relationships(p)), toStringOrNull(nodes(p))",
                         4);
}

TEST_F(ConstantProjectionRowsTest, returnsOneRowPerPathPastAHopOrAFilter) {
    expectOneRowPerMatch("MATCH (a:Person)-[e*1..2]->(b)-->(c)", "RETURN 1", 20);
    expectOneRowPerMatch("MATCH (a:Person)-[e*1..2]->(b) WHERE b.name <> 'x'", "RETURN 1", 25);
    expectOneRowPerMatch("MATCH (a:Person)-[e*1..2]->(b) WHERE b:Person", "RETURN 1", 5);
}

TEST_F(ConstantProjectionRowsTest, cutsThePathsAfterCountingThem) {
    expectOneRowPerMatch("MATCH (a:Person)-[e*1..2]->(b)", "RETURN 1 LIMIT 100", 25);

    StringRowSink skipped;
    runQuery("MATCH (a:Person)-[e*1..2]->(b) RETURN 1 SKIP 1", skipped);
    EXPECT_EQ(skipped.getRows().size(), 24);
}
