#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class UnaryMinusTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// A count is carried unsigned, and its negation is the signed integer Cypher has
TEST_F(UnaryMinusTest, negatesACount) {
    expectRows("MATCH (p:Person) RETURN -count(p)", {{"-8"}});
}

TEST_F(UnaryMinusTest, negatesAPropertyAndADuration) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' RETURN -p.age, -duration({days: 1})", {{"-32", "PT-24H"}});
}

// The lowest int64 has no opposite an int64 can hold
TEST_F(UnaryMinusTest, rejectsTheNegationOfTheLowestInteger) {
    runQueryExpectingError("WITH -9223372036854775807 - 1 AS lowest RETURN -lowest", "Integer overflow");
}

TEST_F(UnaryMinusTest, rejectsTheNegationOfTheLowestIntegerInACell) {
    runQueryExpectingError("UNWIND [-9223372036854775807 - 1, 'x'] AS x WITH x LIMIT 1 RETURN -x", "Integer overflow");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
