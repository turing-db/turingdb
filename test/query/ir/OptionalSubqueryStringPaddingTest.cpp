#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An OPTIONAL CALL pads every column the body returned, a column that owns its strings
// included: a missed row reads null there, not the empty string
class OptionalSubqueryStringPaddingTest : public CallV3Test {
protected:
    void expectRows(const char* query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }
};

// db.procedures() writes its name through a column owning its strings. Only Remy and Adam
// know anybody well, so the body yields for those two and the six others are padded
TEST_F(OptionalSubqueryStringPaddingTest, padsAnOwnedStringColumnWithNull) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) "
               "CALL db.procedures() YIELD name WHERE name = 'db.labels' RETURN name AS n } "
               "RETURN p.name, n",
               {{"Remy", "db.labels"},
                {"Adam", "db.labels"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// The same body with no OPTIONAL, over the two rows it yields for
TEST_F(OptionalSubqueryStringPaddingTest, answersAsTheSameQueryWithoutOptional) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) "
               "CALL db.procedures() YIELD name WHERE name = 'db.labels' RETURN name AS n } "
               "RETURN p.name, n",
               {{"Remy", "db.labels"}, {"Adam", "db.labels"}});
}
