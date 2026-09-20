#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An OPTIONAL CALL pads every column the body returned, a list included: a missed row reads
// null there, not the empty list. StringRowSink renders a list as its elements joined by
// ", ", so a one-element list is that element and an absent one is "null"
class OptionalSubqueryListPaddingTest : public CallV3Test {
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

// The collect is keyed on kn, so it yields a row only for a Person with a KNOWS_WELL edge:
// Remy and Adam, each collecting the one name they know well
TEST_F(OptionalSubqueryListPaddingTest, padsAListColumnWithNull) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS kn, collect(k.name) AS ks } "
               "RETURN p.name, kn, ks",
               {{"Remy", "Adam", "Adam"},
                {"Adam", "Remy", "Remy"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// The same body with no OPTIONAL, over the two rows it yields for
TEST_F(OptionalSubqueryListPaddingTest, answersAsTheSameQueryWithoutOptional) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS kn, collect(k.name) AS ks } "
               "RETURN p.name, kn, ks",
               {{"Remy", "Adam", "Adam"}, {"Adam", "Remy", "Remy"}});
}
