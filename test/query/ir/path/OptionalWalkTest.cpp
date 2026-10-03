#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

// A walk in an OPTIONAL MATCH binds its relationship to the edges each match took, and to
// null on the rows nothing matched
class OptionalWalkTest : public CallV3Test {
};

TEST_F(OptionalWalkTest, bindsNullWhereTheWalkMissed) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) OPTIONAL MATCH (n)-[e]->+(m:Person) RETURN n.name, size(e), m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "1", "Adam"},
        {"Remy", "2", "Remy"},
        {"Remy", "2", "Remy"},
        {"Remy", "3", "Adam"},
        {"Remy", "4", "Remy"},
        {"Remy", "4", "Remy"},
        {"Adam", "1", "Remy"},
        {"Adam", "2", "Adam"},
        {"Adam", "3", "Remy"},
        {"Adam", "4", "Adam"},
        {"Maxime", "null", "null"},
        {"Luc", "null", "null"},
        {"Martina", "null", "null"},
        {"Suhas", "null", "null"},
        {"Cyrus", "null", "null"},
        {"Doruk", "null", "null"},
    }));
}

TEST_F(OptionalWalkTest, countsOnlyTheWalksItFound) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) OPTIONAL MATCH (n)-[e]->+(m:Person) RETURN count(e)", sink);

    EXPECT_EQ(sink.getRows(), Rows {{"10"}});
}

TEST_F(OptionalWalkTest, countsTheWalksOfEachRow) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) OPTIONAL MATCH (n)-[e]->+(m:Person) RETURN n.name, count(e)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "6"},
        {"Adam", "4"},
        {"Maxime", "0"},
        {"Luc", "0"},
        {"Martina", "0"},
        {"Suhas", "0"},
        {"Cyrus", "0"},
        {"Doruk", "0"},
    }));
}
