#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A grouped count(DISTINCT x) counts a value once per group exactly when a whole-row
// DISTINCT would fold its rows together, whatever x holds. Each query crosses its rows
// with the 8 people, so count(x) is 8 times count(DISTINCT x).
class GroupedCountDistinctTest : public CallV3Test {
};

TEST_F(GroupedCountDistinctTest, countsEachPathOncePerGroup) {
    StringRowSink sink;
    runQuery("MATCH p = (a:Person)-[e]->(b), (c:Person) "
             "RETURN a.name AS name, count(p), count(DISTINCT p) ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "24", "3"},
        {"Cyrus", "16", "2"},
        {"Doruk", "8", "1"},
        {"Luc", "16", "2"},
        {"Martina", "8", "1"},
        {"Maxime", "16", "2"},
        {"Remy", "32", "4"},
        {"Suhas", "16", "2"},
    }));
}

TEST_F(GroupedCountDistinctTest, countsEachWalkOncePerGroup) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person), (c:Person) "
             "RETURN n.name AS name, count(p), count(DISTINCT p) ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "32", "4"},
        {"Remy", "48", "6"},
    }));
}

// The people with no edge to another person bind the null path, which no count charges
TEST_F(GroupedCountDistinctTest, chargesNoNullPath) {
    StringRowSink sink;
    runQuery("MATCH (a:Person), (c:Person) OPTIONAL MATCH p = (a)-[e]->(b:Person) "
             "RETURN a.name AS name, count(p), count(DISTINCT p) ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "8", "1"},
        {"Cyrus", "0", "0"},
        {"Doruk", "0", "0"},
        {"Luc", "0", "0"},
        {"Martina", "0", "0"},
        {"Maxime", "0", "0"},
        {"Remy", "8", "1"},
        {"Suhas", "0", "0"},
    }));
}

TEST_F(GroupedCountDistinctTest, chargesNoNullNode) {
    StringRowSink sink;
    runQuery("MATCH (a:Person), (c:Person) OPTIONAL MATCH (a)-[e]->(b:Person) "
             "RETURN a.name AS name, count(b), count(DISTINCT b) ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "8", "1"},
        {"Cyrus", "0", "0"},
        {"Doruk", "0", "0"},
        {"Luc", "0", "0"},
        {"Martina", "0", "0"},
        {"Maxime", "0", "0"},
        {"Remy", "8", "1"},
        {"Suhas", "0", "0"},
    }));
}

// Maxime and Martina are the two people whose name starts with M
TEST_F(GroupedCountDistinctTest, countsEachMapOncePerGroup) {
    StringRowSink sink;
    runQuery("MATCH (a:Person), (c:Person) WITH c, {m: a.name STARTS WITH 'M'} AS initial "
             "RETURN c.name AS name, count(initial), count(DISTINCT initial) ORDER BY name",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"Adam", "8", "2"},
        {"Cyrus", "8", "2"},
        {"Doruk", "8", "2"},
        {"Luc", "8", "2"},
        {"Martina", "8", "2"},
        {"Maxime", "8", "2"},
        {"Remy", "8", "2"},
        {"Suhas", "8", "2"},
    }));
}
