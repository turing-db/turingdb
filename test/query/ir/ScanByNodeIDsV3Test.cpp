#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class ScanByNodeIDsV3Test : public CallV3Test {
};

TEST_F(ScanByNodeIDsV3Test, disjunctionYieldsListedNodesInScanOrder) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n = 5 OR n = 2 RETURN n", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}, {"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, singleEquality) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n = 0 RETURN n.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, repeatedIDYieldsTheNodeOnce) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n = 3 OR n = 3 RETURN n", sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, absentIDMatchesNothing) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n = 2 OR n = 99999 RETURN n", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, countsOverTheListedSet) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n = 3 OR n = 1 OR n = 7 RETURN count(n)", sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, disjunctionOnAnExpandedRootMatchesThePropertyForm) {
    StringRowSink byID;
    runQuery("MATCH (n)-->(m) WHERE n = 0 OR n = 1 RETURN n, m", byID);

    StringRowSink byName;
    runQuery("MATCH (n)-->(m) WHERE n.name = 'Remy' OR n.name = 'Adam' RETURN n, m", byName);

    std::vector<StringRowSink::Row> byIDRows;
    byID.sortedRows(byIDRows);
    std::vector<StringRowSink::Row> byNameRows;
    byName.sortedRows(byNameRows);

    EXPECT_FALSE(byIDRows.empty());
    EXPECT_EQ(byIDRows, byNameRows);
}

TEST_F(ScanByNodeIDsV3Test, disjunctionInACrossProductFactor) {
    StringRowSink sink;
    runQuery("MATCH (n), (m) WHERE n = 2 OR n = 4 RETURN count(m)", sink);

    // Two listed nodes crossed with every one of simpledb's 18 nodes.
    const std::vector<StringRowSink::Row> expected {{"36"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, mixedPredicateStaysAFilter) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n = 5 OR n.name = 'Remy' RETURN n", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, labelledDisjunctionKeepsOnlyTheLabelledNodes) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) WHERE n = 2 OR n = 0 RETURN n.name", sink);

    // Remy (0) is a Person; Computers (2) is an Interest.
    const std::vector<StringRowSink::Row> expected {{"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, labelledDisjunctionYieldsInScanOrder) {
    StringRowSink sink;
    runQuery("MATCH (n:Interest) WHERE n = 5 OR n = 0 OR n = 2 RETURN n.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Computers"}, {"Cooking"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByNodeIDsV3Test, labelledDisjunctionOnAnExpandedRootMatchesThePropertyForm) {
    StringRowSink byID;
    runQuery("MATCH (n:Person)-->(m) WHERE n = 0 OR n = 1 OR n = 2 RETURN n, m", byID);

    StringRowSink byName;
    runQuery("MATCH (n:Person)-->(m) WHERE n.name = 'Remy' OR n.name = 'Adam' OR n.name = 'Computers' RETURN n, m", byName);

    std::vector<StringRowSink::Row> byIDRows;
    byID.sortedRows(byIDRows);
    std::vector<StringRowSink::Row> byNameRows;
    byName.sortedRows(byNameRows);

    EXPECT_FALSE(byIDRows.empty());
    EXPECT_EQ(byIDRows, byNameRows);
}
