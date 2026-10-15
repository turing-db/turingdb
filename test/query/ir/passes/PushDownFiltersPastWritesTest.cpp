#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A WHERE after a CREATE filters the rows the query goes on with, not the rows the CREATE
// writes: every row reaching the CREATE writes, whatever the WHERE keeps afterwards
class PushDownFiltersPastWritesTest : public CallV3Test {
protected:
    Rows runWriting(std::string_view query) {
        StringRowSink sink;
        runWrite(query, sink);

        Rows rows;
        sink.sortedRows(rows);
        return rows;
    }

    Rows run(std::string_view query) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);
        return rows;
    }

    Rows seenEdges() {
        return run("MATCH (x)-[:SEEN]->(y) RETURN x, y");
    }
};

TEST_F(PushDownFiltersPastWritesTest, createsAnEdgePerRowAHopAfterAWalkReaches) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i) "
                                   "CREATE (b)-[:SEEN]->(i) "
                                   "WITH a, b WHERE b.name = 'Adam' "
                                   "RETURN a, b";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "1"}, {"0", "1"}}));
    EXPECT_EQ(seenEdges(), (Rows {{"0", "2"}, {"0", "3"}, {"0", "6"}, {"1", "4"}, {"1", "5"}}));
}

TEST_F(PushDownFiltersPastWritesTest, createsAnEdgePerRowAWalkReaches) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b) "
                                   "CREATE (a)-[:SEEN]->(b) "
                                   "WITH a, b WHERE b.name = 'Adam' "
                                   "RETURN a, b";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "1"}}));
    EXPECT_EQ(seenEdges(), (Rows {{"0", "0"}, {"0", "1"}}));
}

TEST_F(PushDownFiltersPastWritesTest, createsAnEdgePerRowAHopReaches) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[:INTERESTED_IN]->(i) "
                                   "CREATE (a)-[:SEEN]->(i) "
                                   "WITH a, i WHERE i.name = 'Ghosts' "
                                   "RETURN a, i";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "6"}}));
    EXPECT_EQ(seenEdges(), (Rows {{"0", "2"}, {"0", "3"}, {"0", "6"}}));
}

TEST_F(PushDownFiltersPastWritesTest, createsANodePerRowAHopAfterAWalkReaches) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i) "
                                   "CREATE (s:Seen) "
                                   "WITH a, b WHERE b.name = 'Adam' "
                                   "RETURN a, b";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "1"}, {"0", "1"}}));
    EXPECT_EQ(run("MATCH (s:Seen) RETURN count(s)"), (Rows {{"5"}}));
}

TEST_F(PushDownFiltersPastWritesTest, setsAPropertyOnEveryRowAHopAfterAWalkReaches) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i) "
                                   "SET i.seen = true "
                                   "WITH a, b WHERE b.name = 'Adam' "
                                   "RETURN a, b";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "1"}, {"0", "1"}}));
    EXPECT_EQ(run("MATCH (i) WHERE i.seen = true RETURN i"), (Rows {{"2"}, {"3"}, {"4"}, {"5"}, {"6"}}));
}

TEST_F(PushDownFiltersPastWritesTest, createsAnEdgePerRowAHopFromTheFilteredNodeReaches) {
    const std::string_view query = "MATCH (a)-[:KNOWS_WELL]->(b) "
                                   "CREATE (a)-[:SEEN]->(b) "
                                   "WITH a, b WHERE a.name = 'Remy' "
                                   "RETURN a, b";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "1"}}));
    EXPECT_EQ(seenEdges(), (Rows {{"0", "1"}, {"1", "0"}, {"6", "0"}}));
}

TEST_F(PushDownFiltersPastWritesTest, createsAnEdgePerRowOfACrossProduct) {
    const std::string_view query = "MATCH (a {name:'Remy'}), (b:Interest) "
                                   "CREATE (a)-[:SEEN]->(b) "
                                   "WITH a, b WHERE b.name = 'Ghosts' "
                                   "RETURN a, b";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "6"}}));
    EXPECT_EQ(seenEdges(), (Rows {{"0", "10"}, {"0", "13"}, {"0", "14"}, {"0", "16"}, {"0", "2"}, {"0", "3"}, {"0", "4"}, {"0", "5"}, {"0", "6"}, {"0", "7"}}));
}

TEST_F(PushDownFiltersPastWritesTest, createsANodePerUnwoundRow) {
    const std::string_view query = "UNWIND [0, 1, 2] AS x "
                                   "MATCH (a) WHERE id(a) = x "
                                   "CREATE (s:Seen) "
                                   "WITH a WHERE a.name = 'Remy' "
                                   "RETURN a";

    EXPECT_EQ(runWriting(query), (Rows {{"0"}}));
    EXPECT_EQ(run("MATCH (s:Seen) RETURN count(s)"), (Rows {{"3"}}));
}
