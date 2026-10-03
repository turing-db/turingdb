#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

}

class FetchNodesTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void dbProgramOf(std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    void expectFetchesEveryNode(std::string_view query) {
        std::string program;
        dbProgramOf(query, program);

        EXPECT_TRUE(contains(program, "db.fetch_nodes")) << program;
        EXPECT_FALSE(contains(program, "db.scan_nodes")) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
    }
};

TEST_F(FetchNodesTest, fetchesTwoNodesOfOneUnwoundRow) {
    const std::string_view query = "WITH [0, 2] AS ss, [1, 3] AS ds UNWIND range(0, size(ss) - 1) AS i "
                                   "MATCH (a), (b) WHERE a = ss[i] AND b = ds[i] RETURN a.name, b.name";

    expectFetchesEveryNode(query);
    expectRows(query, {{"Computers", "Eighties"}, {"Remy", "Adam"}});
}

TEST_F(FetchNodesTest, createsAnEdgeBetweenTheFetchedNodes) {
    runWrite("WITH [0, 2] AS ss, [1, 3] AS ds UNWIND range(0, size(ss) - 1) AS i "
             "MATCH (a), (b) WHERE a = ss[i] AND b = ds[i] CREATE (a)-[:K]->(b)");

    expectRows("MATCH (a)-[:K]->(b) RETURN a.name, b.name", {{"Computers", "Eighties"}, {"Remy", "Adam"}});
}

TEST_F(FetchNodesTest, fetchesTwoNodesOfOneLiteralRow) {
    const std::string_view query = "UNWIND [[0, 1], [2, 3]] AS r MATCH (a), (b) WHERE a = r[0] AND b = r[1] "
                                   "RETURN a.name, b.name";

    expectFetchesEveryNode(query);
    expectRows(query, {{"Computers", "Eighties"}, {"Remy", "Adam"}});
}

TEST_F(FetchNodesTest, dropsTheCellsNamingNoNode) {
    const std::string_view query = "WITH [0, 99, -1, null, 'x', 2.0, 1] AS ss UNWIND range(0, size(ss) - 1) AS i "
                                   "MATCH (a) WHERE a = ss[i] RETURN i, a.name";

    expectFetchesEveryNode(query);
    expectRows(query, {{"0", "Remy"}, {"6", "Adam"}});
}

TEST_F(FetchNodesTest, keepsARowPerRepeatedID) {
    expectRows("UNWIND [1, 1, 'x'] AS k MATCH (a) WHERE a = k RETURN a.name", {{"Adam"}, {"Adam"}});
}

TEST_F(FetchNodesTest, keepsTheLabelOfTheScan) {
    const std::string_view query = "WITH [0, 2, 1] AS ss UNWIND range(0, 2) AS i "
                                   "MATCH (a:Person) WHERE a = ss[i] RETURN a.name";

    std::string program;
    dbProgramOf(query, program);
    EXPECT_TRUE(contains(program, "db.fetch_nodes")) << program;
    EXPECT_FALSE(contains(program, "db.scan_nodes_by_label")) << program;

    expectRows(query, {{"Adam"}, {"Remy"}});
}

TEST_F(FetchNodesTest, fetchesANodeFromAnotherNode) {
    const std::string_view query = "MATCH (a), (b) WHERE a = b RETURN count(*)";

    std::string program;
    dbProgramOf(query, program);
    EXPECT_TRUE(contains(program, "db.fetch_nodes")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    expectRows(query, {{"18"}});
}

TEST_F(FetchNodesTest, fetchesNoDeletedNode) {
    runWrite("MATCH (n) WHERE n = 3 DETACH DELETE n");

    expectRows("WITH [3, 4] AS ss UNWIND range(0, 1) AS i MATCH (a) WHERE a = ss[i] RETURN a.name", {{"Bio"}});
}

TEST_F(FetchNodesTest, stopsAtTheLimit) {
    expectRows("WITH [0, 1, 2, 3] AS ss UNWIND range(0, 3) AS i MATCH (a) WHERE a = ss[i] RETURN count(*)",
               {{"4"}});

    StringRowSink sink;
    runQuery("WITH [0, 99, 1, 2, 3] AS ss UNWIND range(0, 4) AS i MATCH (a) WHERE a = ss[i] RETURN a LIMIT 2", sink);
    EXPECT_EQ(sink.getRows().size(), 2u);
}

TEST_F(FetchNodesTest, fetchesANodeTheQueryCreated) {
    StringRowSink sink;
    runWrite("CREATE (z:Person {name: 'Zed'}) WITH z MATCH (a:Person) WHERE a = z RETURN a.name", sink);

    const Rows expected {{"Zed"}};
    EXPECT_EQ(sink.getRows(), expected);
}
