#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

using Rows = std::vector<StringRowSink::Row>;

// The check between the edges of two patterns a cross product brings together folds into
// the product as distinct_from, so the product leaves the pairs of one edge out and no
// filter runs after it. The oracle of each case is the same patterns in two MATCH clauses,
// which the rule does not reach, with the pairs written out as a WHERE.
class FuseProductDistinctEdgesTest : public CallV3Test {
protected:
    void dump(std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "db") {
                program = row.back();
            }
        }
    }

    void sortedRows(std::string_view query, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink);

        rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
    }

    void expectFolded(std::string_view query, std::string_view oracle) {
        std::string program;
        dump(query, program);

        EXPECT_TRUE(contains(program, "db.cross_product")) << program;
        EXPECT_TRUE(contains(program, "distinct_from [")) << program;
        EXPECT_FALSE(contains(program, "db.check_edge_distinct")) << program;

        Rows rows;
        sortedRows(query, rows);

        Rows expected;
        sortedRows(oracle, expected);

        EXPECT_EQ(rows, expected) << query;
    }

    static bool contains(std::string_view text, std::string_view part) {
        return text.find(part) != std::string_view::npos;
    }
};

TEST_F(FuseProductDistinctEdgesTest, foldsIntoTheProductOfTwoPatterns) {
    expectFolded("MATCH (a)-[e1]->(b), (c)-[e2]->(d) RETURN count(*)",
                 "MATCH (a)-[e1]->(b) MATCH (c)-[e2]->(d) WHERE e1 <> e2 RETURN count(*)");
}

TEST_F(FuseProductDistinctEdgesTest, keepsTheRowsTheFilterKept) {
    expectFolded("MATCH (a)-[e1]->(b), (c)-[e2]->(d) RETURN a.name, b.name, c.name, d.name",
                 "MATCH (a)-[e1]->(b) MATCH (c)-[e2]->(d) WHERE e1 <> e2 RETURN a.name, b.name, c.name, d.name");
}

TEST_F(FuseProductDistinctEdgesTest, foldsIntoTheProductOfTypedPatterns) {
    expectFolded("MATCH (a)-[e1:KNOWS_WELL]->(b), (c)-[e2:KNOWS_WELL]->(d) RETURN count(*)",
                 "MATCH (a)-[e1:KNOWS_WELL]->(b) MATCH (c)-[e2:KNOWS_WELL]->(d) WHERE e1 <> e2 RETURN count(*)");
}

TEST_F(FuseProductDistinctEdgesTest, foldsAcrossAProductWithANodePattern) {
    expectFolded("MATCH (a)-[e1]->(b), (c)-[e2]->(d), (x) RETURN count(*)",
                 "MATCH (a)-[e1]->(b) MATCH (c)-[e2]->(d) MATCH (x) WHERE e1 <> e2 RETURN count(*)");
}

TEST_F(FuseProductDistinctEdgesTest, foldsBothPairsOfAChainAgainstAHop) {
    expectFolded("MATCH (a)-[e1]->(b)-[e2]->(c), (d)-[e3]->(e) RETURN count(*)",
                 "MATCH (a)-[e1]->(b)-[e2]->(c) MATCH (d)-[e3]->(e) WHERE e3 <> e1 AND e3 <> e2 RETURN count(*)");
}

TEST_F(FuseProductDistinctEdgesTest, keepsTheRowsUnderALimit) {
    std::string program;
    dump("MATCH (a)-[e1]->(b), (c)-[e2]->(d) RETURN a, c LIMIT 20", program);
    EXPECT_FALSE(contains(program, "db.check_edge_distinct")) << program;

    StringRowSink sink;
    runQuery("MATCH (a)-[e1]->(b), (c)-[e2]->(d) RETURN a, c LIMIT 20", sink);
    EXPECT_EQ(sink.getRows().size(), 20u);
}

// A path's edges are not a column of edge IDs the product can compare, so a check against
// a walk stays a filter over the product
TEST_F(FuseProductDistinctEdgesTest, keepsTheCheckAgainstAWalk) {
    std::string program;
    dump("MATCH (a)-[e*1..2]->(b), (c)-[f]->(d) RETURN count(*)", program);

    EXPECT_TRUE(contains(program, "db.check_edge_distinct")) << program;
}
