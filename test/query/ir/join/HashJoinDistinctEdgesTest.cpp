#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "HashJoinQueryTest.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

// The check between the edges of two patterns a hash join brings together folds into the
// join as distinct_from, so the probe leaves the pairs of one edge out and no filter runs
// after it. The oracle of each case is the same patterns in two MATCH clauses, which the
// rule does not reach, with the pairs written out in the WHERE.
class HashJoinDistinctEdgesTest : public HashJoinQueryTest {
protected:
    void sortedRows(std::string_view query, bool forcesJoin, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink, forcesJoin);

        rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
    }

    void expectFolded(std::string_view query, std::string_view oracle) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, "db.hash_join")) << program;
        EXPECT_TRUE(contains(program, "distinct_from [")) << program;
        EXPECT_FALSE(contains(program, "db.check_edge_distinct")) << program;

        std::string lowered;
        nlProgram(query, lowered);
        EXPECT_TRUE(contains(lowered, "nl.hash_join_probe")) << lowered;
        EXPECT_FALSE(contains(lowered, "nl.check_edge_distinct")) << lowered;

        Rows rows;
        sortedRows(query, true, rows);

        Rows expected;
        sortedRows(oracle, false, expected);

        EXPECT_FALSE(expected.empty()) << oracle;
        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(HashJoinDistinctEdgesTest, foldsIntoAJoinOnAProperty) {
    expectFolded("MATCH (a)-[e1]->(b), (c)-[e2]->(d) WHERE b.name = d.name RETURN count(*)",
                 "MATCH (a)-[e1]->(b) MATCH (c)-[e2]->(d) WHERE b.name = d.name AND e1 <> e2 RETURN count(*)");
}

TEST_F(HashJoinDistinctEdgesTest, keepsTheRowsTheFilterKept) {
    expectFolded("MATCH (a)-[e1]->(b), (c)-[e2]->(d) WHERE b.name = d.name RETURN a.name, b.name, c.name",
                 "MATCH (a)-[e1]->(b) MATCH (c)-[e2]->(d) WHERE b.name = d.name AND e1 <> e2 RETURN a.name, b.name, c.name");
}

TEST_F(HashJoinDistinctEdgesTest, foldsBothPairsOfAChainAgainstAHop) {
    expectFolded("MATCH (a)-[e1]->(b)-[e2]->(c), (d)-[e3]->(f) WHERE c.name = f.name RETURN count(*)",
                 "MATCH (a)-[e1]->(b)-[e2]->(c) MATCH (d)-[e3]->(f) WHERE c.name = f.name AND e3 <> e1 AND e3 <> e2 RETURN count(*)");
}
