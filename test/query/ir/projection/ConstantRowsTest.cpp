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

// An op reading constants alone runs over a single row, so codegen lays every constant an
// op reads alone over the rows in flight with db.broadcast_constant: the rows a query emits
// follow the use-def chains a pass reads
class ConstantRowsTest : public CallV3Test {
protected:
    void expectRowCount(std::string_view query, size_t expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows().size(), expected) << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void programOf(std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }
};

TEST_F(ConstantRowsTest, returnsOneRowPerPath) {
    expectRowCount("MATCH (a:Person)-[e*1..2]->(b) RETURN 1", 25);
    expectRowCount("MATCH (a:Person)-[*1..2]->(b) RETURN null", 25);
    expectRowCount("MATCH (a:Person {name: 'Remy'})-[*2..2]->(b) RETURN 1", 4);
    expectRowCount("MATCH p = (a:Person {name: 'Remy'})-[*2..2]->(b) "
                   "RETURN toStringOrNull(relationships(p)), toStringOrNull(nodes(p))",
                   4);
}

TEST_F(ConstantRowsTest, returnsOneRowPerPathPastAHopOrACut) {
    expectRowCount("MATCH (a:Person)-[e*1..2]->(b)-->(c) RETURN 1", 20);
    expectRowCount("MATCH (a:Person)-[e*1..2]->(b) WHERE b:Person RETURN 1", 5);
    expectRowCount("MATCH (a:Person)-[e*1..2]->(b) RETURN 1 LIMIT 100", 25);
    expectRowCount("MATCH (a:Person)-[e*1..2]->(b) RETURN 1 SKIP 1", 24);
    expectRowCount("MATCH (a:Person)-[e*1..2]->(b) RETURN DISTINCT 1", 1);
}

TEST_F(ConstantRowsTest, readsTheExplorationThroughTheBroadcast) {
    std::string program;
    programOf("MATCH (a:Person {name: 'Remy'})-[*2..2]->(b) RETURN 1", program);

    EXPECT_TRUE(contains(program, "db.broadcast_constant")) << program;
    EXPECT_FALSE(contains(program, " distinct")) << program;
}

TEST_F(ConstantRowsTest, laysOutTheConstantsAfterAnAggregateOverItsRow) {
    expectRows("MATCH (n:Person) RETURN count(*) * (size('ab') - 1) AS c", {{"8"}});
}

TEST_F(ConstantRowsTest, unwindsAConstantListForEveryMatchedRow) {
    expectRows("MATCH (n:Person) WHERE n.age = 32 UNWIND [1, 2] AS x RETURN x", {{"1"}, {"1"}, {"2"}, {"2"}});
}

TEST_F(ConstantRowsTest, runsAConstantsAloneProjectionOverASingleRow) {
    expectRows("RETURN 1", {{"1"}});
    expectRows("MATCH (n:Nothing) RETURN 1 / 0", {});
}
