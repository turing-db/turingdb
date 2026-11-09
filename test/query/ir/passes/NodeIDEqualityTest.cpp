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

class NodeIDEqualityTest : public CallV3Test {
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
};

TEST_F(NodeIDEqualityTest, fetchesEachUnwoundNodeID) {
    const std::string_view query = "UNWIND [1, 1, 3] AS i MATCH (n) WHERE id(n) = i RETURN n.name";

    std::string program;
    dbProgramOf(query, program);
    EXPECT_TRUE(contains(program, "db.fetch_nodes")) << program;
    EXPECT_FALSE(contains(program, "db.scan_nodes()")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    expectRows(query, {{"Adam"}, {"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDEqualityTest, comparesAnIDToADoubleByValue) {
    expectRows("MATCH (n) WHERE id(n) = 2.0 RETURN n.name", {{"Computers"}});
    expectRows("UNWIND [2.0, 2.5] AS i MATCH (n) WHERE id(n) = i RETURN n.name", {{"Computers"}});
}

TEST_F(NodeIDEqualityTest, joinsANodeToAnIntegerProperty) {
    runWrite("CREATE (:Key {v: 1}), (:Key {v: 3}), (:Key {v: 99})");

    expectRows("MATCH (k:Key), (n) WHERE id(n) = k.v RETURN n.name", {{"Adam"}, {"Eighties"}});
    expectRows("MATCH (k:Key), (n) WHERE k.v = id(n) RETURN n.name", {{"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDEqualityTest, endsAWalkOnEachUnwoundNodeID) {
    expectRows("UNWIND [1, 1] AS x MATCH (a:Person)-[:KNOWS_WELL*1..2]->(b) WHERE id(b) = x RETURN a.name, x",
               {{"Adam", "1"}, {"Adam", "1"}, {"Remy", "1"}, {"Remy", "1"}});
}

TEST_F(NodeIDEqualityTest, readsTheIDBesideTheNode) {
    expectRows("UNWIND [1, 1] AS i MATCH (n) WHERE id(n) = i RETURN i, id(n), n.name",
               {{"1", "1", "Adam"}, {"1", "1", "Adam"}});
}

TEST_F(NodeIDEqualityTest, endsAWalkOnACarriedNodeID) {
    runWrite("MATCH (n:Person {name: 'Remy'}) SET n.target = 1");

    expectRows("MATCH (a:Person) WITH a, a.target AS x MATCH (a)-[:KNOWS_WELL*1..2]->(b) WHERE id(b) = x RETURN a.name",
               {{"Remy"}});
    expectRows("MATCH (a:Person) WITH a, a.target AS x MATCH (a)-[:KNOWS_WELL*1..2]->(b) WHERE b = x RETURN a.name",
               {{"Remy"}});
}
