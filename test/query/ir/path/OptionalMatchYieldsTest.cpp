#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
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

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

size_t countOf(std::string_view text, std::string_view part) {
    size_t count = 0;
    for (size_t position = text.find(part); position != std::string_view::npos; position = text.find(part, position + 1)) {
        count++;
    }

    return count;
}

}

// An OPTIONAL MATCH yields each variable its pattern binds once, and a variable nothing
// reads past the match is not padded and carried at all
class OptionalMatchYieldsTest : public CallV3Test {
protected:
    void stageDump(std::string_view query, std::string_view stage, std::string& dump) {
        StringRowSink sink;
        runQuery(query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                dump = row.back();
            }
        }

        ASSERT_FALSE(dump.empty()) << "no " << stage << " stage for: " << query;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sorted(expected)) << "query: " << query;
    }
};

TEST_F(OptionalMatchYieldsTest, codegenYieldsARelationshipOnce) {
    std::string dump;
    stageDump("EXPLAIN (codegen) MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL]->(c) RETURN n.name", "codegen", dump);

    EXPECT_TRUE(contains(dump, ":3 = db.optional_match(")) << dump;
}

TEST_F(OptionalMatchYieldsTest, codegenExpandsAWalkOnce) {
    std::string dump;
    stageDump("EXPLAIN (codegen) MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..1]->(c) RETURN n.name", "codegen", dump);

    EXPECT_EQ(countOf(dump, "db.expand_path"), 1u) << dump;
}

TEST_F(OptionalMatchYieldsTest, codegenBuildsANamedWalkOnce) {
    std::string dump;
    stageDump("EXPLAIN (codegen) MATCH (n:Person) OPTIONAL MATCH p = (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name", "codegen", dump);

    EXPECT_EQ(countOf(dump, "db.make_path"), 1u) << dump;
    EXPECT_EQ(countOf(dump, "db.expand_path"), 1u) << dump;
}

TEST_F(OptionalMatchYieldsTest, codegenExpandsEachGroupOnce) {
    std::string dump;
    stageDump("EXPLAIN (codegen) MATCH (n:Person) OPTIONAL MATCH (n) ((x)-[r:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name", "codegen", dump);

    EXPECT_EQ(countOf(dump, "kind sources"), 1u) << dump;
    EXPECT_EQ(countOf(dump, "kind edges"), 1u) << dump;
    EXPECT_EQ(countOf(dump, "kind ends"), 1u) << dump;
}

TEST_F(OptionalMatchYieldsTest, carriesNoUnreadRelationship) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL]->(c) RETURN n.name", "db", dump);

    EXPECT_TRUE(contains(dump, "} : {!db.column<!storage.node_id>}")) << dump;
}

TEST_F(OptionalMatchYieldsTest, expandsNoUnreadWalk) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..1]->(c) RETURN n.name", "db", dump);

    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(OptionalMatchYieldsTest, buildsNoUnreadNamedWalk) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH p = (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name", "db", dump);

    EXPECT_FALSE(contains(dump, "db.make_path")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(OptionalMatchYieldsTest, expandsNoUnreadGroup) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH (n) ((x)-[r:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name", "db", dump);

    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(OptionalMatchYieldsTest, expandsTheWalkItReturnsOnce) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name, e", "db", dump);

    EXPECT_EQ(countOf(dump, "db.expand_path"), 1u) << dump;
}

TEST_F(OptionalMatchYieldsTest, keepsEveryRowItDropsNoColumnOf) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name",
                     {{"Remy"}, {"Remy"}, {"Adam"}, {"Adam"},
                      {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(OptionalMatchYieldsTest, returnsARelationshipAsNullWhereItMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL]->(c) RETURN n.name, e",
                     {{"Remy", "0"},
                      {"Adam", "4"},
                      {"Maxime", "null"},
                      {"Luc", "null"},
                      {"Martina", "null"},
                      {"Suhas", "null"},
                      {"Cyrus", "null"},
                      {"Doruk", "null"}});
}

TEST_F(OptionalMatchYieldsTest, returnsAWalkAsNullWhereItMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name, e",
                     {{"Remy", "0"},
                      {"Remy", "0, 4"},
                      {"Adam", "4"},
                      {"Adam", "4, 0"},
                      {"Maxime", "null"},
                      {"Luc", "null"},
                      {"Martina", "null"},
                      {"Suhas", "null"},
                      {"Cyrus", "null"},
                      {"Doruk", "null"}});
}

TEST_F(OptionalMatchYieldsTest, returnsTheNodesOfANamedWalkAsNullWhereItMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name, nodes(p)",
                     {{"Remy", "0, 1"},
                      {"Remy", "0, 1, 0"},
                      {"Adam", "1, 0"},
                      {"Adam", "1, 0, 1"},
                      {"Maxime", "null"},
                      {"Luc", "null"},
                      {"Martina", "null"},
                      {"Suhas", "null"},
                      {"Cyrus", "null"},
                      {"Doruk", "null"}});
}

TEST_F(OptionalMatchYieldsTest, returnsGroupsAsNullWhereTheyMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n) ((x)-[r:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, x, r, y",
                     {{"Remy", "0", "0", "1"},
                      {"Remy", "0, 1", "0, 4", "1, 0"},
                      {"Adam", "1", "4", "0"},
                      {"Adam", "1, 0", "4, 0", "0, 1"},
                      {"Maxime", "null", "null", "null"},
                      {"Luc", "null", "null", "null"},
                      {"Martina", "null", "null", "null"},
                      {"Suhas", "null", "null", "null"},
                      {"Cyrus", "null", "null", "null"},
                      {"Doruk", "null", "null", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
