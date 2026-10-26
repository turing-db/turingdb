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

// size() and count() of a variable-length relationship, a group variable, or the nodes or
// relationships of a named walk read a list, which codegen expands. The db passes answer
// them off the walk's handles instead, inside an OPTIONAL MATCH too: the size is the number
// of repetitions the walk took, or its nodes, and a count tallies the walk's rows.
class FusePathLengthTest : public CallV3Test {
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

TEST_F(FusePathLengthTest, codegenSizesTheRelationshipsOfAWalk) {
    std::string dump;
    stageDump("EXPLAIN (codegen) MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN size(e)", "codegen", dump);

    EXPECT_TRUE(contains(dump, "db.expand_path")) << dump;
    EXPECT_TRUE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.path_length")) << dump;
}

TEST_F(FusePathLengthTest, readsTheSizeOfTheRelationshipsOffTheHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN size(e)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, codegenSizesAGroupOfABodyOfSeveralSteps) {
    std::string dump;
    stageDump("EXPLAIN (codegen) MATCH (n) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,2} (c) RETURN size(y)", "codegen", dump);

    EXPECT_TRUE(contains(dump, "db.expand_path")) << dump;
    EXPECT_TRUE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.path_length")) << dump;
}

TEST_F(FusePathLengthTest, readsTheSizeOfAGroupOfABodyOfSeveralStepsOffTheHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,2} (c) RETURN size(y)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_TRUE(contains(dump, "steps 2")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, keepsTheListReturnedBesideItsSize) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN e, size(e)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_TRUE(contains(dump, "db.expand_path")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
}

TEST_F(FusePathLengthTest, readsTheSizeOfTheNodesAndRelationshipsOfANamedWalkOffTheHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->+(m:Person) RETURN size(nodes(p)), size(relationships(p))", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length(%3) nodes :")) << dump;
    EXPECT_TRUE(contains(dump, "db.path_length(%3) :")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.to_nullable")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, readsTheSizeOfAnOptionalWalkOffTheHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name, size(e)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, readsTheSizeOfAnOptionalNamedWalkOffTheHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH p = (n)-[:KNOWS_WELL*1..2]->(c) RETURN n.name, size(nodes(p)), size(relationships(p))", "db", dump);

    EXPECT_TRUE(contains(dump, ") nodes :")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.path_elements")) << dump;
    EXPECT_FALSE(contains(dump, "db.make_path")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, readsTheSizeOfOptionalGroupsOffTheHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(x), size(y)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, keepsAnOptionalListReturnedBesideItsSize) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name, e, size(e)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_EQ(countOf(dump, "db.expand_path"), 1u) << dump;
    EXPECT_FALSE(contains(dump, "db.size")) << dump;
}

TEST_F(FusePathLengthTest, codegenCountsTheRelationshipsOfAWalk) {
    std::string dump;
    stageDump("EXPLAIN (codegen) MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN count(e)", "codegen", dump);

    EXPECT_TRUE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, countsAWalkPerKeyOffItsHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN n.name, count(e)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.group_aggregate")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, countsAWalkBesideACollectOffItsHandles) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN n.name, count(e), collect(c.name)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.collect")) << dump;
    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, expandsTheWalksADistinctCountCompares) {
    std::string dump;
    stageDump("EXPLAIN (db) MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN count(DISTINCT e)", "db", dump);

    EXPECT_TRUE(contains(dump, "db.expand_path")) << dump;
}

TEST_F(FusePathLengthTest, sizesTheRelationshipsOfAWalk) {
    expectSortedRows("MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN n.name, size(e)",
                     {{"Remy", "1"},
                      {"Remy", "2"},
                      {"Adam", "1"},
                      {"Adam", "2"},
                      {"Ghosts", "1"},
                      {"Ghosts", "2"},
                      {"Ghosts", "3"}});
}

TEST_F(FusePathLengthTest, sizesEachGroupOfABodyOfSeveralSteps) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,2} (c) RETURN n.name, size(x), size(y), size(z)",
                     {{"Remy", "1", "1", "1"},
                      {"Adam", "1", "1", "1"},
                      {"Ghosts", "1", "1", "1"}});
}

TEST_F(FusePathLengthTest, sizesAGroupAsTheRepetitionsOfItsBody) {
    expectSortedRows("MATCH (n) ((x)-->(y)-->(z)){1,2} (c) RETURN size(y) AS s, count(*)",
                     {{"1", "12"},
                      {"2", "10"}});
}

TEST_F(FusePathLengthTest, sizesAGroupOfABodyOfSeveralStepsInAWhere) {
    expectSortedRows("MATCH (n) ((x)-->(y)-->(z)){1,2} (c) WHERE size(z) = 2 RETURN n.name, count(*)",
                     {{"Remy", "4"},
                      {"Adam", "3"},
                      {"Ghosts", "3"}});
}

TEST_F(FusePathLengthTest, sizesTheRelationshipsOfAWalkPastAWith) {
    expectSortedRows("MATCH (n)-[e:KNOWS_WELL*1..3]->(c) WITH n, e RETURN n.name, size(e)",
                     {{"Remy", "1"},
                      {"Remy", "2"},
                      {"Adam", "1"},
                      {"Adam", "2"},
                      {"Ghosts", "1"},
                      {"Ghosts", "2"},
                      {"Ghosts", "3"}});
}

TEST_F(FusePathLengthTest, sizesAGroupOfABodyOfSeveralStepsPastAWith) {
    expectSortedRows("MATCH (n) ((x)-->(y)-->(z)){1,2} (c) WITH y RETURN size(y) AS s, count(*)",
                     {{"1", "12"},
                      {"2", "10"}});
}

TEST_F(FusePathLengthTest, countsTheRelationshipsOfAWalkPastAWith) {
    expectSortedRows("MATCH (n)-[e:KNOWS_WELL*1..3]->(c) WITH n, e RETURN n.name, count(e), collect(size(e))[0] > 0",
                     {{"Remy", "2", "true"},
                      {"Adam", "2", "true"},
                      {"Ghosts", "3", "true"}});
}

TEST_F(FusePathLengthTest, countsTheRelationshipsOfAWalk) {
    expectSortedRows("MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN count(e)", {{"7"}});
}

TEST_F(FusePathLengthTest, countsTheRelationshipsOfAWalkPerKey) {
    expectSortedRows("MATCH (n)-[e:KNOWS_WELL*1..3]->(c) RETURN n.name, count(e), size(collect(c.name))",
                     {{"Remy", "2", "2"},
                      {"Adam", "2", "2"},
                      {"Ghosts", "3", "3"}});
}

TEST_F(FusePathLengthTest, countsAGroupOfABodyOfSeveralSteps) {
    expectSortedRows("MATCH (n) ((x)-->(y)-->(z)){1,2} (c) RETURN count(y), count(DISTINCT y)",
                     {{"22", "6"}});
}

TEST_F(FusePathLengthTest, sizesAnOptionalWalkAsNullWhereItMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..1]->(c) RETURN n.name, size(e)",
                     {{"Remy", "1"},
                      {"Adam", "1"},
                      {"Maxime", "null"},
                      {"Luc", "null"},
                      {"Martina", "null"},
                      {"Suhas", "null"},
                      {"Cyrus", "null"},
                      {"Doruk", "null"}});
}

TEST_F(FusePathLengthTest, countsAnOptionalWalkWhereItMatched) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..1]->(c) RETURN count(e), count(*)",
                     {{"2", "8"}});
}

TEST_F(FusePathLengthTest, sizesTheNodesAndRelationshipsOfANamedWalk) {
    expectSortedRows("MATCH p = (n:Person)-[e]->+(m:Person) RETURN n.name, m.name, size(nodes(p)), size(relationships(p)), length(p)",
                     {{"Remy", "Adam", "2", "1", "1"},
                      {"Remy", "Remy", "3", "2", "2"},
                      {"Remy", "Remy", "3", "2", "2"},
                      {"Remy", "Adam", "4", "3", "3"},
                      {"Remy", "Remy", "5", "4", "4"},
                      {"Remy", "Remy", "5", "4", "4"},
                      {"Adam", "Remy", "2", "1", "1"},
                      {"Adam", "Adam", "3", "2", "2"},
                      {"Adam", "Remy", "4", "3", "3"},
                      {"Adam", "Adam", "5", "4", "4"}});
}

TEST_F(FusePathLengthTest, sizesTheNodesOfANamedWalkThatStayedPut) {
    expectSortedRows("MATCH p = (n:Person)-[:KNOWS_WELL*0..1]->(m) RETURN n.name, m.name, size(nodes(p)), size(relationships(p))",
                     {{"Remy", "Remy", "1", "0"},
                      {"Remy", "Adam", "2", "1"},
                      {"Adam", "Adam", "1", "0"},
                      {"Adam", "Remy", "2", "1"},
                      {"Maxime", "Maxime", "1", "0"},
                      {"Luc", "Luc", "1", "0"},
                      {"Martina", "Martina", "1", "0"},
                      {"Suhas", "Suhas", "1", "0"},
                      {"Cyrus", "Cyrus", "1", "0"},
                      {"Doruk", "Doruk", "1", "0"}});
}

TEST_F(FusePathLengthTest, sizesAnOptionalWalkOfSeveralHopsAsNullWhereItMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name, c.name, size(e)",
                     {{"Remy", "Adam", "1"},
                      {"Remy", "Remy", "2"},
                      {"Adam", "Remy", "1"},
                      {"Adam", "Adam", "2"},
                      {"Maxime", "null", "null"},
                      {"Luc", "null", "null"},
                      {"Martina", "null", "null"},
                      {"Suhas", "null", "null"},
                      {"Cyrus", "null", "null"},
                      {"Doruk", "null", "null"}});
}

TEST_F(FusePathLengthTest, sizesAnOptionalNamedWalkAsNullWhereItMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH p = (n)-[:KNOWS_WELL*1..2]->(c) "
                     "RETURN n.name, c.name, size(nodes(p)), size(relationships(p)), length(p)",
                     {{"Remy", "Adam", "2", "1", "1"},
                      {"Remy", "Remy", "3", "2", "2"},
                      {"Adam", "Remy", "2", "1", "1"},
                      {"Adam", "Adam", "3", "2", "2"},
                      {"Maxime", "null", "null", "null", "null"},
                      {"Luc", "null", "null", "null", "null"},
                      {"Martina", "null", "null", "null", "null"},
                      {"Suhas", "null", "null", "null", "null"},
                      {"Cyrus", "null", "null", "null", "null"},
                      {"Doruk", "null", "null", "null", "null"}});
}

TEST_F(FusePathLengthTest, sizesOptionalGroupsAsNullWhereTheyMissed) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(x), size(y)",
                     {{"Remy", "1", "1"},
                      {"Remy", "2", "2"},
                      {"Adam", "1", "1"},
                      {"Adam", "2", "2"},
                      {"Maxime", "null", "null"},
                      {"Luc", "null", "null"},
                      {"Martina", "null", "null"},
                      {"Suhas", "null", "null"},
                      {"Cyrus", "null", "null"},
                      {"Doruk", "null", "null"}});
}

TEST_F(FusePathLengthTest, sizesAnOptionalWalkReturnedBesideItsSize) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL*1..2]->(c) RETURN n.name, e, size(e)",
                     {{"Remy", "0", "1"},
                      {"Remy", "0, 4", "2"},
                      {"Adam", "4", "1"},
                      {"Adam", "4, 0", "2"},
                      {"Maxime", "null", "null"},
                      {"Luc", "null", "null"},
                      {"Martina", "null", "null"},
                      {"Suhas", "null", "null"},
                      {"Cyrus", "null", "null"},
                      {"Doruk", "null", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
