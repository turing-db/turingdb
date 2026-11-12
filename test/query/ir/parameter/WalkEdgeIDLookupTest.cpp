#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

}

class WalkEdgeIDLookupTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void dbProgramOf(std::string_view query, const ParameterMap& parameters, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), parameters, sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    void expectNoEdgeScan(std::string_view query, const std::string& program) {
        EXPECT_TRUE(contains(program, "db.const_scan_edges")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_edges")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_out_edges")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_in_edges")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << query << "\n" << program;
    }

    void expectSeededPlan(std::string_view query, const ParameterMap& parameters) {
        std::string program;
        dbProgramOf(query, parameters, program);

        expectNoEdgeScan(query, program);
        EXPECT_FALSE(contains(program, "db.scan_nodes(")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_nodes_by_label(")) << query << "\n" << program;
    }

    void expectSeededRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        expectSeededPlan(query, parameters);
        expectRows(query, parameters, expected);
    }

    void expectEndedRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        std::string program;
        dbProgramOf(query, parameters, program);

        expectNoEdgeScan(query, program);
        EXPECT_TRUE(contains(program, "end_nodes")) << query << "\n" << program;

        expectRows(query, parameters, expected);
    }

    static void setIntegers(ParameterMap& parameters, std::string_view name, const std::vector<int64_t>& values) {
        ParameterValue::List& list = parameters.insert(std::string(name)).setList();
        for (const int64_t value : values) {
            list.emplace_back().setInt64(value);
        }
    }
};

TEST_F(WalkEdgeIDLookupTest, seedsTheWalkAtTheEdgeALiteralIDNames) {
    const ParameterMap parameters;

    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = 3 RETURN a.name, b.name", parameters, {{"Remy", "Eighties"}});
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = 0 RETURN a.name, b.name, rs",
                     parameters,
                     {{"Remy", "Adam", "0"}, {"Remy", "Bio", "0, 5"}, {"Remy", "Cooking", "0, 6"}, {"Remy", "Remy", "0, 4"}});
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(head(rs)) = 0 RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Adam"}, {"Remy", "Bio"}, {"Remy", "Cooking"}, {"Remy", "Remy"}});
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE rs[0] = 0 RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Adam"}, {"Remy", "Bio"}, {"Remy", "Cooking"}, {"Remy", "Remy"}});
}

TEST_F(WalkEdgeIDLookupTest, seedsTheWalkAtTheEdgeAParameterNames) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(0);

    const Rows expected {{"Remy", "Adam"}, {"Remy", "Bio"}, {"Remy", "Cooking"}, {"Remy", "Remy"}};

    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = $id RETURN a.name, b.name", parameters, expected);
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE rs[0] = $id RETURN a.name, b.name", parameters, expected);
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = $id + 4 RETURN a.name, b.name, rs",
                     parameters,
                     {{"Adam", "Adam", "4, 0"},
                      {"Adam", "Computers", "4, 2"},
                      {"Adam", "Eighties", "4, 3"},
                      {"Adam", "Ghosts", "4, 1"},
                      {"Adam", "Remy", "4"}});
}

TEST_F(WalkEdgeIDLookupTest, seedsTheWalkAtTheEdgesOfAList) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {1, 3, 99});

    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) IN [0, 4] RETURN a.name, b.name, rs",
                     parameters,
                     {{"Adam", "Adam", "4, 0"},
                      {"Adam", "Computers", "4, 2"},
                      {"Adam", "Eighties", "4, 3"},
                      {"Adam", "Ghosts", "4, 1"},
                      {"Adam", "Remy", "4"},
                      {"Remy", "Adam", "0"},
                      {"Remy", "Bio", "0, 5"},
                      {"Remy", "Cooking", "0, 6"},
                      {"Remy", "Remy", "0, 4"}});
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) IN $ids RETURN a.name, b.name, rs",
                     parameters,
                     {{"Remy", "Eighties", "3"}, {"Remy", "Ghosts", "1"}, {"Remy", "Remy", "1, 7"}});
}

TEST_F(WalkEdgeIDLookupTest, foldsALiteralListIntoTheSeeds) {
    const ParameterMap parameters;

    expectSeededRows("UNWIND [3, 0] AS i MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = i RETURN a.name, b.name, rs",
                     parameters,
                     {{"Remy", "Adam", "0"}, {"Remy", "Bio", "0, 5"}, {"Remy", "Cooking", "0, 6"}, {"Remy", "Eighties", "3"}, {"Remy", "Remy", "0, 4"}});
}

TEST_F(WalkEdgeIDLookupTest, keepsTheWalkATrailPastTheListedEdge) {
    const ParameterMap parameters;

    expectSeededRows("MATCH (a)-[rs*1..3]->(b) WHERE id(rs[0]) = 0 RETURN a.name, b.name, rs",
                     parameters,
                     {{"Remy", "Adam", "0"},
                      {"Remy", "Bio", "0, 5"},
                      {"Remy", "Computers", "0, 4, 2"},
                      {"Remy", "Cooking", "0, 6"},
                      {"Remy", "Eighties", "0, 4, 3"},
                      {"Remy", "Ghosts", "0, 4, 1"},
                      {"Remy", "Remy", "0, 4"}});
    expectSeededRows("MATCH (a)-[rs*1..3]->(b) WHERE id(rs[0]) = 4 RETURN a.name, b.name, rs",
                     parameters,
                     {{"Adam", "Adam", "4, 0"},
                      {"Adam", "Bio", "4, 0, 5"},
                      {"Adam", "Computers", "4, 2"},
                      {"Adam", "Cooking", "4, 0, 6"},
                      {"Adam", "Eighties", "4, 3"},
                      {"Adam", "Ghosts", "4, 1"},
                      {"Adam", "Remy", "4"},
                      {"Adam", "Remy", "4, 1, 7"}});
}

TEST_F(WalkEdgeIDLookupTest, seedsEachBoundOfTheWalk) {
    const ParameterMap parameters;

    expectSeededRows("MATCH (a)-[rs*1..1]->(b) WHERE id(rs[0]) = 0 RETURN a.name, b.name, rs", parameters, {{"Remy", "Adam", "0"}});
    expectSeededRows("MATCH (a)-[rs*0..2]->(b) WHERE id(rs[0]) = 0 RETURN a.name, b.name, rs",
                     parameters,
                     {{"Remy", "Adam", "0"}, {"Remy", "Bio", "0, 5"}, {"Remy", "Cooking", "0, 6"}, {"Remy", "Remy", "0, 4"}});
}

TEST_F(WalkEdgeIDLookupTest, seedsTheWalkOfEitherDirection) {
    const ParameterMap parameters;

    expectSeededRows("MATCH (a)<-[rs*1..2]-(b) WHERE id(rs[0]) = 4 RETURN a.name, b.name, rs",
                     parameters,
                     {{"Remy", "Adam", "4"}, {"Remy", "Remy", "4, 0"}});
    expectSeededRows("MATCH (a)-[rs*1..2]-(b) WHERE id(rs[0]) = 3 RETURN a.name, b.name",
                     parameters,
                     {{"Eighties", "Adam"},
                      {"Eighties", "Adam"},
                      {"Eighties", "Computers"},
                      {"Eighties", "Ghosts"},
                      {"Eighties", "Ghosts"},
                      {"Eighties", "Remy"},
                      {"Remy", "Eighties"}});
}

TEST_F(WalkEdgeIDLookupTest, seedsEachNodeOnce) {
    const ParameterMap parameters;

    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) IN [1, 3] RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Eighties"}, {"Remy", "Ghosts"}, {"Remy", "Remy"}});
    expectSeededRows("MATCH (a)-[rs*1..1]-(b) WHERE id(rs[0]) IN [0, 4] RETURN a.name, b.name",
                     parameters,
                     {{"Adam", "Remy"}, {"Adam", "Remy"}, {"Remy", "Adam"}, {"Remy", "Adam"}});
}

TEST_F(WalkEdgeIDLookupTest, walksASelfLoopAsTheWalkDoes) {
    const ParameterMap parameters;

    runWrite("CREATE (x:Loop {name: 'x'}) CREATE (x)-[:SELF]->(x)");

    expectSeededRows("MATCH (a)-[rs*1..2]-(b) WHERE id(rs[0]) = 18 RETURN a.name, b.name, rs",
                     parameters,
                     {{"x", "x", "18"}, {"x", "x", "18"}});
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = 18 RETURN a.name, b.name, rs", parameters, {{"x", "x", "18"}});
    expectEndedRows("MATCH (a)-[rs*1..2]-(b) WHERE id(last(rs)) = 18 RETURN a.name, b.name, rs",
                    parameters,
                    {{"x", "x", "18"}, {"x", "x", "18"}});
}

TEST_F(WalkEdgeIDLookupTest, seedsTheWalkAtAnEdgeTheQueryCreated) {
    const ParameterMap parameters;
    const std::string_view query = "CREATE (:Probe {name: 'p'})-[:PROBE]->(:Probe {name: 'q'}) WITH 1 AS one "
                                   "MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = 18 RETURN a.name, b.name";

    expectSeededPlan(query, parameters);

    StringRowSink sink;
    runWrite(query, parameters, sink);

    const Rows expected {{"p", "q"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(WalkEdgeIDLookupTest, keepsTheEdgeTypesAndLabelsOfTheWalk) {
    const ParameterMap parameters;

    expectSeededRows("MATCH (a)-[rs:KNOWS_WELL*1..2]->(b) WHERE id(rs[0]) = 0 RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Adam"}, {"Remy", "Remy"}});
    expectSeededRows("MATCH (a)-[rs:KNOWS_WELL*1..2]->(b) WHERE id(rs[0]) = 3 RETURN a.name, b.name", parameters, {});
    expectSeededRows("MATCH (a:Interest)-[rs*1..2]->(b) WHERE id(rs[0]) = 7 RETURN a.name, b.name",
                     parameters,
                     {{"Ghosts", "Adam"}, {"Ghosts", "Computers"}, {"Ghosts", "Eighties"}, {"Ghosts", "Ghosts"}, {"Ghosts", "Remy"}});
    expectSeededRows("MATCH (a:Person)-[rs*1..2]->(b) WHERE id(rs[0]) = 7 RETURN a.name, b.name", parameters, {});
}

TEST_F(WalkEdgeIDLookupTest, seedsNoWalkForAMissingID) {
    ParameterMap parameters;
    parameters.insert("missing").setInt64(99);

    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = 99 RETURN a.name, b.name", parameters, {});
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = $missing RETURN a.name, b.name", parameters, {});
    expectSeededRows("MATCH (a)-[rs*1..2]->(b) WHERE id(rs[0]) = -1 RETURN a.name, b.name", parameters, {});
}

TEST_F(WalkEdgeIDLookupTest, buildsThePathOfTheSeededWalk) {
    const ParameterMap parameters;

    expectSeededRows("MATCH p = (a)-[rs*1..2]->(b) WHERE id(rs[0]) = 0 RETURN p",
                     parameters,
                     {{"(0), [0], (1)"}, {"(0), [0], (1), [4], (0)"}, {"(0), [0], (1), [5], (4)"}, {"(0), [0], (1), [6], (5)"}});
}

TEST_F(WalkEdgeIDLookupTest, walksThePatternOutFromTheSeededWalk) {
    const ParameterMap parameters;

    expectSeededRows("MATCH (x)-[r]->(a)-[rs*1..2]->(b) WHERE id(rs[0]) = 3 RETURN x.name, a.name, b.name",
                     parameters,
                     {{"Adam", "Remy", "Eighties"}, {"Ghosts", "Remy", "Eighties"}});
}

TEST_F(WalkEdgeIDLookupTest, endsTheWalkOnTheListedEdge) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(3);

    expectEndedRows("MATCH (a)-[rs*1..2]->(b) WHERE id(last(rs)) = 3 RETURN a.name, b.name",
                    parameters,
                    {{"Adam", "Eighties"}, {"Ghosts", "Eighties"}, {"Remy", "Eighties"}});
    expectEndedRows("MATCH (a)-[rs*1..2]->(b) WHERE last(rs) = $id RETURN a.name, b.name",
                    parameters,
                    {{"Adam", "Eighties"}, {"Ghosts", "Eighties"}, {"Remy", "Eighties"}});
    expectEndedRows("MATCH (a)-[rs*1..3]->(b) WHERE id(rs[-1]) = 0 RETURN a.name, b.name, rs",
                    parameters,
                    {{"Adam", "Adam", "4, 0"}, {"Ghosts", "Adam", "7, 0"}, {"Remy", "Adam", "0"}, {"Remy", "Adam", "1, 7, 0"}});
    expectEndedRows("MATCH (a)-[rs*1..2]->(b) WHERE id(last(rs)) = 99 RETURN a.name, b.name", parameters, {});
}

TEST_F(WalkEdgeIDLookupTest, endsTheWalkOfEitherDirection) {
    const ParameterMap parameters;

    expectEndedRows("MATCH (a)<-[rs*1..2]-(b) WHERE id(last(rs)) = 0 RETURN a.name, b.name, rs",
                    parameters,
                    {{"Adam", "Remy", "0"}, {"Bio", "Remy", "5, 0"}, {"Cooking", "Remy", "6, 0"}, {"Remy", "Remy", "4, 0"}});
    expectEndedRows("MATCH (a)-[rs*1..1]-(b) WHERE id(last(rs)) = 3 RETURN a.name, b.name",
                    parameters,
                    {{"Eighties", "Remy"}, {"Remy", "Eighties"}});
}
