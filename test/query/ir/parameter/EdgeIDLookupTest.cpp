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

class EdgeIDLookupTest : public CallV3Test {
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

    void expectLookup(std::string_view query, const ParameterMap& parameters, std::string_view lookup) {
        std::string program;
        dbProgramOf(query, parameters, program);

        EXPECT_TRUE(contains(program, lookup)) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_nodes(")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_nodes_by_label(")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_edges")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_out_edges")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_in_edges")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << query << "\n" << program;
    }

    void expectListedRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        expectLookup(query, parameters, "db.const_scan_edges");
        expectRows(query, parameters, expected);
    }

    void expectFetchedRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        expectLookup(query, parameters, "db.fetch_edges");
        expectRows(query, parameters, expected);
    }

    static void setIntegers(ParameterMap& parameters, std::string_view name, const std::vector<int64_t>& values) {
        ParameterValue::List& list = parameters.insert(std::string(name)).setList();
        for (const int64_t value : values) {
            list.emplace_back().setInt64(value);
        }
    }
};

TEST_F(EdgeIDLookupTest, listsTheEdgeALiteralIDNames) {
    const ParameterMap parameters;

    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = 3 RETURN a.name, type(r), b.name",
                     parameters,
                     {{"Remy", "INTERESTED_IN", "Eighties"}});
    expectListedRows("MATCH ()-[r]->() WHERE 3 = id(r) RETURN id(r)", parameters, {{"3"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE r = 3 RETURN a.name, b.name", parameters, {{"Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, listsTheEdgeAParameterNames) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(3);

    expectListedRows("MATCH ()-[r]->() WHERE id(r) = $id RETURN r", parameters, {{"3"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = $id RETURN a.name, b.name", parameters, {{"Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, listsTheEdgesOfMembershipsAndDisjunctions) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {0, 3, 4, 7});
    parameters.insert("id").setInt64(3);

    expectListedRows("MATCH (a)-[r:KNOWS_WELL]->(b) WHERE id(r) IN $ids RETURN a.name, b.name",
                     parameters,
                     {{"Adam", "Remy"}, {"Ghosts", "Remy"}, {"Remy", "Adam"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) IN [1, 3, 99] RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Eighties"}, {"Remy", "Ghosts"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = $id OR id(r) = 5 OR id(r) IN [9] RETURN a.name, b.name",
                     parameters,
                     {{"Adam", "Bio"}, {"Maxime", "Padel"}, {"Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, listsTheEdgeAComputedValueNames) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(3);

    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = $id + 1 RETURN a.name, b.name", parameters, {{"Adam", "Remy"}});
}

TEST_F(EdgeIDLookupTest, listsTheEdgesDoublesAndMixedListsName) {
    ParameterMap parameters;
    ParameterValue::List& mixed = parameters.insert("mixed").setList();
    mixed.emplace_back().setInt64(1);
    mixed.emplace_back().setDouble(3.0);
    mixed.emplace_back().setString("x");
    parameters.insert("double").setDouble(5.0);

    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) IN $mixed RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Eighties"}, {"Remy", "Ghosts"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = $double RETURN a.name, b.name", parameters, {{"Adam", "Bio"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = 3.0 OR id(r) = 2.5 RETURN a.name, b.name", parameters, {{"Remy", "Eighties"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) IN [1, 'x', 3.0] RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Eighties"}, {"Remy", "Ghosts"}});
}

TEST_F(EdgeIDLookupTest, listsNoEdgeForAMissingOrANegativeID) {
    ParameterMap parameters;
    parameters.insert("missing").setInt64(99);
    parameters.insert("negative").setInt64(-1);

    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = 99 RETURN a.name, b.name", parameters, {});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = $missing RETURN a.name, b.name", parameters, {});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = -1 RETURN a.name, b.name", parameters, {});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = $negative RETURN a.name, b.name", parameters, {});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) IN [-1, 99, 2] RETURN a.name, b.name", parameters, {{"Remy", "Computers"}});
}

TEST_F(EdgeIDLookupTest, bindsTheEndpointsOfEitherDirection) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(3);

    expectListedRows("MATCH (a)<-[r]-(b) WHERE id(r) = 3 RETURN a.name, b.name", parameters, {{"Eighties", "Remy"}});
    expectListedRows("MATCH (a)-[r]-(b) WHERE id(r) = 3 RETURN a.name, b.name",
                     parameters,
                     {{"Eighties", "Remy"}, {"Remy", "Eighties"}});
    expectListedRows("MATCH (a)-[r]-(b) WHERE id(r) = $id RETURN a.name, b.name",
                     parameters,
                     {{"Eighties", "Remy"}, {"Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, keepsTheTypeLabelAndPropertyFilters) {
    const ParameterMap parameters;

    expectListedRows("MATCH (a)-[r:KNOWS_WELL]->(b) WHERE id(r) = 3 RETURN a.name, b.name", parameters, {});
    expectListedRows("MATCH (a)-[r:KNOWS_WELL]->(b) WHERE id(r) = 0 RETURN a.name, b.name", parameters, {{"Remy", "Adam"}});
    expectListedRows("MATCH (a:Interest)<-[r]-(b:Person) WHERE id(r) IN [3, 7] RETURN a.name, b.name", parameters, {{"Eighties", "Remy"}});
    expectListedRows("MATCH (a:Person)-[r]->(b) WHERE id(r) IN [3, 7] RETURN a.name, b.name", parameters, {{"Remy", "Eighties"}});
    expectListedRows("MATCH (a:Person)-[r]-(b) WHERE id(r) IN [3, 4] RETURN a.name, b.name",
                     parameters,
                     {{"Adam", "Remy"}, {"Remy", "Adam"}, {"Remy", "Eighties"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) IN [0, 1, 2] AND b.name = 'Adam' RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Adam"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) IN [0, 1, 2] AND r.duration = 20 RETURN a.name, b.name",
                     parameters,
                     {{"Remy", "Adam"}, {"Remy", "Ghosts"}});
}

TEST_F(EdgeIDLookupTest, keepsTheFiltersOnTheSource) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(3);

    expectListedRows("MATCH (a)-[r]->(b) WHERE a.name = 'Remy' AND id(r) IN [3, 4] RETURN a.name, b.name", parameters, {{"Remy", "Eighties"}});
    expectListedRows("MATCH (a:Person {name: 'Adam'})-[r]->(b) WHERE id(r) IN [3, 4] RETURN a.name, b.name", parameters, {{"Adam", "Remy"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE a.age > 30 AND id(r) IN [3, 4, 8] RETURN a.name, b.name",
                     parameters,
                     {{"Adam", "Remy"}, {"Remy", "Eighties"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(a) = 0 AND id(r) = $id RETURN a.name, b.name", parameters, {{"Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, buildsThePathOfTheListedEdge) {
    const ParameterMap parameters;

    expectListedRows("MATCH p = (a)-[r]->(b) WHERE id(r) = 3 RETURN p", parameters, {{"(0), [3], (3)"}});
    expectListedRows("MATCH p = (a)<-[r]-(b) WHERE id(r) = 3 RETURN p", parameters, {{"(3), [3], (0)"}});
}

TEST_F(EdgeIDLookupTest, walksOnFromTheListedEdge) {
    const ParameterMap parameters;

    expectListedRows("MATCH (a)-[r]->(b)-[s]->(c) WHERE id(r) = 0 RETURN a.name, b.name, c.name",
                     parameters,
                     {{"Remy", "Adam", "Bio"}, {"Remy", "Adam", "Cooking"}, {"Remy", "Adam", "Remy"}});
    expectListedRows("MATCH (a)-[s]->(b)-[r]->(c) WHERE id(r) = 3 RETURN a.name, b.name, c.name",
                     parameters,
                     {{"Adam", "Remy", "Eighties"}, {"Ghosts", "Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, listsAnEdgeTheQueryCreated) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(18);

    const std::string_view byParameter = "CREATE (:Probe {name: 'p'})-[:PROBE]->(:Probe {name: 'q'}) WITH 1 AS one "
                                         "MATCH (a)-[r]->(b) WHERE id(r) = $id RETURN a.name, type(r), b.name";
    const std::string_view byLiteral = "CREATE (:Probe {name: 's'})-[:PROBE]->(:Probe {name: 't'}) WITH 1 AS one "
                                       "MATCH (a)-[r]-(b) WHERE id(r) = 19 RETURN a.name, type(r), b.name";

    expectLookup(byParameter, parameters, "db.const_scan_edges");
    expectLookup(byLiteral, parameters, "db.const_scan_edges");

    StringRowSink byParameterRows;
    runWrite(byParameter, parameters, byParameterRows);

    StringRowSink byLiteralRows;
    runWrite(byLiteral, byLiteralRows);

    Rows rows;
    byLiteralRows.sortedRows(rows);

    const Rows expectedByParameter {{"p", "PROBE", "q"}};
    const Rows expectedByLiteral {{"s", "PROBE", "t"}, {"t", "PROBE", "s"}};
    EXPECT_EQ(byParameterRows.getRows(), expectedByParameter);
    EXPECT_EQ(rows, expectedByLiteral);
}

TEST_F(EdgeIDLookupTest, fetchesAnEdgeTheQueryCreated) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {18, 99});

    const std::string_view query = "CREATE (:Probe {name: 'p'})-[:PROBE]->(:Probe {name: 'q'}) WITH 1 AS one "
                                   "UNWIND $ids AS i MATCH (a)-[r]->(b) WHERE id(r) = i RETURN a.name, type(r), b.name";

    expectLookup(query, parameters, "db.fetch_edges");

    StringRowSink rows;
    runWrite(query, parameters, rows);

    const Rows expected {{"p", "PROBE", "q"}};
    EXPECT_EQ(rows.getRows(), expected);
}

TEST_F(EdgeIDLookupTest, listsASelfLoopFromEachEnd) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {18});

    runWrite("CREATE (x:Loop {name: 'x'}) CREATE (x)-[:SELF]->(x)");

    expectListedRows("MATCH (a)-[r]-(b) WHERE id(r) = 18 RETURN a.name, b.name", parameters, {{"x", "x"}, {"x", "x"}});
    expectListedRows("MATCH (a)-[r]->(b) WHERE id(r) = 18 RETURN a.name, b.name", parameters, {{"x", "x"}});
    expectFetchedRows("UNWIND $ids AS i MATCH (a)-[r]-(b) WHERE id(r) = i RETURN a.name, b.name",
                      parameters,
                      {{"x", "x"}, {"x", "x"}});
}

TEST_F(EdgeIDLookupTest, fetchesTheEdgeEachUnwoundValueNames) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(3);
    ids.emplace_back().setInt64(3);
    ids.emplace_back().setInt64(99);
    ids.emplace_back().setInt64(-1);
    ids.emplace_back().setDouble(4.0);
    ids.emplace_back().setDouble(2.5);
    ids.emplace_back().setNull();
    ids.emplace_back().setString("x");

    expectFetchedRows("UNWIND $ids AS i MATCH (a)-[r]->(b) WHERE id(r) = i RETURN i, a.name, b.name",
                      parameters,
                      {{"3", "Remy", "Eighties"}, {"3", "Remy", "Eighties"}, {"4", "Adam", "Remy"}});
    expectFetchedRows("UNWIND $ids AS i MATCH ()-[r]->() WHERE id(r) = i RETURN r", parameters, {{"3"}, {"3"}, {"4"}});
}

TEST_F(EdgeIDLookupTest, fetchesTheEdgesOfEitherDirectionPerRow) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {0, 3, 3});

    expectFetchedRows("UNWIND $ids AS i MATCH (a)<-[r:INTERESTED_IN]-(b:Person) WHERE id(r) = i RETURN a.name, b.name",
                      parameters,
                      {{"Eighties", "Remy"}, {"Eighties", "Remy"}});
    expectFetchedRows("UNWIND $ids AS i MATCH (a)-[r]-(b) WHERE id(r) = i RETURN a.name, b.name",
                      parameters,
                      {{"Adam", "Remy"},
                       {"Eighties", "Remy"},
                       {"Eighties", "Remy"},
                       {"Remy", "Adam"},
                       {"Remy", "Eighties"},
                       {"Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, fetchesTheEdgeAColumnOfAnotherMatchNames) {
    const ParameterMap parameters;

    expectFetchedRows("MATCH (x:Person) WHERE x.age = 32 MATCH (a)-[r]->(b) WHERE id(r) = x.age - 28 RETURN x.name, a.name, b.name",
                      parameters,
                      {{"Adam", "Adam", "Remy"}, {"Remy", "Adam", "Remy"}});
}

TEST_F(EdgeIDLookupTest, walksOnFromTheFetchedEdge) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {3});

    expectFetchedRows("UNWIND $ids AS i MATCH (a)-[s]->(b)-[r]->(c) WHERE id(r) = i RETURN a.name, b.name, c.name",
                      parameters,
                      {{"Adam", "Remy", "Eighties"}, {"Ghosts", "Remy", "Eighties"}});
}

TEST_F(EdgeIDLookupTest, foldsALiteralListIntoTheListedEdges) {
    const ParameterMap parameters;

    expectListedRows("UNWIND [3, 5] AS i MATCH (a)-[r]->(b) WHERE id(r) = i RETURN i, a.name, b.name",
                     parameters,
                     {{"3", "Remy", "Eighties"}, {"5", "Adam", "Bio"}});
    expectFetchedRows("UNWIND [3, 3] AS i MATCH ()-[r]->() WHERE id(r) = i RETURN i", parameters, {{"3"}, {"3"}});
    expectListedRows("UNWIND [3, 5] AS i MATCH (a)-[s]->(b)-[r]->(c) WHERE id(r) = i RETURN a.name, b.name, c.name",
                     parameters,
                     {{"Adam", "Remy", "Eighties"}, {"Ghosts", "Remy", "Eighties"}, {"Remy", "Adam", "Bio"}});
}
