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

class NodeIDOfLookupTest : public CallV3Test {
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
        EXPECT_FALSE(contains(program, "db.scan_nodes()")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << query << "\n" << program;
    }

    void expectFetchedRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        expectLookup(query, parameters, "db.fetch_nodes");
        expectRows(query, parameters, expected);
    }

    void expectListedRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        expectLookup(query, parameters, "db.const_scan_nodes");
        expectRows(query, parameters, expected);
    }
};

TEST_F(NodeIDOfLookupTest, fetchesTheNodesAListOfAnyValuesNames) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setDouble(3.0);
    ids.emplace_back().setString("x");

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, fetchesTheNodesAListOfDoublesNames) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setDouble(1.0);
    ids.emplace_back().setDouble(3.0);

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, fetchesARepeatedIntegerOncePerRow) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setInt64(3);

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, fetchesNoNodeForANullOrANegativeNumber) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setNull();
    ids.emplace_back().setInt64(-1);
    ids.emplace_back().setDouble(-3.0);
    ids.emplace_back().setDouble(-0.0);

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Remy"}});
}

TEST_F(NodeIDOfLookupTest, fetchesNoNodeForAFractionalNumber) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setDouble(2.5);
    ids.emplace_back().setDouble(3.0);
    ids.emplace_back().setDouble(1e300);

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, fetchesNoNodeForAStringABooleanOrANestedList) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setString("4");
    ids.emplace_back().setBool(true);
    ids.emplace_back().setList().emplace_back().setInt64(5);
    ids.emplace_back().setInt64(3);

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, fetchesWithTheIDOnEitherSide) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setDouble(3.0);
    ids.emplace_back().setString("x");

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE i = id(n) RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, readsTheUnwoundValueBesideTheFetchedNode) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setDouble(3.0);
    ids.emplace_back().setString("x");

    expectFetchedRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN i, n.name", parameters, {{"1", "Adam"}, {"3", "Eighties"}});
}

TEST_F(NodeIDOfLookupTest, listsTheNodesAMixedLiteralListNames) {
    const ParameterMap parameters;

    expectListedRows("UNWIND [1, 'x', 3.0] AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
    expectListedRows("UNWIND [1, 3] AS i MATCH (n) WHERE id(n) = i RETURN i, n.name", parameters, {{"1", "Adam"}, {"3", "Eighties"}});
    expectListedRows("UNWIND [3.0] AS i MATCH (n) WHERE id(n) = i RETURN i, n.name", parameters, {{"3", "Eighties"}});
}

TEST_F(NodeIDOfLookupTest, fetchesTheNodesAMixedLiteralListNamesWhenItsValuesAreRead) {
    const ParameterMap parameters;

    expectFetchedRows("UNWIND [1, 'x', 3.0] AS i MATCH (n) WHERE id(n) = i RETURN i, n.name", parameters, {{"1", "Adam"}, {"3", "Eighties"}});
    expectFetchedRows("UNWIND [2.5, 3.0] AS i MATCH (n) WHERE id(n) = i RETURN i, n.name", parameters, {{"3", "Eighties"}});
    expectFetchedRows("UNWIND [1, 1.0] AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Adam"}});
}

TEST_F(NodeIDOfLookupTest, fetchesTheNodeAMapValueNames) {
    ParameterMap parameters;
    ParameterValue::List& rows = parameters.insert("rows").setList();
    rows.emplace_back().setMap()["id"].setString("1");
    rows.emplace_back().setMap()["id"].setString("3");
    rows.emplace_back().setMap()["id"].setString("x");

    expectFetchedRows("UNWIND $rows AS r MATCH (n) WHERE id(n) = toInteger(r.id) RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
    expectFetchedRows("UNWIND $rows AS r MATCH (n) WHERE id(n) = r.id RETURN n.name", parameters, {});

    ParameterMap numbers;
    ParameterValue::List& numberRows = numbers.insert("rows").setList();
    numberRows.emplace_back().setMap()["id"].setInt64(1);
    numberRows.emplace_back().setMap()["id"].setDouble(3.0);
    numberRows.emplace_back().setMap()["id"].setDouble(2.5);

    expectFetchedRows("UNWIND $rows AS r MATCH (n) WHERE id(n) = r.id RETURN n.name", numbers, {{"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, fetchesTheNodeAPropertyCarriedByAWithNames) {
    const ParameterMap parameters;

    expectFetchedRows("MATCH (m:Person) WITH m.name AS name, m.age - 31 AS i MATCH (n) WHERE id(n) = i RETURN name, n.name",
                      parameters,
                      {{"Adam", "Adam"}, {"Remy", "Adam"}});

    runWrite("CREATE (:Key {v: 1.0}), (:Key {v: 3.0}), (:Key {v: 2.5})");
    expectFetchedRows("MATCH (k:Key) WITH k.v AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(NodeIDOfLookupTest, listsTheNodesOfMembershipsAndDisjunctions) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setDouble(3.0);
    ids.emplace_back().setString("x");
    parameters.insert("integer").setInt64(1);
    parameters.insert("double").setDouble(3.0);

    expectListedRows("MATCH (n) WHERE id(n) IN $ids RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
    expectListedRows("MATCH (n) WHERE id(n) = $integer OR id(n) = $double OR id(n) = 5 RETURN n.name",
                     parameters,
                     {{"Adam"}, {"Cooking"}, {"Eighties"}});
    expectListedRows("MATCH (n) WHERE id(n) IN [1, 'x', 3.0] OR id(n) = 2.0 OR id(n) = 2.5 RETURN n.name",
                     parameters,
                     {{"Adam"}, {"Computers"}, {"Eighties"}});
    expectListedRows("MATCH (n) WHERE id(n) = $double + 1 OR id(n) IN [$integer] RETURN n.name", parameters, {{"Adam"}, {"Bio"}});
}

TEST_F(NodeIDOfLookupTest, comparesAnIDOutsideAFilterAsItself) {
    const ParameterMap parameters;

    expectRows("MATCH (n) WHERE id(n) < 3 RETURN n.name, id(n) = 1.0, id(n) = 2.5, id(n) = -1",
               parameters,
               {{"Adam", "true", "false", "false"}, {"Computers", "false", "false", "false"}, {"Remy", "false", "false", "false"}});
    expectRows("MATCH (n) WHERE NOT id(n) = 2.5 AND id(n) < 2 RETURN n.name", parameters, {{"Adam"}, {"Remy"}});
    expectRows("UNWIND [2.5, 'x'] AS i MATCH (n) WHERE NOT id(n) = i AND id(n) < 2 RETURN n.name, i",
               parameters,
               {{"Adam", "2.5"}, {"Adam", "x"}, {"Remy", "2.5"}, {"Remy", "x"}});
}

TEST_F(NodeIDOfLookupTest, comparesTwoIDsAsTheirNodes) {
    const ParameterMap parameters;

    expectRows("MATCH (n), (m) WHERE id(n) = id(m) AND id(m) < 2 RETURN n.name, m.name", parameters, {{"Adam", "Adam"}, {"Remy", "Remy"}});
    expectRows("MATCH (n)-[r]->(m) WHERE id(n) = r RETURN n.name, m.name", parameters, {{"Doruk", "Gym"}, {"Maxime", "Bio"}, {"Remy", "Adam"}});
    expectRows("MATCH (n)-[r]->(m) WHERE id(n) = coalesce(r, r) RETURN n.name, m.name",
               parameters,
               {{"Doruk", "Gym"}, {"Maxime", "Bio"}, {"Remy", "Adam"}});
    expectRows("MATCH (n)-[r]->(m) WHERE id(m) = id(r) RETURN n.name, m.name",
               parameters,
               {{"Cyrus", "Gym"}, {"Cyrus", "Travel"}, {"Luc", "Animals"}, {"Remy", "Computers"}, {"Remy", "Eighties"}, {"Suhas", "JiuJitsu"}});
}
