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

void setIntegers(ParameterMap& parameters, std::string_view name, const std::vector<int64_t>& values) {
    ParameterValue::List& list = parameters.insert(name).setList();
    for (const int64_t value : values) {
        list.emplace_back().setInt64(value);
    }
}

}

class ParameterLookupTest : public CallV3Test {
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

        EXPECT_TRUE(contains(program, lookup)) << program;
        EXPECT_FALSE(contains(program, "db.scan_nodes()")) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
    }
};

TEST_F(ParameterLookupTest, looksUpANodeIDParameter) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(1);

    const std::string_view query = "MATCH (n) WHERE id(n) = $id RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}});
}

TEST_F(ParameterLookupTest, looksUpANodeIDParameterOnTheLeft) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(3);

    const std::string_view query = "MATCH (n) WHERE $id = id(n) RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Eighties"}});
}

TEST_F(ParameterLookupTest, looksUpANodeIDParameterNamingNoNode) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(999);

    const std::string_view query = "MATCH (n) WHERE id(n) = $id RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {});

    parameters.insert("id").setInt64(-1);
    expectRows(query, parameters, {});

    parameters.set("id", ParameterValue());
    expectRows(query, parameters, {});
}

TEST_F(ParameterLookupTest, keepsTheLabelOfALookedUpNode) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(1);

    const std::string_view query = "MATCH (n:Interest) WHERE id(n) = $id RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {});

    parameters.insert("id").setInt64(3);
    expectRows(query, parameters, {{"Eighties"}});
}

TEST_F(ParameterLookupTest, looksUpADisjunctionOfParametersAndLiterals) {
    ParameterMap parameters;
    parameters.insert("first").setInt64(1);
    parameters.insert("second").setInt64(1);

    const std::string_view query = "MATCH (n) WHERE id(n) = $first OR id(n) = $second OR id(n) = 3 RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(ParameterLookupTest, looksUpAListOfNodeIDs) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {1, 3});

    const std::string_view query = "MATCH (n) WHERE id(n) IN $ids RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(ParameterLookupTest, returnsANodeListedTwiceOnce) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {3, 1, 3, 1});

    const std::string_view query = "MATCH (n) WHERE id(n) IN $ids RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Eighties"}});
    expectRows("MATCH (n) WHERE id(n) IN $ids RETURN count(n)", parameters, {{"2"}});
}

TEST_F(ParameterLookupTest, listsNoNodeForANullANegativeOrAnAbsentID) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setNull();
    ids.emplace_back().setInt64(-1);
    ids.emplace_back().setInt64(999);

    const std::string_view query = "MATCH (n) WHERE id(n) IN $ids RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}});
    expectRows("MATCH (n) WHERE id(n) IN [1, null, -1, 999] RETURN n.name", parameters, {{"Adam"}});
}

TEST_F(ParameterLookupTest, comparesNonIntegerElementsAsTheFilterDoes) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setString("x");
    ids.emplace_back().setDouble(3.0);

    expectRows("MATCH (n) WHERE id(n) IN $ids RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
    expectRows("MATCH (n) WHERE id(n) IN [1, 'x', 3.0] RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(ParameterLookupTest, looksUpAnEmptyListOfNodeIDs) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {});

    expectRows("MATCH (n) WHERE id(n) IN $ids RETURN n.name", parameters, {});
}

TEST_F(ParameterLookupTest, looksUpANodeTheQueryCreated) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(18);

    StringRowSink byParameter;
    runWrite("CREATE (z:Person {name: 'Zed'}) WITH z MATCH (a:Person) WHERE id(a) = $id RETURN a.name", parameters, byParameter);

    StringRowSink byLiteral;
    runWrite("CREATE (z:Person {name: 'Zod'}) WITH z MATCH (a:Person) WHERE id(a) = 19 RETURN a.name", byLiteral);

    const Rows expectedByParameter {{"Zed"}};
    const Rows expectedByLiteral {{"Zod"}};
    EXPECT_EQ(byParameter.getRows(), expectedByParameter);
    EXPECT_EQ(byLiteral.getRows(), expectedByLiteral);
}

TEST_F(ParameterLookupTest, fetchesEachUnwoundNodeID) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {1, 1, 3});

    const std::string_view query = "UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN n.name";
    expectLookup(query, parameters, "db.fetch_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Adam"}, {"Eighties"}});
    expectRows("UNWIND $ids AS i MATCH (n) WHERE id(n) = i RETURN count(n)", parameters, {{"3"}});
}

TEST_F(ParameterLookupTest, walksAHopFromALookedUpNode) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(0);

    const std::string_view query = "MATCH (a)-[:KNOWS_WELL]->(b) WHERE id(a) = $id RETURN b.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}});

    std::string program;
    dbProgramOf(query, parameters, program);
    EXPECT_FALSE(contains(program, "db.scan_edges")) << program;
}

TEST_F(ParameterLookupTest, endsAWalkOnALookedUpNode) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(1);

    const std::string_view query = "MATCH (a:Person)-[:KNOWS_WELL*1..2]->(b) WHERE id(b) = $id RETURN a.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Remy"}});
}

TEST_F(ParameterLookupTest, walksAHopBackFromALookedUpNode) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(0);

    const std::string_view query = "MATCH (a:Person)-[:KNOWS_WELL]->(b) WHERE id(b) = $id RETURN a.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}});
}

TEST_F(ParameterLookupTest, looksUpAPropertyParameter) {
    ParameterMap parameters;
    parameters.insert("name").setString("Adam");

    const std::string_view query = "MATCH (n {name: $name}) RETURN id(n)";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"1"}});

    parameters.insert("name").setString("Nobody");
    expectRows(query, parameters, {});
}

TEST_F(ParameterLookupTest, walksAHopFromALookedUpProperty) {
    ParameterMap parameters;
    parameters.insert("name").setString("Remy");

    const std::string_view query = "MATCH (a)-[:KNOWS_WELL]->(b) WHERE a.name = $name RETURN b.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"Adam"}});
}

TEST_F(ParameterLookupTest, walksAPathFromItsLookedUpMiddle) {
    ParameterMap parameters;
    parameters.insert("name").setString("Gym");

    const std::string_view query = "MATCH (q)-[:INTERESTED_IN]->(i:Interest {name: $name})<-[:INTERESTED_IN]-(p) "
                                   "RETURN q.name, p.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {
        {"Cyrus", "Cyrus"},
        {"Cyrus", "Doruk"},
        {"Cyrus", "Suhas"},
        {"Doruk", "Cyrus"},
        {"Doruk", "Doruk"},
        {"Doruk", "Suhas"},
        {"Suhas", "Cyrus"},
        {"Suhas", "Doruk"},
        {"Suhas", "Suhas"},
    });
}

TEST_F(ParameterLookupTest, startsAtALookedUpPropertyRatherThanAFewLabelledNodes) {
    ParameterMap parameters;
    parameters.insert("name").setString("Ghosts");

    const std::string_view query = "MATCH (a:Founder)-[:INTERESTED_IN]->(i) WHERE i.name = $name RETURN a.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"Remy"}});

    std::string program;
    dbProgramOf(query, parameters, program);
    EXPECT_FALSE(contains(program, "db.scan_nodes_by_label")) << program;

    std::string literalProgram;
    dbProgramOf("MATCH (a:Founder)-[:INTERESTED_IN]->(i) WHERE i.name = 'Ghosts' RETURN a.name", parameters, literalProgram);
    EXPECT_FALSE(contains(literalProgram, "db.scan_nodes_by_label")) << literalProgram;
}

TEST_F(ParameterLookupTest, joinsTwoPatternsFromTheLookedUpOne) {
    ParameterMap parameters;
    parameters.insert("name").setString("Remy");

    const std::string_view query = "MATCH (p:Person)-[:INTERESTED_IN]->(i1:Interest), (q:Person)-[:INTERESTED_IN]->(i2:Interest) "
                                   "WHERE i1 = i2 AND p.name = $name RETURN q.name, i1.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {
        {"Luc", "Computers"},
        {"Remy", "Computers"},
        {"Remy", "Eighties"},
        {"Remy", "Ghosts"},
    });
}

TEST_F(ParameterLookupTest, looksUpAPropertyParameterAmongLabelledNodes) {
    ParameterMap parameters;
    parameters.insert("real").setBool(true);

    const std::string_view query = "MATCH (n:Exotic) WHERE n.isReal = $real RETURN n.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"Ghosts"}});
}

TEST_F(ParameterLookupTest, looksUpANumberParameterByValue) {
    ParameterMap parameters;
    parameters.insert("age").setDouble(32.0);

    const std::string_view query = "MATCH (n:Person) WHERE n.age = $age RETURN n.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"Adam"}, {"Remy"}});

    parameters.insert("age").setDouble(32.5);
    expectRows(query, parameters, {});

    parameters.insert("age").setInt64(32);
    expectRows(query, parameters, {{"Adam"}, {"Remy"}});
}

TEST_F(ParameterLookupTest, keepsTheOtherConjunctsOfALookedUpProperty) {
    ParameterMap parameters;
    parameters.insert("age").setInt64(32);

    const std::string_view query = "MATCH (n) WHERE n.age = $age AND n.name STARTS WITH 'R' RETURN n.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"Remy"}});
}

