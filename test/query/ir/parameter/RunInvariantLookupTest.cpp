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

class RunInvariantLookupTest : public CallV3Test {
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

    void expectNoNodeIDLookup(std::string_view query, const ParameterMap& parameters) {
        std::string program;
        dbProgramOf(query, parameters, program);

        EXPECT_FALSE(contains(program, "db.const_scan_nodes")) << query << "\n" << program;
    }
};

TEST_F(RunInvariantLookupTest, looksUpANodeIDComputedFromAParameter) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(0);

    const std::string_view query = "MATCH (n) WHERE id(n) = $id + 1 RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}});

    parameters.insert("id").setInt64(2);
    expectRows(query, parameters, {{"Eighties"}});

    parameters.insert("id").setInt64(998);
    expectRows(query, parameters, {});
}

TEST_F(RunInvariantLookupTest, looksUpAListOfNodeIDParameters) {
    ParameterMap parameters;
    parameters.insert("a").setInt64(1);
    parameters.insert("b").setInt64(3);

    const std::string_view query = "MATCH (n) WHERE id(n) IN [$a, $b] RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(RunInvariantLookupTest, looksUpANodeIDConvertedFromAParameter) {
    ParameterMap parameters;
    parameters.insert("s").setString("3");

    const std::string_view query = "MATCH (n) WHERE id(n) = toInteger($s) RETURN n.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Eighties"}});

    parameters.insert("s").setString("x");
    expectRows(query, parameters, {});
}

TEST_F(RunInvariantLookupTest, looksUpAPropertyValueComputedFromLiterals) {
    const ParameterMap parameters;

    const std::string_view query = "MATCH (n) WHERE n.name = 'Ad' + 'am' RETURN id(n)";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"1"}});
}

TEST_F(RunInvariantLookupTest, looksUpAPropertyValueComputedFromParameters) {
    ParameterMap parameters;
    parameters.insert("first").setString("Ad");
    parameters.insert("last").setString("am");

    const std::string_view query = "MATCH (n) WHERE n.name = $first + $last RETURN id(n)";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"1"}});
}

TEST_F(RunInvariantLookupTest, looksUpANumberPropertyComputedFromAParameter) {
    ParameterMap parameters;
    parameters.insert("age").setInt64(31);

    const std::string_view query = "MATCH (n:Person) WHERE n.age = $age + 1 RETURN n.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"Adam"}, {"Remy"}});
}

TEST_F(RunInvariantLookupTest, walksAHopFromANodeLookedUpByAComputedID) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(1);

    const std::string_view query = "MATCH (a)-[:KNOWS_WELL]->(b) WHERE id(a) = $id - 1 RETURN b.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}});

    std::string program;
    dbProgramOf(query, parameters, program);
    EXPECT_FALSE(contains(program, "db.scan_edges")) << program;
}

TEST_F(RunInvariantLookupTest, endsAWalkOnANodeLookedUpByAComputedID) {
    ParameterMap parameters;
    parameters.insert("id").setInt64(0);

    const std::string_view query = "MATCH (a:Person)-[:KNOWS_WELL*1..2]->(b) WHERE id(b) = $id + 1 RETURN a.name";
    expectLookup(query, parameters, "db.const_scan_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Remy"}});
}

TEST_F(RunInvariantLookupTest, startsAtAPropertyLookedUpByAComputedValue) {
    ParameterMap parameters;
    parameters.insert("rest").setString("osts");

    const std::string_view query = "MATCH (a:Founder)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gh' + $rest RETURN a.name";
    expectLookup(query, parameters, "db.scan_nodes_by_property_value");
    expectRows(query, parameters, {{"Remy"}});

    std::string program;
    dbProgramOf(query, parameters, program);
    EXPECT_FALSE(contains(program, "db.scan_nodes_by_label")) << program;
}

TEST_F(RunInvariantLookupTest, fetchesAnIDReadFromAnotherPattern) {
    const ParameterMap parameters;

    const std::string_view query = "MATCH (m {name: 'Remy'}), (n) WHERE id(n) = m.age - 31 RETURN n.name";
    expectNoNodeIDLookup(query, parameters);
    expectLookup(query, parameters, "db.fetch_nodes");
    expectRows(query, parameters, {{"Adam"}});
}

TEST_F(RunInvariantLookupTest, fetchesAnIDComputedPerUnwoundRow) {
    ParameterMap parameters;
    setIntegers(parameters, "ids", {0, 2});

    const std::string_view query = "UNWIND $ids AS i MATCH (n) WHERE id(n) = i + 1 RETURN n.name";
    expectNoNodeIDLookup(query, parameters);
    expectLookup(query, parameters, "db.fetch_nodes");
    expectRows(query, parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(RunInvariantLookupTest, divideByZeroOnlyOverScannedNodes) {
    ParameterMap parameters;
    parameters.insert("zero").setInt64(0);

    const std::string_view byID = "MATCH (n:NoSuchLabel) WHERE id(n) = 1 / $zero RETURN n.name";
    expectNoNodeIDLookup(byID, parameters);
    expectRows(byID, parameters, {});

    const std::string_view byProperty = "MATCH (n:NoSuchLabel) WHERE n.age = 1 / $zero RETURN n.name";
    expectRows(byProperty, parameters, {});

    runQueryExpectingError("MATCH (n) WHERE id(n) = 1 / $zero RETURN n.name", parameters, "zero");
}
