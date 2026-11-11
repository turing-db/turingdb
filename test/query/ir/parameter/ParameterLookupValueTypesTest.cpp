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

class ParameterLookupValueTypesTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectLookup(std::string_view query, const ParameterMap& parameters) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), parameters, sink);
        ASSERT_EQ(sink.getRows().size(), 1u);

        const std::string& program = sink.getRows().front().back();
        EXPECT_TRUE(contains(program, "db.const_scan_nodes")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.scan_nodes()")) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << query << "\n" << program;
    }

    void expectLookedUpRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        expectLookup(query, parameters);
        expectRows(query, parameters, expected);
    }
};

TEST_F(ParameterLookupValueTypesTest, looksUpTheNumbersAMixedListHolds) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setString("x");
    ids.emplace_back().setDouble(3.0);

    expectLookedUpRows("MATCH (n) WHERE id(n) IN $ids RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(ParameterLookupValueTypesTest, looksUpTheIntegralNumbersOfAList) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setString("x");
    ids.emplace_back().setDouble(3.0);
    ids.emplace_back().setNull();
    ids.emplace_back().setList().emplace_back().setInt64(2);
    ids.emplace_back().setDouble(2.5);
    ids.emplace_back().setInt64(4);
    ids.emplace_back().setDouble(-1.0);

    const std::string_view query = "MATCH (n) WHERE id(n) IN $ids RETURN n.name";
    expectLookedUpRows(query, parameters, {{"Adam"}, {"Bio"}, {"Eighties"}});

    ParameterValue::List& doubles = parameters.insert("ids").setList();
    doubles.emplace_back().setDouble(1.0);
    doubles.emplace_back().setDouble(2.5);
    doubles.emplace_back().setDouble(3.0);
    expectLookedUpRows(query, parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(ParameterLookupValueTypesTest, looksUpNoNodeForAListOfNoNumber) {
    const std::string_view query = "MATCH (n) WHERE id(n) IN $ids RETURN n.name";

    ParameterMap empty;
    empty.insert("ids").setList();
    expectLookedUpRows(query, empty, {});

    ParameterMap strings;
    strings.insert("ids").setList().emplace_back().setString("1");
    expectLookedUpRows(query, strings, {});

    ParameterMap nulls;
    nulls.insert("ids").setList().emplace_back().setNull();
    expectLookedUpRows(query, nulls, {});

    ParameterMap lists;
    ParameterValue::List& nested = lists.insert("ids").setList();
    nested.emplace_back().setList().emplace_back().setInt64(1);
    nested.emplace_back().setList().emplace_back().setInt64(2);
    expectLookedUpRows(query, lists, {});
}

TEST_F(ParameterLookupValueTypesTest, looksUpADoubleParameter) {
    const std::string_view query = "MATCH (n) WHERE id(n) = $id RETURN n.name";

    ParameterMap parameters;
    parameters.insert("id").setDouble(2.0);
    expectLookedUpRows(query, parameters, {{"Computers"}});

    parameters.insert("id").setDouble(-0.0);
    expectLookedUpRows(query, parameters, {{"Remy"}});

    parameters.insert("id").setDouble(2.5);
    expectLookedUpRows(query, parameters, {});

    parameters.insert("id").setDouble(1e300);
    expectLookedUpRows(query, parameters, {});
}

TEST_F(ParameterLookupValueTypesTest, looksUpNoNodeForANullParameter) {
    ParameterMap parameters;
    parameters.insert("id").setNull();

    expectLookedUpRows("MATCH (n) WHERE id(n) = $id RETURN n.name", parameters, {});
    expectLookedUpRows("MATCH (n) WHERE id(n) IN $id RETURN n.name", parameters, {});
    expectLookedUpRows("MATCH (n) WHERE n.age = $id RETURN n.name", parameters, {});
}

TEST_F(ParameterLookupValueTypesTest, looksUpNoNodeForAParameterOfAnotherType) {
    const std::string_view query = "MATCH (n) WHERE id(n) = $id RETURN n.name";

    ParameterMap parameters;
    parameters.insert("id").setString("2");
    expectLookedUpRows(query, parameters, {});

    parameters.insert("id").setBool(true);
    expectLookedUpRows(query, parameters, {});

    parameters.insert("id").setList().emplace_back().setInt64(2);
    expectLookedUpRows(query, parameters, {});
}

TEST_F(ParameterLookupValueTypesTest, looksUpADisjunctionOfAnIntegerAndADouble) {
    ParameterMap parameters;
    parameters.insert("integer").setInt64(1);
    parameters.insert("double").setDouble(3.0);

    expectLookedUpRows("MATCH (n) WHERE id(n) = $integer OR id(n) = $double RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
    expectLookedUpRows("MATCH (n) WHERE id(n) = $double OR id(n) = 5 RETURN n.name", parameters, {{"Cooking"}, {"Eighties"}});
}

TEST_F(ParameterLookupValueTypesTest, looksUpTheNumbersALiteralListHolds) {
    const ParameterMap parameters;

    expectLookedUpRows("MATCH (n) WHERE id(n) IN [1, 'x', 3.0] RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
    expectLookedUpRows("MATCH (n) WHERE id(n) IN [1, 'x', 3.0, null, [2], 2.5, 4] RETURN n.name", parameters, {{"Adam"}, {"Bio"}, {"Eighties"}});
    expectLookedUpRows("MATCH (n) WHERE id(n) IN [1.0, 3.0] RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
    expectLookedUpRows("MATCH (n) WHERE id(n) IN [1, 3.0] OR id(n) = 5 RETURN n.name", parameters, {{"Adam"}, {"Cooking"}, {"Eighties"}});
}

TEST_F(ParameterLookupValueTypesTest, looksUpALiteralDouble) {
    const ParameterMap parameters;

    expectLookedUpRows("MATCH (n) WHERE id(n) = 3.0 RETURN n.name", parameters, {{"Eighties"}});
    expectLookedUpRows("MATCH (n) WHERE id(n) = -0.0 RETURN n.name", parameters, {{"Remy"}});
    expectLookedUpRows("MATCH (n) WHERE id(n) = 2.5 RETURN n.name", parameters, {});
    expectLookedUpRows("MATCH (n) WHERE id(n) = 1 OR id(n) = 3.0 RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(ParameterLookupValueTypesTest, looksUpNoNodeForALiteralOfAnotherType) {
    const ParameterMap parameters;

    expectLookedUpRows("MATCH (n) WHERE id(n) = null RETURN n.name", parameters, {});
    expectLookedUpRows("MATCH (n) WHERE id(n) = 'x' RETURN n.name", parameters, {});
    expectLookedUpRows("MATCH (n) WHERE id(n) = [3] RETURN n.name", parameters, {});
}

TEST_F(ParameterLookupValueTypesTest, looksUpAnUnwoundListOfDoubles) {
    const ParameterMap parameters;

    expectLookedUpRows("UNWIND [1.0, 3.0] AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}, {"Eighties"}});
}

TEST_F(ParameterLookupValueTypesTest, comparesANodeToADoubleAsTheFilterDoes) {
    ParameterMap parameters;
    parameters.insert("id").setDouble(2.0);

    expectRows("MATCH (n) WHERE n = $id RETURN n.name", parameters, {});
    expectRows("MATCH (n) WHERE n = 3.0 RETURN n.name", parameters, {});
    expectRows("MATCH (n) WHERE n IN [1, 3.0] RETURN n.name", parameters, {{"Adam"}});
    expectLookedUpRows("MATCH (n) WHERE n IN [1, 'x'] RETURN n.name", parameters, {{"Adam"}});
}
