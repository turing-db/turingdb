#include <gtest/gtest.h>

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

constexpr std::string_view SEARCHES_A_MAP = "IN operand must be a list, not 'Map'";

}

class MembershipOperandTypeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void bindMap(ParameterMap& parameters) {
        parameters.insert("x").setMap()["a"].setInt64(1);
    }
};

TEST_F(MembershipOperandTypeTest, aMapLiteralIsNoListToSearch) {
    runQueryExpectingError("RETURN 1 IN {a: 1}", SEARCHES_A_MAP);
    runQueryExpectingError("RETURN null IN {a: 1}", SEARCHES_A_MAP);
    runQueryExpectingError("MATCH (n) WHERE n.name IN {a: 1} RETURN n.name", SEARCHES_A_MAP);
}

TEST_F(MembershipOperandTypeTest, aMapParameterIsNoListToSearch) {
    ParameterMap parameters;
    bindMap(parameters);

    runQueryExpectingError("MATCH (n) WHERE id(n) IN $x RETURN n.name", parameters, SEARCHES_A_MAP);
    runQueryExpectingError("RETURN 1 IN $x", parameters, SEARCHES_A_MAP);
    runQueryExpectingError("WITH $x AS m RETURN 1 IN m", parameters, SEARCHES_A_MAP);
}

TEST_F(MembershipOperandTypeTest, aComputedMapIsNoListToSearch) {
    runQueryExpectingError("WITH {a: 1} AS m RETURN 1 IN m", SEARCHES_A_MAP);
    runQueryExpectingError("MATCH (n:Person) RETURN 1 IN properties(n)", SEARCHES_A_MAP);
    runQueryExpectingError("MATCH (n:Person) RETURN n.name IN n {.name}", SEARCHES_A_MAP);
}

TEST_F(MembershipOperandTypeTest, aMapPropertyIsNoListToSearch) {
    runWrite("CREATE (:Box {name: 'b', m: {a: 1}})");

    runQueryExpectingError("MATCH (n:Box) RETURN 1 IN n.m", SEARCHES_A_MAP);
}

TEST_F(MembershipOperandTypeTest, aScalarParameterIsNoListToSearch) {
    ParameterMap parameters;
    parameters.insert("s").setString("abc");
    parameters.insert("i").setInt64(5);

    runQueryExpectingError("RETURN 1 IN $s", parameters, "IN operand must be a list, not 'String'");
    runQueryExpectingError("RETURN 1 IN $i", parameters, "IN operand must be a list, not 'Integer'");
}

TEST_F(MembershipOperandTypeTest, aListParameterIsSearched) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(0);
    ids.emplace_back().setInt64(1);
    parameters.insert("i").setInt64(5);
    parameters.insert("none").setNull();
    parameters.insert("ms").setList().emplace_back().setMap()["a"].setInt64(1);
    bindMap(parameters);

    expectRows("MATCH (n) WHERE id(n) IN $ids RETURN n.name", parameters, {{"Adam"}, {"Remy"}});
    expectRows("RETURN null IN $ids, $none IN $ids, 1 IN $none", parameters, {{"null", "null", "null"}});
    expectRows("RETURN $i IN [1, 5], $i IN $ids", parameters, {{"true", "false"}});
    expectRows("RETURN $x IN [{a: 1}], $x IN $ms", parameters, {{"true", "true"}});
}

TEST_F(MembershipOperandTypeTest, aCellHoldingAListIsSearched) {
    ParameterMap parameters;
    parameters.insert("x").setMap()["a"].setList().emplace_back().setInt64(1);

    expectRows("RETURN 1 IN {a: [1]}.a, 1 IN $x.a, 2 IN $x.a", parameters, {{"true", "true", "false"}});
}

TEST_F(MembershipOperandTypeTest, aScalarParameterUnwindsToOneRow) {
    ParameterMap parameters;
    parameters.insert("i").setInt64(5);

    expectRows("UNWIND $i AS x RETURN x", parameters, {{"5"}});
}
