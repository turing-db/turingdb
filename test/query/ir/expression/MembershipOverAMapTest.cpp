#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::array<std::string_view, 16> SEARCHES_OF_A_MAP {
    "RETURN 1 IN {a: 1}",
    "RETURN 'a' IN {a: 1}",
    "RETURN null IN {a: 1}",
    "RETURN 1 IN {}",
    "WITH {a: 1} AS m RETURN 1 IN m",
    "MATCH (n:Person) RETURN 1 IN n.attrs",
    "MATCH (n:Person) WHERE 1 IN n.attrs RETURN n.name",
    "MATCH (n:Person) RETURN 'Remy' IN properties(n)",
    "MATCH (n:Person) RETURN 'Remy' IN n {.name}",
    "RETURN 1 IN [{a: 1}][0]",
    "UNWIND [{a: 1}] AS m RETURN 1 IN m",
    "RETURN 1 IN coalesce(null, {a: 1})",
    "RETURN 1 IN CASE WHEN true THEN {a: 1} END",
    "RETURN 1 IN $map",
    "RETURN 1 IN $maps[0]",
    "UNWIND $maps AS m RETURN 1 IN m",
};

}

class MembershipOverAMapTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (n {name: 'Remy'}) SET n.attrs = {a: 1, xs: [1, 2], inner: {b: 1}}");

        _parameters.insert("map").setMap()["a"].setInt64(1);
        _parameters.insert("maps").setList().emplace_back().setMap()["a"].setInt64(1);
        _parameters.insert("none").setNull();
    }

    QueryStatus runWithParameters(std::string_view query, RowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink,
                              &_parameters);

        return status;
    }

    void expectRejected(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runWithParameters(query, sink);
        const std::string error = status.getError();

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << "query: " << query << "\nerror: " << error;
        EXPECT_NE(error.find("IN operand must be a list, not 'Map'"), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    void expectRow(std::string_view query, const Row& expected) {
        RowSink sink;
        const QueryStatus status = runWithParameters(query, sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows expectedRows {expected};
        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expectedRows) << "query: " << query << "\ngot:\n" << actualText;
    }

    ParameterMap _parameters;
};

TEST_F(MembershipOverAMapTest, rejectsAMapOnTheRight) {
    for (const std::string_view query : SEARCHES_OF_A_MAP) {
        expectRejected(query);
    }
}

TEST_F(MembershipOverAMapTest, searchesNullAsNull) {
    expectRow("RETURN 1 IN null, {a: 1} IN null, 1 IN $none", {"null", "null", "null"});
}

TEST_F(MembershipOverAMapTest, searchesAListOfMapsForAMap) {
    expectRow("RETURN {a: 1} IN [{a: 1}, {a: 2}], {a: 3} IN [{a: 1}], {a: 1} IN $maps", {"true", "false", "true"});
}

TEST_F(MembershipOverAMapTest, searchesACellAsTheListItsRowHolds) {
    expectRows("UNWIND [[1, 2], {a: 1}, 'a'] AS x RETURN 1 IN x", {{"false"}, {"false"}, {"true"}});
    expectRow("MATCH (n {name: 'Remy'}) RETURN 1 IN n.attrs.xs, 1 IN n.attrs.inner, {b: 1} IN n.attrs.inner", {"true", "false", "true"});
    expectRow("RETURN 1 IN $map.a", {"true"});
}
