#include <gtest/gtest.h>

#include <span>
#include <string_view>
#include <vector>

#include "ParameterMap.h"

#include "ParameterBindingTest.h"
#include "ParameterTestHelpers.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* scanByParameterType = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type([#storage.parameter<"type"> : !storage.edge_type_id]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* checkByParameterType = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, [#storage.parameter<"type"> : !storage.edge_type_id]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// The fixture has 15 INTERESTED_IN edges and 3 KNOWS_WELL edges
class EdgeTypeParameterBindingTest : public ParameterBindingTest {
};

TEST_F(EdgeTypeParameterBindingTest, aStringBindsOneType) {
    ParameterMap parameters;
    setString(parameters, "type", "KNOWS_WELL");

    expectRowCount(scanByParameterType, parameters, 3);
}

TEST_F(EdgeTypeParameterBindingTest, aListBindsADisjunction) {
    ParameterMap parameters;
    const std::vector<std::string_view> types {"KNOWS_WELL", "INTERESTED_IN"};
    setStrings(parameters, "type", types);

    expectRowCount(scanByParameterType, parameters, 18);
}

TEST_F(EdgeTypeParameterBindingTest, aTypeCheckBindsTheParameter) {
    ParameterMap parameters;
    setString(parameters, "type", "INTERESTED_IN");

    expectRowCount(checkByParameterType, parameters, 15);
}

TEST_F(EdgeTypeParameterBindingTest, anUnknownTypeMatchesNothing) {
    ParameterMap parameters;
    setString(parameters, "type", "DISLIKES");

    expectRowCount(scanByParameterType, parameters, 0);
}

TEST_F(EdgeTypeParameterBindingTest, anUndefinedParameterIsRejected) {
    expectError(scanByParameterType, nullptr, "Parameter $type is not defined");
}

TEST_F(EdgeTypeParameterBindingTest, anIntegerIsRejected) {
    ParameterMap parameters;
    ParameterValue number;
    number.setInt64(1);
    parameters.set("type", number);

    expectError(scanByParameterType, &parameters, "Parameter $type has type Int64: an edge type is a String or a list of Strings");
}

TEST_F(EdgeTypeParameterBindingTest, anEmptyListIsRejected) {
    ParameterMap parameters;
    ParameterValue none;
    none.setList();
    parameters.set("type", none);

    expectError(scanByParameterType, &parameters, "Parameter $type holds no edge type");
}

TEST_F(EdgeTypeParameterBindingTest, anEmptyNameIsRejected) {
    ParameterMap parameters;
    setString(parameters, "type", "");

    expectError(scanByParameterType, &parameters, "Parameter $type holds an empty edge type name");
}
