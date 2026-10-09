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

constexpr const char* scanByParameterLabel = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_label([#storage.parameter<"label"> : !storage.label_id]) : !db.column<!storage.node_id>
  db.output(%0) names ["n"] : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* scanByMixedLabels = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person", #storage.parameter<"extra"> : !storage.label_id]) : !db.column<!storage.node_id>
  db.output(%0) names ["n"] : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* constraintByParameterLabel = R"mlir(
func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_label_set(%0) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %2 = db.check_label_constraint(%1, [#storage.parameter<"label"> : !storage.label_id]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %3 = db.filter(%2, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%3) names ["n"] : !db.column<!storage.node_id>
  return
}
)mlir";

}

class LabelParameterBindingTest : public ParameterBindingTest {
};

TEST_F(LabelParameterBindingTest, aStringBindsOneLabel) {
    ParameterMap parameters;
    setString(parameters, "label", "Founder");

    expectRows(scanByParameterLabel, parameters, {{"0"}, {"1"}});
}

TEST_F(LabelParameterBindingTest, aListBindsEveryLabel) {
    ParameterMap parameters;
    const std::vector<std::string_view> labels {"Interest", "Exotic"};
    setStrings(parameters, "label", labels);

    expectRows(scanByParameterLabel, parameters, {{"3"}, {"6"}});
}

TEST_F(LabelParameterBindingTest, aParameterJoinsTheLiteralLabels) {
    ParameterMap parameters;
    setString(parameters, "extra", "SoftwareEngineering");

    expectRows(scanByMixedLabels, parameters, {{"0"}, {"12"}, {"15"}, {"9"}});
}

TEST_F(LabelParameterBindingTest, aLabelConstraintBindsTheParameter) {
    ParameterMap parameters;
    setString(parameters, "label", "Founder");

    expectRows(constraintByParameterLabel, parameters, {{"0"}, {"1"}});
}

TEST_F(LabelParameterBindingTest, anUnknownLabelMatchesNothing) {
    ParameterMap parameters;
    setString(parameters, "label", "Comet");

    expectRows(scanByParameterLabel, parameters, {});
}

TEST_F(LabelParameterBindingTest, anUndefinedParameterIsRejected) {
    const ParameterMap parameters;
    expectError(scanByParameterLabel, &parameters, "Parameter $label is not defined");
}

TEST_F(LabelParameterBindingTest, aProgramWithoutAMapRejectsTheParameter) {
    expectError(scanByParameterLabel, nullptr, "Parameter $label is not defined");
}

TEST_F(LabelParameterBindingTest, anIntegerIsRejected) {
    ParameterMap parameters;
    ParameterValue age;
    age.setInt64(32);
    parameters.set("label", age);

    expectError(scanByParameterLabel, &parameters, "Parameter $label has type Int64: a label is a String or a list of Strings");
}

TEST_F(LabelParameterBindingTest, aNullIsRejected) {
    ParameterMap parameters;
    ParameterValue null;
    null.setNull();
    parameters.set("label", null);

    expectError(scanByParameterLabel, &parameters, "Parameter $label is null: a label is a String or a list of Strings");
}

TEST_F(LabelParameterBindingTest, aListWithANonStringIsRejected) {
    ParameterMap parameters;
    ParameterValue mixed;
    ParameterValue::List& list = mixed.setList();
    list.emplace_back().setString("Person");
    list.emplace_back().setInt64(1);
    parameters.set("label", mixed);

    expectError(scanByParameterLabel, &parameters, "Element 1 of parameter $label has type Int64: a label is a String");
}

TEST_F(LabelParameterBindingTest, anEmptyListIsRejected) {
    ParameterMap parameters;
    ParameterValue none;
    none.setList();
    parameters.set("label", none);

    expectError(scanByParameterLabel, &parameters, "Parameter $label holds no label");
}

TEST_F(LabelParameterBindingTest, anEmptyNameIsRejected) {
    ParameterMap parameters;
    setString(parameters, "label", "");

    expectError(scanByParameterLabel, &parameters, "Parameter $label holds an empty label name");
}
