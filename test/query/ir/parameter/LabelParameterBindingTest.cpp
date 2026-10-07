#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "CompilerException.h"
#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "StorageDialect.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "SimpleGraph.h"
#include "StringRowSink.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

void setString(ParameterMap& parameters, std::string_view name, std::string_view value) {
    ParameterValue parameter;
    parameter.setString(value);
    parameters.set(name, parameter);
}

void setStrings(ParameterMap& parameters, std::string_view name, std::span<const std::string_view> values) {
    ParameterValue parameter;
    ParameterValue::List& list = parameter.setList();
    for (const std::string_view value : values) {
        list.emplace_back().setString(value);
    }

    parameters.set(name, parameter);
}

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

// Runs a db program parsed from text on the SimpleGraph fixture, with the parameter map
// the translator binds the program's parameters from.
class LabelParameterBindingTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void execute(const char* programText, const ParameterMap* parameters, StringRowSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        LocalMemory memory;
        DBDialectInterpreter interpreter(*module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE, nullptr, nullptr, nullptr, nullptr, parameters);
        interpreter.run();
    }

    void expectRows(const char* programText, const ParameterMap& parameters, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        execute(programText, &parameters, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected);
    }

    void expectError(const char* programText, const ParameterMap* parameters, std::string_view reason) {
        StringRowSink sink;
        std::string error;

        try {
            execute(programText, parameters, sink);
        } catch (const CompilerException& e) {
            error = e.what();
        }

        ASSERT_FALSE(error.empty()) << "accepted: " << programText;
        EXPECT_NE(error.find(reason), std::string::npos) << error;
    }

private:
    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
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
