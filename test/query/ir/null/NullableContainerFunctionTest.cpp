#include <gtest/gtest.h>

#include <memory>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "IRTestRows.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// No query reaches a list function over a nullable list constant: lowering broadcasts it first.
// "x" is not an integer, so %absent is the null one.
constexpr const char* LIST_PROGRAM = R"mlir(
func.func @main() {
  %list = nl.constant([1, 2, 3])
  %present = nl.to_nullable %list : (!nl.chunk<!storage.list<i64>>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
  %text = nl.constant("x" : !storage.string)
  %absentInteger = nl.to_integer %text : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i64>>
  %held = nl.is_not_null %absentInteger : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.bool>
  %heldRows = nl.broadcast_constant %held : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
  %absentIntegerRows = nl.broadcast_constant %absentInteger : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<i64>>
  %tailRows = nl.make_list(%absentIntegerRows) : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.list<i64>>
  %tail = nl.row_constant %tailRows : <!storage.list<i64>>
  %heldTail = nl.broadcast_constant %tail, %heldRows : (!nl.chunk<!storage.list<i64>>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.list<i64>>
  %tailOrNull = nl.case({%heldRows}, {%heldTail}) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.list<i64>>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
  %absentTail = nl.row_constant %tailOrNull : <!storage.nullable<!storage.list<i64>>>
  %absent = nl.concat %list, %absentTail : (!nl.chunk<!storage.list<i64>>, !nl.chunk<!storage.nullable<!storage.list<i64>>>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
  %zero = nl.constant(0 : i64)
  %two = nl.constant(2 : i64)
  %from = nl.broadcast_constant %zero : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  %to = nl.broadcast_constant %two : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  %presentSlice = nl.list_slice %present, from %from, to %to : (!nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
  %absentSlice = nl.list_slice %absent, from %from, to %to : (!nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
  %half = nl.constant(5.000000e-01 : f64)
  %fraction = nl.broadcast_constant %half : (!nl.chunk<f64>) -> !nl.chunk<!storage.nullable<f64>>
  %presentMedian = nl.percentile_cont %present, %fraction : (!nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.nullable<f64>>) -> !nl.chunk<!storage.nullable<f64>>
  %absentMedian = nl.percentile_cont %absent, %fraction : (!nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.nullable<f64>>) -> !nl.chunk<!storage.nullable<f64>>
  %index = nl.to_any %from : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.any>
  %presentFirst = nl.container_index %present, %index : (!nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.any>) -> !nl.chunk<!storage.nullable<i64>>
  %absentFirst = nl.container_index %absent, %index : (!nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.any>) -> !nl.chunk<!storage.nullable<i64>>
  %absentRows = nl.broadcast_constant %absent : (!nl.chunk<!storage.nullable<!storage.list<i64>>>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
  %absentRowFirst = nl.container_index %absentRows, %index : (!nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.any>) -> !nl.chunk<!storage.nullable<i64>>
  nl.output(%presentSlice, %absentSlice, %presentMedian, %absentMedian, %presentFirst, %absentFirst, %absentRowFirst) : !nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.nullable<!storage.list<i64>>>, !nl.chunk<!storage.nullable<f64>>, !nl.chunk<!storage.nullable<f64>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
  func.return
}
)mlir";

// The map sibling: a key read and a map merge over a nullable map constant
constexpr const char* MAP_PROGRAM = R"mlir(
func.func @main() {
  %one = nl.constant(1 : i64)
  %oneRows = nl.broadcast_constant %one : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  %mapRows = nl.make_map(%oneRows) keys ["a"] : !nl.chunk<!storage.nullable<i64>>
  %map = nl.row_constant %mapRows : <!storage.map>
  %present = nl.to_nullable %map : (!nl.chunk<!storage.map>) -> !nl.chunk<!storage.nullable<!storage.map>>
  %text = nl.constant("x" : !storage.string)
  %absentInteger = nl.to_integer %text : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i64>>
  %held = nl.is_not_null %absentInteger : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.bool>
  %heldRows = nl.broadcast_constant %held : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
  %heldMap = nl.broadcast_constant %map, %heldRows : (!nl.chunk<!storage.map>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.map>
  %mapOrNull = nl.case({%heldRows}, {%heldMap}) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.map>) -> !nl.chunk<!storage.nullable<!storage.map>>
  %absent = nl.row_constant %mapOrNull : <!storage.nullable<!storage.map>>
  %presentA = nl.static_map_key %present key "a" : (!nl.chunk<!storage.nullable<!storage.map>>) -> !nl.chunk<!storage.any>
  %absentA = nl.static_map_key %absent key "a" : (!nl.chunk<!storage.nullable<!storage.map>>) -> !nl.chunk<!storage.any>
  %presentMerged = nl.merge_maps(%present, %map) : (!nl.chunk<!storage.nullable<!storage.map>>, !nl.chunk<!storage.map>) -> !nl.chunk<!storage.nullable<!storage.map>>
  %absentMerged = nl.merge_maps(%absent, %map) : (!nl.chunk<!storage.nullable<!storage.map>>, !nl.chunk<!storage.map>) -> !nl.chunk<!storage.nullable<!storage.map>>
  nl.output(%presentA, %absentA, %presentMerged, %absentMerged) : !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.nullable<!storage.map>>, !nl.chunk<!storage.nullable<!storage.map>>
  func.return
}
)mlir";

}

class NullableContainerFunctionTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void runProgram(const char* programText, NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        LocalMemory memory;
        NLInterpreter interpreter(*module, &reader.getView(), &sink, &memory);
        interpreter.run();
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(NullableContainerFunctionTest, readsNullableListConstants) {
    RowSink sink;
    runProgram(LIST_PROGRAM, sink);

    const Rows expected {{"[1, 2]", "null", "2.000000", "null", "1", "null", "null"}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NullableContainerFunctionTest, readsNullableMapConstants) {
    RowSink sink;
    runProgram(MAP_PROGRAM, sink);

    const Rows expected {{"1", "null", "{a: 1}", "null"}};
    EXPECT_EQ(sink.rows(), expected);
}
