#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Lowering lays every constant out over one row before a string or point function reads it,
// so these programs hand the functions the nullable constants a conversion or a
// nl.row_constant answers: toString(toInteger('123')), toString(toInteger('x')) and the rest
const char* const nullableConstantArgumentProgram = R"mlir(
func.func @main() {
  %text = nl.constant("123" : !storage.string)
  %bad = nl.constant("x" : !storage.string)
  %digit = nl.constant("1" : !storage.string)
  %word = nl.constant("a1b" : !storage.string)
  %one = nl.constant(1 : i64)
  %origin = nl.constant({x = 1.000000e+00 : f64, y = 1.000000e+00 : f64})
  %map = nl.constant({"1" = 7 : i64})

  %start = nl.broadcast_constant %one : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  %words = nl.broadcast_constant %word : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<!storage.string>>

  %number = nl.to_integer %text : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i64>>
  %string = nl.to_string %number : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %noNumber = nl.to_integer %bad : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i64>>
  %noString = nl.to_string %noNumber : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %digitNumber = nl.to_integer %digit : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i64>>
  %delimiter = nl.to_string %digitNumber : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>

  %sub = nl.substring %string, %start : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %noSub = nl.substring %noString, %start : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %parts = nl.split %words, %delimiter : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>
  %noParts = nl.split %words, %noString : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>

  %partList = nl.row_constant %parts : <!storage.nullable<!storage.list<!storage.string>>>
  %splitOnList = nl.split %words, %partList : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>

  %value = nl.dynamic_map_key %map, %delimiter : (!nl.chunk<!storage.map>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.any>

  %origins = nl.broadcast_constant %origin : (!nl.chunk<!storage.map>) -> !nl.chunk<!storage.map>
  %points = nl.to_point %origins : (!nl.chunk<!storage.map>) -> !nl.chunk<!storage.nullable<!storage.point>>
  %point = nl.row_constant %points : <!storage.nullable<!storage.point>>
  %distance = nl.point_distance %point, %points : (!nl.chunk<!storage.nullable<!storage.point>>, !nl.chunk<!storage.nullable<!storage.point>>) -> !nl.chunk<!storage.nullable<f64>>
  %inBox = nl.point_within_bbox %point, %points, %points : (!nl.chunk<!storage.nullable<!storage.point>>, !nl.chunk<!storage.nullable<!storage.point>>, !nl.chunk<!storage.nullable<!storage.point>>) -> !nl.chunk<!storage.nullable<i1>>

  %left = nl.left %string, %start : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %right = nl.right %string, %start : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %noLeft = nl.left %noString, %start : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %replaced = nl.replace %string, %delimiter, %words : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %noReplaced = nl.replace %noString, %delimiter, %words : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>

  nl.output(%sub, %noSub, %parts, %noParts, %splitOnList, %value, %distance, %inBox, %left, %right, %noLeft, %replaced, %noReplaced) names ["sub", "noSub", "parts", "noParts", "splitOnList", "value", "distance", "inBox", "left", "right", "noLeft", "replaced", "noReplaced"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>, !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>, !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>, !nl.chunk<!storage.any>, !nl.chunk<!storage.nullable<f64>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
  func.return
}
)mlir";

class NullableConstantFunctionArgumentTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void runProgram(const char* programText, RowSink& sink) {
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

}

TEST_F(NullableConstantFunctionArgumentTest, functionsReadNullableConstantArguments) {
    RowSink sink;
    runProgram(nullableConstantArgumentProgram, sink);

    const Rows& rows = sink.rows();
    std::string text;
    describeRows(rows, text);

    const Rows expected {{"23", "null", "[a, b]", "null", "[, 1, ]", "7", "0.000000", "true", "1", "3", "null", "a1b23", "null"}};
    EXPECT_EQ(rows, expected) << text;
}
