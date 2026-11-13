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

// Cypher folds a function over the null literal before it reaches nl, so only an nl program
// hands a function the untyped null constant.
constexpr const char* PROGRAM = R"mlir(
func.func @main() {
  %null = nl.constant("" : !storage.nullable<none>)
  %integer = nl.to_integer %null : (!nl.chunk<!storage.nullable<none>>) -> !nl.chunk<!storage.nullable<i64>>
  nl.output(%integer) : !nl.chunk<!storage.nullable<i64>>
  func.return
}
)mlir";

}

class UntypedNullFunctionArgumentTest : public TuringTest {
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

TEST_F(UntypedNullFunctionArgumentTest, answersNull) {
    RowSink sink;
    runProgram(PROGRAM, sink);

    const Rows expected {{"null"}};
    EXPECT_EQ(sink.rows(), expected);
}
