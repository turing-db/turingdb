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
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

// No query of the other suites reaches nl.not over a constant
constexpr const char* CONSTANT_NOT_PROGRAM = R"mlir(
func.func @main() {
  %true = nl.constant(true)
  %false = nl.constant(false)
  %nullableTrue = nl.to_nullable %true : (!nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  %nullableFalse = nl.to_nullable %false : (!nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  %notTrue = nl.not %true : (!nl.chunk<i1>) -> !nl.chunk<i1>
  %notNullableTrue = nl.not %nullableTrue : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
  %notNullableFalse = nl.not %nullableFalse : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
  %maybe = nl.constant("maybe" : !storage.string)
  %unknown = nl.to_boolean %maybe : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
  %notUnknown = nl.not %unknown : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
  nl.output(%notTrue, %notNullableTrue, %notNullableFalse, %notUnknown) : !nl.chunk<i1>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>
  func.return
}
)mlir";

}

class NullableLogicProgramTest : public TuringTest {
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

TEST_F(NullableLogicProgramTest, negatesConstants) {
    RowSink sink;
    runProgram(CONSTANT_NOT_PROGRAM, sink);

    const Rows expected {{"false", "false", "true", "null"}};
    EXPECT_EQ(sink.rows(), expected);
}

// Of the 10 interests, Eighties is not real and Bio, Cooking and Padel carry no isReal
class NullableLogicTest : public WriteQueryTest {
};

TEST_F(NullableLogicTest, negatesANullableBoolean) {
    expectRows("MATCH (i:Interest) RETURN i.name, NOT i.isReal",
               {
                   {"Computers", "false"},
                   {"Eighties", "true"},
                   {"Bio", "null"},
                   {"Cooking", "null"},
                   {"Ghosts", "false"},
                   {"Padel", "null"},
                   {"Animals", "false"},
                   {"Gym", "false"},
                   {"Travel", "false"},
                   {"JiuJitsu", "false"},
               });
}

TEST_F(NullableLogicTest, negatesANullableBooleanConstant) {
    expectRows("UNWIND [1, 2] AS x RETURN x, NOT toBoolean('maybe'), NOT toBoolean('false')",
               {
                   {"1", "null", "true"},
                   {"2", "null", "true"},
               });
}

TEST_F(NullableLogicTest, negatesTaggedCells) {
    expectRows("UNWIND [true, null, false, 1] AS b WITH b WHERE b IS NULL OR b <> 1 RETURN b, NOT b",
               {
                   {"true", "false"},
                   {"null", "null"},
                   {"false", "true"},
               });
}

TEST_F(NullableLogicTest, fallsThroughALazyCaseOnANullableCondition) {
    expectRows("MATCH (i:Interest) RETURN i.name, CASE WHEN i.isReal THEN 10 / size(i.name) ELSE -1 END",
               {
                   {"Computers", "1"},
                   {"Eighties", "-1"},
                   {"Bio", "-1"},
                   {"Cooking", "-1"},
                   {"Ghosts", "1"},
                   {"Padel", "-1"},
                   {"Animals", "1"},
                   {"Gym", "3"},
                   {"Travel", "1"},
                   {"JiuJitsu", "1"},
               });
}

TEST_F(NullableLogicTest, filtersOnANullableBooleanConstant) {
    expectRows("MATCH (i:Interest) WHERE i.name = 'Gym' OR NOT toBoolean('maybe') RETURN i.name",
               {
                   {"Gym"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
