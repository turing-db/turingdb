#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "IRTestRows.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "Graph.h"
#include "LocalMemory.h"
#include "SimpleGraph.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

const char* const comparisonProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
  %null = db.constant("" : !storage.nullable<none>)
  %x = db.broadcast_constant(%null, %n) : (!db.column<!storage.nullable<none>>, !db.column<!storage.node_id>) -> !db.column<!storage.nullable<none>>
  %a = db.constant("a" : !storage.string)
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %below = db.lt %x, %a : (!db.column<!storage.nullable<none>>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %above = db.gt %name, %x : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<none>>) -> !db.column<!storage.bool>
  db.output(%below, %above) : !db.column<!storage.bool>, !db.column<!storage.bool>
  return
}
)mlir";

const char* const stringProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
  %null = db.constant("" : !storage.nullable<none>)
  %x = db.broadcast_constant(%null, %n) : (!db.column<!storage.nullable<none>>, !db.column<!storage.node_id>) -> !db.column<!storage.nullable<none>>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %starts = db.starts_with %x, %name : (!db.column<!storage.nullable<none>>, !db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.bool>
  %joined = db.concat %name, %x : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<none>>) -> !db.column<none>
  db.output(%starts, %joined) : !db.column<!storage.bool>, !db.column<none>
  return
}
)mlir";

}

// The null literal laid out over rows holds no value a kernel can read, so every binary op
// reads it as the null of the type on its other side. simpledb has 10 interests.
class LaidOutNullOperandTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    void runProgram(const char* programText, size_t chunkSize, RowSink& sink) {
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(programText);
        ASSERT_TRUE(dbModule);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));

        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    void expectRows(const char* programText, const Rows& expected) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink sink;
            runProgram(programText, chunkSize, sink);

            EXPECT_EQ(sink.rows(), expected) << "chunk size " << chunkSize;
        }
    }

    mlir::MLIRContext _context;
    std::unique_ptr<Graph> _graph;
};

TEST_F(LaidOutNullOperandTest, comparesAsANullOfTheOtherOperandsType) {
    expectRows(comparisonProgram, Rows(10, Row {"null", "null"}));
}

TEST_F(LaidOutNullOperandTest, testsAndJoinsTextAsANullString) {
    expectRows(stringProgram, Rows(10, Row {"null", "null"}));
}
