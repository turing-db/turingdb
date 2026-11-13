#include <gtest/gtest.h>

#include <memory>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "Graph.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "SimpleGraph.h"
#include "StorageDialect.h"
#include "TuringException.h"
#include "TuringTest.h"

#include "IRTestOps.h"
#include "IRTestRows.h"

using namespace db;
using namespace turing::test;

// A pass erasing what a rewrite left unread keeps an op that may raise: its error is still
// owed. A count over the product of two label scans reads the graph's label counts, and the
// product goes with the count, unless a factor divides by zero.
class UnreadRaisingOpEraseTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void countFromMetadata(const char* programText, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module);

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createCountFromMetadata());
        ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

        EXPECT_EQ(countOps<mlir::db::CountScanRows>(*module), 1u);
        EXPECT_EQ(countOps<mlir::db::Count>(*module), 0u);
    }

    void run(mlir::ModuleOp module, RowSink& sink) {
        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();
    }

    std::unique_ptr<Graph> _graph;
    mlir::MLIRContext _context;
};

// MATCH (a:Person), (b:Interest) RETURN count(*), with a factor dividing every age by zero
const char* const productDividingByZero = R"mlir(
func.func @main() {
  %p:3 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
    %zero = db.constant(0 : i64)
    %ratio = db.div %age, %zero : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<none>
    db.yield %a, %ratio : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %n = db.count(%p#0) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// The same product, its factor dividing every age by two
const char* const productDividingByTwo = R"mlir(
func.func @main() {
  %p:3 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
    %two = db.constant(2 : i64)
    %ratio = db.div %age, %two : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<none>
    db.yield %a, %ratio : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %n = db.count(%p#0) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

TEST_F(UnreadRaisingOpEraseTest, keepsAProductWhoseFactorMayRaise) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(countFromMetadata(productDividingByZero, module));

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);

    RowSink sink;
    EXPECT_THROW(run(*module, sink), TuringException);
}

TEST_F(UnreadRaisingOpEraseTest, erasesAProductNoFactorOfWhichMayRaise) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(countFromMetadata(productDividingByTwo, module));

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);

    RowSink sink;
    ASSERT_NO_FATAL_FAILURE(run(*module, sink));
    EXPECT_EQ(sink.rows(), (Rows {{"80"}}));
}
