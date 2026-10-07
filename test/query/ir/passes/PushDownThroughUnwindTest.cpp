#include <gtest/gtest.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

class PushDownThroughUnwindTest : public ::testing::Test {
protected:
    PushDownThroughUnwindTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    void pushDown(const char* programText, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module);

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createPushDownFilters());

        ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));
    }

    mlir::MLIRContext _context;
};

// UNWIND range(1, 3) AS i MATCH (a) WHERE a.age > 30 RETURN a, i
const char* const carriedColumnPredicate = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %first = db.constant(1 : i64)
  %last = db.constant(3 : i64)
  %list = db.range(%first, %last) : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.list<i64>>
  %i, %ac = db.unwind(%list, {%a}) : (!db.column<!storage.list<i64>>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%ac, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %lim = db.constant(30 : i64)
  %mask = db.gt %age, %lim : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %af, %if = db.filter(%mask, {%ac, %i}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%af, %if) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

TEST_F(PushDownThroughUnwindTest, sinksACarriedColumnPredicateBelowTheUnwind) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    pushDown(carriedColumnPredicate, module);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);

    llvm::SmallVector<mlir::db::Unwind> unwinds = collect<mlir::db::Unwind>(*module);
    ASSERT_EQ(unwinds.size(), 1u);
    mlir::db::Unwind unwind = unwinds.front();

    // The unwind repeats the rows the predicate kept rather than every scanned node
    EXPECT_TRUE(filters.front()->isBeforeInBlock(unwind));
    EXPECT_EQ(unwind.getColumnsToFilter().front().getDefiningOp(), filters.front().getOperation());

    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(reads.front().getInputNodes().getDefiningOp()));
}

// UNWIND range(1, 3) AS i MATCH (a) WHERE i > 1 RETURN a, i
const char* const elementPredicate = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %first = db.constant(1 : i64)
  %last = db.constant(3 : i64)
  %list = db.range(%first, %last) : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.list<i64>>
  %i, %ac = db.unwind(%list, {%a}) : (!db.column<!storage.list<i64>>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  %mask = db.gt %i, %first : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %af, %if = db.filter(%mask, {%ac, %i}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%af, %if) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

TEST_F(PushDownThroughUnwindTest, keepsAnElementPredicateAfterTheUnwind) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    pushDown(elementPredicate, module);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);

    llvm::SmallVector<mlir::db::Unwind> unwinds = collect<mlir::db::Unwind>(*module);
    ASSERT_EQ(unwinds.size(), 1u);

    EXPECT_TRUE(unwinds.front()->isBeforeInBlock(filters.front()));
}

// UNWIND range(1, 3) AS i MATCH (a) WHERE a.age = i RETURN a, i
const char* const crossLineagePredicate = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %first = db.constant(1 : i64)
  %last = db.constant(3 : i64)
  %list = db.range(%first, %last) : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.list<i64>>
  %i, %ac = db.unwind(%list, {%a}) : (!db.column<!storage.list<i64>>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%ac, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %mask = db.eq %age, %i : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
  %af, %if = db.filter(%mask, {%ac, %i}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%af, %if) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

TEST_F(PushDownThroughUnwindTest, keepsAPredicateOverTheElementAndACarriedColumn) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    pushDown(crossLineagePredicate, module);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);

    llvm::SmallVector<mlir::db::Unwind> unwinds = collect<mlir::db::Unwind>(*module);
    ASSERT_EQ(unwinds.size(), 1u);

    EXPECT_TRUE(unwinds.front()->isBeforeInBlock(filters.front()));
}
