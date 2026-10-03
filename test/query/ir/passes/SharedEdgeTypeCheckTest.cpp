#include <gtest/gtest.h>

#include <string>
#include <vector>

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
#include "NLDialect.h"
#include "StorageDialect.h"

#include "TuringTest.h"

#include "IRTestEdgeTypes.h"

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

namespace {

// The mask the filter tests is read a second time. Narrowing the scan would leave that
// second reader testing a column the scan has already cut down to the types the check
// asks for, so every row it sees passes and the rows it disagreed with are gone.
const char* const narrowerCheckReadTwice = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %nok = db.not %ok : (!db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same program with the mask read once, which the pass is free to fold.
const char* const narrowerCheckReadOnce = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class SharedEdgeTypeCheckTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runNarrow(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createNarrowEdgeTypeReads());

        return mlir::succeeded(passManager.run(module));
    }

    void expectScanTypes(mlir::ModuleOp module, const std::vector<std::string>& edgeTypes) {
        llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(module);
        ASSERT_EQ(scans.size(), 1u);

        expectEdgeTypes(scans.front().getEdgeTypes(), edgeTypes);
    }

    mlir::MLIRContext _context;
};

TEST_F(SharedEdgeTypeCheckTest, aMaskReadTwiceKeepsItsCheckAndItsScansTypes) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(narrowerCheckReadTwice);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runNarrow(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::NotOp>(*module), 1u);

    expectScanTypes(*module, {"KNOWS_WELL", "INTERESTED_IN"});
}

TEST_F(SharedEdgeTypeCheckTest, aMaskReadOnceIsFoldedIntoTheScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(narrowerCheckReadOnce);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runNarrow(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    expectScanTypes(*module, {"KNOWS_WELL"});
}
