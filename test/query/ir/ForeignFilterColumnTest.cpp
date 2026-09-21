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

// The filter carries a column the scan never bound. Narrowing the scan shortens the columns
// it did bind, and the carried one is handed back whole, so the two stop lining up row for
// row.
const char* const filterCarriesAColumnTheScanDidNotBind = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %nf = db.filter(%ok, {%s, %n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %nf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Every carried column comes out of the scan, which the pass is free to narrow.
const char* const filterCarriesOnlyTheScansColumns = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class ForeignFilterColumnTest : public TuringTest {
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

TEST_F(ForeignFilterColumnTest, aCarriedColumnFromElsewhereKeepsTheCheckAndTheScansTypes) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterCarriesAColumnTheScanDidNotBind);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runNarrow(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);

    expectScanTypes(*module, {"KNOWS_WELL", "INTERESTED_IN"});
}

TEST_F(ForeignFilterColumnTest, carryingOnlyTheScansOwnColumnsFolds) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterCarriesOnlyTheScansColumns);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runNarrow(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    expectScanTypes(*module, {"KNOWS_WELL"});
}
