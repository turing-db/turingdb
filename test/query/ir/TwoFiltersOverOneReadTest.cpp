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

// Two filters read one scan. The first repeats the scan's own types, so folding it away
// changes no row - but it takes the first filter's users off the scan, and the second
// filter asks for less than the scan carries. Narrowing the scan for the second would take
// the INTERESTED_IN rows away from whoever reads the first.
const char* const twoFiltersOverOneScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok1 = db.check_edge_type_constraint(%et, ["KNOWS_WELL", "INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %s1, %t1 = db.filter(%ok1, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %ok2 = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %s2, %t2 = db.filter(%ok2, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s1, %t1, %s2, %t2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class TwoFiltersOverOneReadTest : public TuringTest {
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

    mlir::MLIRContext _context;
};

TEST_F(TwoFiltersOverOneReadTest, aSecondFilterDoesNotNarrowTheScanTheFirstStillReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoFiltersOverOneScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runNarrow(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    expectEdgeTypes(scans.front().getEdgeTypes(), {"KNOWS_WELL", "INTERESTED_IN"});

    // The narrower predicate is no longer carried by the scan, so something has to still
    // apply it.
    EXPECT_GE(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 1u);
    EXPECT_GE(countOps<mlir::db::FilterOp>(*module), 1u);
}
