#include <gtest/gtest.h>

#include <memory>

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

namespace {

// UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN 1 once the scan is fused: the driver is
// i, recomputed from the scanned nodes for the broadcast alone
const char* const elementIDDriverProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %c = db.constant(1 : i64)
  %x = db.broadcast_constant(%c, %id) : (!db.column<i64>, !db.column<none>) -> !db.column<none>
  db.output(%x) : !db.column<none>
  return
}
)mlir";

// A chain of row-wise ops, one of them reading a constant beside the rows
const char* const chainedDriverProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  %thirty = db.constant(30 : i64)
  %older = db.gt %age, %thirty : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>
  %c = db.constant(1 : i64)
  %x = db.broadcast_constant(%c, %older) : (!db.column<i64>, !db.column<!storage.bool>) -> !db.column<none>
  db.output(%x) : !db.column<none>
  return
}
)mlir";

// The driver is read beside the broadcast, so only the broadcast moves
const char* const sharedDriverProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %c = db.constant(1 : i64)
  %x = db.broadcast_constant(%c, %id) : (!db.column<i64>, !db.column<none>) -> !db.column<none>
  db.output(%x, %id) : !db.column<none>, !db.column<none>
  return
}
)mlir";

// A division by a column may raise, so it is kept and the broadcast stays on it
const char* const raisingDriverProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  %ratio = db.div %age, %age : (!db.column<!storage.nullable<i64>>, !db.column<!storage.nullable<i64>>) -> !db.column<none>
  %c = db.constant(1 : i64)
  %x = db.broadcast_constant(%c, %ratio) : (!db.column<i64>, !db.column<none>) -> !db.column<none>
  db.output(%x) : !db.column<none>
  return
}
)mlir";

}

// A broadcast reads only the rows of its driver, so it is driven from the column those rows
// come from, and the row-wise ops that only computed the driver go
class DriveBroadcastsFromRowSourcesTest : public ::testing::Test {
protected:
    DriveBroadcastsFromRowSourcesTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    void runPass(const char* programText, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module);

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createDriveBroadcastsFromRowSources());
        ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));
    }

    static mlir::db::BroadcastConstant findBroadcast(mlir::ModuleOp module) {
        mlir::db::BroadcastConstant found;
        module.walk([&found](mlir::db::BroadcastConstant broadcast) {
            found = broadcast;
        });

        return found;
    }

    template <typename OpType>
    static size_t countOps(mlir::ModuleOp module) {
        size_t count = 0;
        module.walk([&count](OpType) {
            count++;
        });

        return count;
    }

    mlir::MLIRContext _context;
};

TEST_F(DriveBroadcastsFromRowSourcesTest, drivesFromTheScanPastAnElementID) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    runPass(elementIDDriverProgram, module);

    mlir::db::BroadcastConstant broadcast = findBroadcast(*module);
    ASSERT_TRUE(broadcast);
    EXPECT_TRUE(broadcast.getDriver().getDefiningOp<mlir::db::ScanNodes>());
    EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 0u);
}

TEST_F(DriveBroadcastsFromRowSourcesTest, drivesFromTheScanPastAChainOfRowWiseOps) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    runPass(chainedDriverProgram, module);

    mlir::db::BroadcastConstant broadcast = findBroadcast(*module);
    ASSERT_TRUE(broadcast);
    EXPECT_TRUE(broadcast.getDriver().getDefiningOp<mlir::db::ScanNodes>());
    EXPECT_EQ(countOps<mlir::db::GtOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 0u);
}

TEST_F(DriveBroadcastsFromRowSourcesTest, keepsADriverReadElsewhere) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    runPass(sharedDriverProgram, module);

    mlir::db::BroadcastConstant broadcast = findBroadcast(*module);
    ASSERT_TRUE(broadcast);
    EXPECT_TRUE(broadcast.getDriver().getDefiningOp<mlir::db::ScanNodes>());
    EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 1u);
}

TEST_F(DriveBroadcastsFromRowSourcesTest, staysOnADriverThatMayRaise) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    runPass(raisingDriverProgram, module);

    mlir::db::BroadcastConstant broadcast = findBroadcast(*module);
    ASSERT_TRUE(broadcast);
    EXPECT_TRUE(broadcast.getDriver().getDefiningOp<mlir::db::DivOp>());
    EXPECT_EQ(countOps<mlir::db::DivOp>(*module), 1u);
}
