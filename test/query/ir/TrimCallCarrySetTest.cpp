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

namespace {

template <typename OpType>
llvm::SmallVector<OpType> collect(mlir::ModuleOp module) {
    llvm::SmallVector<OpType> ops;
    module.walk([&](OpType op) {
        ops.push_back(op);
    });

    return ops;
}

}

// A call replicates each carried column once per row the procedure emits for it, so a
// column nothing reads past the call is copied for no reader. The trim cuts it from the
// carry set and leaves the arguments as they were.
class TrimCallCarrySetTest : public ::testing::Test {
protected:
    TrimCallCarrySetTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runTrim(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createTrimUnreadColumns());

        return mlir::succeeded(passManager.run(module));
    }

    mlir::MLIRContext _context;
};

// MATCH (n) WITH n, n.age AS age CALL gnn.neighbourhoodSample(n, 3) YIELD tgt RETURN n, tgt
const char* const callCarryingAnUnreadAge = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %size = db.constant(3 : i64)
  %tgt, %nc, %agec = db.call_procedure("gnn.neighbourhoodSample", {%n, %size}, {%n, %age}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<none>, !db.column<!storage.node_id>, !db.column<i64>)
  db.output(%nc, %tgt) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

TEST_F(TrimCallCarrySetTest, dropsTheCarriedColumnNothingReadsPastTheCall) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(callCarryingAnUnreadAge);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CallProcedure> calls = collect<mlir::db::CallProcedure>(*module);
    ASSERT_EQ(calls.size(), 1u);
    mlir::db::CallProcedure call = calls.front();

    // The arguments stay: the node and the sample size still drive the procedure.
    const mlir::OperandRange inputs = call.getInputs();
    ASSERT_EQ(inputs.size(), 2u);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(inputs[0].getDefiningOp()));
    EXPECT_TRUE(mlir::isa<mlir::db::ConstantOp>(inputs[1].getDefiningOp()));

    // Only the node rides past the call, coming out as the result after the yield.
    const mlir::OperandRange carried = call.getCarriedColumns();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_EQ(carried.front(), inputs[0]);
    EXPECT_EQ(call->getNumResults(), 2u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::OperandRange columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], call->getResult(1));
    EXPECT_EQ(columns[1], call->getResult(0));
}

// MATCH (n) CALL gnn.neighbourhoodSample(n, 3) YIELD tgt RETURN tgt
const char* const callWhoseCarrySetIsUnread = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %size = db.constant(3 : i64)
  %tgt, %nc = db.call_procedure("gnn.neighbourhoodSample", {%n, %size}, {%n}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  db.output(%tgt) : !db.column<none>
  return
}
)mlir";

// The yield is the call's own row set, so unlike a filter or a cut it needs no carried
// column left to be sized by.
TEST_F(TrimCallCarrySetTest, dropsTheWholeCarrySetOfACallNothingReadsPast) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(callWhoseCarrySetIsUnread);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CallProcedure> calls = collect<mlir::db::CallProcedure>(*module);
    ASSERT_EQ(calls.size(), 1u);
    mlir::db::CallProcedure call = calls.front();

    EXPECT_EQ(call.getInputs().size(), 2u);
    EXPECT_EQ(call.getCarriedColumns().size(), 0u);
    EXPECT_EQ(call->getNumResults(), 1u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front(), call->getResult(0));
}

// MATCH (n) WITH n, n.age AS age CALL gnn.neighbourhoodSample(n, 3) YIELD tgt RETURN n, age, tgt
const char* const callWhoseCarrySetIsRead = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %size = db.constant(3 : i64)
  %tgt, %nc, %agec = db.call_procedure("gnn.neighbourhoodSample", {%n, %size}, {%n, %age}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<none>, !db.column<!storage.node_id>, !db.column<i64>)
  db.output(%nc, %agec, %tgt) : !db.column<!storage.node_id>, !db.column<i64>, !db.column<none>
  return
}
)mlir";

TEST_F(TrimCallCarrySetTest, leavesACallWhoseCarrySetIsReadAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(callWhoseCarrySetIsRead);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::CallProcedure> before = collect<mlir::db::CallProcedure>(*module);
    ASSERT_EQ(before.size(), 1u);

    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CallProcedure> after = collect<mlir::db::CallProcedure>(*module);
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after.front(), before.front());
    EXPECT_EQ(after.front().getCarriedColumns().size(), 2u);
    EXPECT_EQ(after.front()->getNumResults(), 3u);
}

// MATCH (n) WITH n, n.age AS age CALL gnn.neighbourhoodSample(n, 3) YIELD tgt AS m
// CALL gnn.neighbourhoodSample(m, 3) YIELD tgt AS k RETURN n, m, k
const char* const chainedCallsCarryingAnUnreadAge = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %size = db.constant(3 : i64)
  %m, %nc, %agec = db.call_procedure("gnn.neighbourhoodSample", {%n, %size}, {%n, %age}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<none>, !db.column<!storage.node_id>, !db.column<i64>)
  %k, %ncc, %mc, %agecc = db.call_procedure("gnn.neighbourhoodSample", {%m, %size}, {%nc, %m, %agec}) yields ["tgt"] : (!db.column<none>, !db.column<i64>, !db.column<!storage.node_id>, !db.column<none>, !db.column<i64>) -> (!db.column<none>, !db.column<!storage.node_id>, !db.column<none>, !db.column<i64>)
  db.output(%ncc, %mc, %k) : !db.column<!storage.node_id>, !db.column<none>, !db.column<none>
  return
}
)mlir";

// The age rides through both calls and is read after neither: trimming the second call
// leaves the first's copy unread, and the same run trims that too.
TEST_F(TrimCallCarrySetTest, trimsAChainOfCallsInOneRun) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(chainedCallsCarryingAnUnreadAge);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CallProcedure> calls = collect<mlir::db::CallProcedure>(*module);
    ASSERT_EQ(calls.size(), 2u);
    mlir::db::CallProcedure firstCall = calls[0];
    mlir::db::CallProcedure secondCall = calls[1];

    ASSERT_EQ(firstCall.getCarriedColumns().size(), 1u);
    EXPECT_EQ(firstCall.getCarriedColumns().front(), firstCall.getInputs().front());
    EXPECT_EQ(firstCall->getNumResults(), 2u);

    const mlir::OperandRange secondCarried = secondCall.getCarriedColumns();
    ASSERT_EQ(secondCarried.size(), 2u);
    EXPECT_EQ(secondCarried[0], firstCall->getResult(1));
    EXPECT_EQ(secondCarried[1], firstCall->getResult(0));
    EXPECT_EQ(secondCall->getNumResults(), 3u);
}
