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

namespace {

// The check names the label the scan guarantees: redundant, and removed
constexpr const char* literalCheckOverLiteralScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* parameterCheckOverLiteralScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, [#storage.parameter<"label"> : !storage.label_id]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* literalCheckOverParameterScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label([#storage.parameter<"label"> : !storage.label_id]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* parameterCheckSharedByAlternativesOverLiteralScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, [["Person", #storage.parameter<"label"> : !storage.label_id], ["Founder", #storage.parameter<"label"> : !storage.label_id]]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* parameterCheckOverTheSameParameterScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label([#storage.parameter<"label"> : !storage.label_id]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, [#storage.parameter<"label"> : !storage.label_id]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* parameterCheckOverAnotherParameterScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label([#storage.parameter<"first"> : !storage.label_id]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, [#storage.parameter<"second"> : !storage.label_id]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af) : !db.column<!storage.node_id>
  return
}
)mlir";

}

class LabelParameterPassTest : public ::testing::Test {
protected:
    LabelParameterPassTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parseAndRemove(const char* programText) {
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module);
        if (!module) {
            return module;
        }

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createRemoveRedundantLabelChecks());
        EXPECT_TRUE(mlir::succeeded(passManager.run(*module)));
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

        return module;
    }

    mlir::MLIRContext _context;
};

TEST_F(LabelParameterPassTest, aLiteralCheckTheScanGuaranteesIsRemoved) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(literalCheckOverLiteralScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
}

TEST_F(LabelParameterPassTest, aParameterCheckOverALiteralScanStays) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(parameterCheckOverLiteralScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(LabelParameterPassTest, aLiteralCheckOverAParameterScanStays) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(literalCheckOverParameterScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(LabelParameterPassTest, aParameterSharedByEveryAlternativeStays) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(parameterCheckSharedByAlternativesOverLiteralScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(LabelParameterPassTest, aParameterCheckOverTheSameParameterScanIsRemoved) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(parameterCheckOverTheSameParameterScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(LabelParameterPassTest, aParameterCheckOverAnotherParameterScanStays) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(parameterCheckOverAnotherParameterScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}
