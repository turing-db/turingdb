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

constexpr const char* orOfANameAndAParameter = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %k = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %p = db.check_edge_type_constraint(%et, [#storage.parameter<"type"> : !storage.edge_type_id]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ok = db.or %k, %p : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* andOfANameAndAParameter = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %k = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %p = db.check_edge_type_constraint(%et, [#storage.parameter<"type"> : !storage.edge_type_id]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ok = db.and %k, %p : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* parameterCheckOverATypedScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, [#storage.parameter<"type"> : !storage.edge_type_id]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* parameterCheckImpliedByAParameterScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type([#storage.parameter<"type"> : !storage.edge_type_id]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, [#storage.parameter<"type"> : !storage.edge_type_id, "KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* literalCheckOverAParameterScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type([#storage.parameter<"type"> : !storage.edge_type_id]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class EdgeTypeParameterPassTest : public ::testing::Test {
protected:
    EdgeTypeParameterPassTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parseAndRun(const char* programText, std::unique_ptr<mlir::Pass> pass) {
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module);
        if (!module) {
            return module;
        }

        mlir::PassManager passManager(&_context);
        passManager.addPass(std::move(pass));
        EXPECT_TRUE(mlir::succeeded(passManager.run(*module)));
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

        return module;
    }

    mlir::MLIRContext _context;
};

TEST_F(EdgeTypeParameterPassTest, anOrWithAParameterFusesIntoOneCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRun(orOfANameAndAParameter, mlir::db::createFuseEdgeTypePredicates());
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::CheckEdgeTypeConstraint> checks = collect<mlir::db::CheckEdgeTypeConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);
    EXPECT_EQ(checks.front().getEdgeTypes().size(), 2u);
}

TEST_F(EdgeTypeParameterPassTest, anAndWithAParameterStaysApart) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRun(andOfANameAndAParameter, mlir::db::createFuseEdgeTypePredicates());
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::AndOp>(*module), 1u);
}

TEST_F(EdgeTypeParameterPassTest, aParameterCheckDoesNotNarrowTheRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRun(parameterCheckOverATypedScan, mlir::db::createNarrowEdgeTypeReads());
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 1u);

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_EQ(scans.front().getEdgeTypes().size(), 2u);
}

TEST_F(EdgeTypeParameterPassTest, aCheckAllowingEveryTypeTheReadAsksForIsDropped) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRun(parameterCheckImpliedByAParameterScan, mlir::db::createNarrowEdgeTypeReads());
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_EQ(scans.front().getEdgeTypes().size(), 1u);
}

TEST_F(EdgeTypeParameterPassTest, aLiteralCheckOverAParameterReadStays) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRun(literalCheckOverAParameterScan, mlir::db::createNarrowEdgeTypeReads());
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}
