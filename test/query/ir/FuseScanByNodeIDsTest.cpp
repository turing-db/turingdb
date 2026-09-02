#include <gtest/gtest.h>

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

template <typename OpType>
size_t countOps(mlir::ModuleOp module) {
    return collect<OpType>(module).size();
}

}

class FuseScanByNodeIDsTest : public ::testing::Test {
protected:
    FuseScanByNodeIDsTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runFuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseScanByNodeIDs());

        return mlir::succeeded(passManager.run(module));
    }

    // The production order: a filter sinks to its scan first, then fuses into it.
    bool runPushDownThenFuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createPushDownFilters());
        passManager.addPass(mlir::db::createFuseScanByNodeIDs());

        return mlir::succeeded(passManager.run(module));
    }

    // The module holds exactly its original scan and filter and no const scan.
    void expectUntouched(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 1u);
        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module) + countOps<mlir::db::ScanNodesByLabel>(module), 1u);
    }

    mlir::MLIRContext _context;
};

// MATCH (n) WHERE n = 5 OR n = 2 OR n = 5 RETURN n
const char* const repeatedDisjunction = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k5 = db.constant(5 : i64)
  %e1 = db.eq %n, %k5 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %k2 = db.constant(2 : i64)
  %e2 = db.eq %n, %k2 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %o1 = db.or %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %k5b = db.constant(5 : i64)
  %e3 = db.eq %n, %k5b : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %mask = db.or %o1, %e3 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, fusesDisjunctionIntoSortedUniqueConstScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(repeatedDisjunction);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    // The listed set is the filter's: each node once, in scan order.
    const llvm::ArrayRef<int64_t> nodeIDs = constScan.getNodeIDs();
    const std::vector<int64_t> expected {2, 5};
    EXPECT_EQ(std::vector<int64_t>(nodeIDs.begin(), nodeIDs.end()), expected);

    // Nothing of the scan, the filter or its mask cone survives.
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ConstantOp>(*module), 0u);

    // The output reads the const scan directly.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front().getDefiningOp(), constScan.getOperation());
}

// MATCH (n) WHERE n = 3 RETURN n
const char* const singleEquality = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k3 = db.constant(3 : i64)
  %mask = db.eq %n, %k3 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, fusesSingleEquality) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(singleEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);

    const llvm::ArrayRef<int64_t> nodeIDs = constScans.front().getNodeIDs();
    const std::vector<int64_t> expected {3};
    EXPECT_EQ(std::vector<int64_t>(nodeIDs.begin(), nodeIDs.end()), expected);

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// MATCH (n) WHERE 7 = n OR n = 3 RETURN n
const char* const constantOnEitherSide = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k7 = db.constant(7 : i64)
  %e1 = db.eq %k7, %n : (!db.column<i64>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %k3 = db.constant(3 : i64)
  %e2 = db.eq %n, %k3 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %mask = db.or %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, acceptsConstantOnEitherSide) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(constantOnEitherSide);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);

    const llvm::ArrayRef<int64_t> nodeIDs = constScans.front().getNodeIDs();
    const std::vector<int64_t> expected {3, 7};
    EXPECT_EQ(std::vector<int64_t>(nodeIDs.begin(), nodeIDs.end()), expected);
}

// MATCH (n) WHERE n = 1 OR n.age = 32 RETURN n
const char* const propertyLeaf = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k1 = db.constant(1 : i64)
  %e1 = db.eq %n, %k1 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %k32 = db.constant(32 : i64)
  %e2 = db.eq %age, %k32 : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %mask = db.or %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, leavesPropertyLeafAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertyLeaf);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
}

// MATCH (n) WHERE n = 1 AND n = 2 RETURN n
const char* const conjunction = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k1 = db.constant(1 : i64)
  %e1 = db.eq %n, %k1 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %k2 = db.constant(2 : i64)
  %e2 = db.eq %n, %k2 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %mask = db.and %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, leavesConjunctionAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(conjunction);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
}

// MATCH (n) WHERE n = -1 RETURN n
const char* const negativeLiteral = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k = db.constant(-1 : i64)
  %mask = db.eq %n, %k : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, leavesNegativeLiteralAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(negativeLiteral);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
}

// MATCH (n) WHERE n = true RETURN n
const char* const boolLiteral = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k = db.constant(true)
  %mask = db.eq %n, %k : (!db.column<!storage.node_id>, !db.column<i1>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, leavesBoolLiteralAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(boolLiteral);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
}

// MATCH (n:Person) WHERE n = 3 RETURN n
const char* const labelScan = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %k3 = db.constant(3 : i64)
  %mask = db.eq %n, %k3 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, fusesLabelScanIntoConstScanWithLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    const llvm::ArrayRef<int64_t> nodeIDs = constScan.getNodeIDs();
    const std::vector<int64_t> expected {3};
    EXPECT_EQ(std::vector<int64_t>(nodeIDs.begin(), nodeIDs.end()), expected);

    // The label test the fused scan carried is re-applied over the listed nodes.
    llvm::SmallVector<mlir::db::GetNodeLabelSet> labelSets = collect<mlir::db::GetNodeLabelSet>(*module);
    ASSERT_EQ(labelSets.size(), 1u);
    mlir::db::GetNodeLabelSet labelSet = labelSets.front();
    EXPECT_EQ(labelSet.getInputNodes().getDefiningOp(), constScan.getOperation());

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);
    mlir::db::CheckLabelConstraint check = checks.front();
    EXPECT_EQ(check.getLabelsetIds().getDefiningOp(), labelSet.getOperation());

    const mlir::ArrayAttr labels = check.getLabels();
    ASSERT_EQ(labels.size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Person");

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();
    EXPECT_EQ(filter.getMask().getDefiningOp(), check.getOperation());
    ASSERT_EQ(filter.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(filter.getColumnsToFilter().front().getDefiningOp(), constScan.getOperation());

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front().getDefiningOp(), filter.getOperation());

    // The labelled scan and the ID mask are gone.
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ConstantOp>(*module), 0u);
}

// MATCH (n) WITH n, n.age AS age WHERE n = 3 RETURN n, age
const char* const filterCarryingProperty = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %k3 = db.constant(3 : i64)
  %mask = db.eq %n, %k3 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf, %agef = db.filter(%mask, {%n, %age}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<!storage.node_id>, !db.column<i64>)
  db.output(%nf, %agef) : !db.column<!storage.node_id>, !db.column<i64>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, leavesFilterCarryingAnotherColumnAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterCarryingProperty);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
}

// MATCH (n), (m) WHERE n = 4 OR n = 2 RETURN n, m, once the filter sank into its factor
const char* const disjunctionInFactor = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    %k4 = db.constant(4 : i64)
    %e1 = db.eq %n, %k4 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
    %k2 = db.constant(2 : i64)
    %e2 = db.eq %n, %k2 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
    %mask = db.or %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
    %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.yield %nf : !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, fusesInsideCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(disjunctionInFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    const llvm::ArrayRef<int64_t> nodeIDs = constScan.getNodeIDs();
    const std::vector<int64_t> expected {2, 4};
    EXPECT_EQ(std::vector<int64_t>(nodeIDs.begin(), nodeIDs.end()), expected);

    // The const scan took the filter's place inside the left factor, which now yields it;
    // the right factor's scan is untouched.
    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);
    mlir::db::CrossProduct product = products.front();

    mlir::db::Yield leftYield = mlir::cast<mlir::db::Yield>(product.getLeftFactor().front().getTerminator());
    ASSERT_EQ(leftYield.getColumns().size(), 1u);
    EXPECT_EQ(leftYield.getColumns().front().getDefiningOp(), constScan.getOperation());

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
}

// MATCH (n)-->(m) WHERE n = 1 OR n = 3 RETURN n, m, as codegen emits it: filtered after the hop
const char* const disjunctionAfterHop = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %k1 = db.constant(1 : i64)
  %e1 = db.eq %s, %k1 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %k3 = db.constant(3 : i64)
  %e2 = db.eq %s, %k3 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %mask = db.or %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%mask, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByNodeIDsTest, fusesFilterPushedDownToScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(disjunctionAfterHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDownThenFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    const llvm::ArrayRef<int64_t> nodeIDs = constScan.getNodeIDs();
    const std::vector<int64_t> expected {1, 3};
    EXPECT_EQ(std::vector<int64_t>(nodeIDs.begin(), nodeIDs.end()), expected);

    // The hop now expands the const scan, and no filter is left anywhere.
    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getInputNodes().getDefiningOp(), constScan.getOperation());

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
}
