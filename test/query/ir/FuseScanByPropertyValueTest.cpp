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
#include "StorageTypes.h"

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

class FuseScanByPropertyValueTest : public ::testing::Test {
protected:
    FuseScanByPropertyValueTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runFuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseScanByPropertyValue());

        return mlir::succeeded(passManager.run(module));
    }

    // The production order: a filter sinks to its scan first, then fuses into it.
    bool runPushDownThenFuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createPushDownFilters());
        passManager.addPass(mlir::db::createFuseScanByPropertyValue());

        return mlir::succeeded(passManager.run(module));
    }

    // The module holds exactly one fused scan and nothing of the scan-read-compare-filter chain.
    mlir::db::ScanNodesByPropertyValue expectSoleFusedScan(mlir::ModuleOp module) {
        llvm::SmallVector<mlir::db::ScanNodesByPropertyValue> scans = collect<mlir::db::ScanNodesByPropertyValue>(module);
        EXPECT_EQ(scans.size(), 1u);

        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::EqOp>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ConstantOp>(module), 0u);

        return scans.empty() ? mlir::db::ScanNodesByPropertyValue() : scans.front();
    }

    // The module holds exactly its original scan and filter and no fused scan.
    void expectUntouched(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 1u);
        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module) + countOps<mlir::db::ScanNodesByLabel>(module), 1u);
    }

    mlir::MLIRContext _context;
};

// MATCH (n) WHERE n.age = 32 RETURN n
const char* const integerEquality = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(32 : i64)
  %mask = db.eq %age, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, fusesIntegerEquality) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(integerEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module);
    ASSERT_TRUE(scan);
    EXPECT_EQ(scan.getProperty(), "age");

    const mlir::IntegerAttr literal = mlir::dyn_cast<mlir::IntegerAttr>(scan.getValue());
    ASSERT_TRUE(literal);
    EXPECT_TRUE(literal.getType().isSignlessInteger(64));
    EXPECT_EQ(literal.getInt(), 32);

    // The output now reads the fused scan directly; the filter is gone.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front().getDefiningOp(), scan.getOperation());
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// MATCH (n) WHERE 32 = n.age RETURN n
const char* const constantOnTheLeft = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k = db.constant(32 : i64)
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %mask = db.eq %k, %age : (!db.column<i64>, !db.column<none>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, acceptsConstantOnEitherSide) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(constantOnTheLeft);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module);
    ASSERT_TRUE(scan);
    EXPECT_EQ(scan.getProperty(), "age");
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// MATCH (n) WHERE n.name = 'Remy' RETURN n
const char* const stringEquality = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant("Remy" : !storage.string)
  %mask = db.eq %name, %k : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, fusesStringEquality) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(stringEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module);
    ASSERT_TRUE(scan);
    EXPECT_EQ(scan.getProperty(), "name");

    const mlir::StringAttr literal = mlir::dyn_cast<mlir::StringAttr>(scan.getValue());
    ASSERT_TRUE(literal);
    EXPECT_EQ(literal.getValue(), "Remy");
    EXPECT_TRUE(mlir::isa<mlir::storage::StringType>(literal.getType()));
}

// MATCH (n) WHERE n.ratio = 0.5 RETURN n
const char* const doubleEquality = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ratio = db.get_node_properties(%n, "ratio") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(5.000000e-01 : f64)
  %mask = db.eq %ratio, %k : (!db.column<none>, !db.column<f64>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, fusesDoubleEquality) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(doubleEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module);
    ASSERT_TRUE(scan);

    const mlir::FloatAttr literal = mlir::dyn_cast<mlir::FloatAttr>(scan.getValue());
    ASSERT_TRUE(literal);
    EXPECT_DOUBLE_EQ(literal.getValueAsDouble(), 0.5);
}

// MATCH (n) WHERE n.isFrench = true RETURN n
const char* const boolEquality = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %french = db.get_node_properties(%n, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(true)
  %mask = db.eq %french, %k : (!db.column<none>, !db.column<i1>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, fusesBoolEquality) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(boolEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module);
    ASSERT_TRUE(scan);

    const mlir::BoolAttr literal = mlir::dyn_cast<mlir::BoolAttr>(scan.getValue());
    ASSERT_TRUE(literal);
    EXPECT_TRUE(literal.getValue());
}

// MATCH (n) WHERE n.isFrench = n.hasPhD RETURN n
const char* const propertyAgainstProperty = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %french = db.get_node_properties(%n, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
  %phd = db.get_node_properties(%n, "hasPhD") : (!db.column<!storage.node_id>) -> !db.column<none>
  %mask = db.eq %french, %phd : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, leavesPropertyAgainstPropertyAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertyAgainstProperty);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

// MATCH (n) WHERE n = 3 RETURN n: a node-ID equality is the const scan pass's business
const char* const nodeIDEquality = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k = db.constant(3 : i64)
  %mask = db.eq %n, %k : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, leavesNodeIDEqualityAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nodeIDEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

// MATCH (n) WHERE n.age = 32 AND n.isFrench = true RETURN n: a conjunction is one mask
const char* const conjunction = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(32 : i64)
  %e1 = db.eq %age, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %french = db.get_node_properties(%n, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
  %t = db.constant(true)
  %e2 = db.eq %french, %t : (!db.column<none>, !db.column<i1>) -> !db.column<!storage.bool>
  %mask = db.and %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, leavesConjunctionAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(conjunction);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

// MATCH (n) WITH n, n.name AS name WHERE n.age = 32 RETURN n, name
const char* const filterCarryingAnotherColumn = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(32 : i64)
  %mask = db.eq %age, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf, %namef = db.filter(%mask, {%n, %name}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%nf, %namef) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, leavesFilterCarryingAnotherColumnAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterCarryingAnotherColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

// MATCH (n:Person) WHERE n.age = 32 RETURN n
const char* const labelScan = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(32 : i64)
  %mask = db.eq %age, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, fusesLabelScanIntoALabelledPropertyScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module);
    ASSERT_TRUE(scan);
    EXPECT_EQ(scan.getProperty(), "age");

    // The label test rides on the fused scan, which walks only the matching label sets'
    // ranges: no check and no filter is left.
    const std::optional<mlir::ArrayAttr> labels = scan.getLabels();
    ASSERT_TRUE(labels);
    ASSERT_EQ(labels->size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>((*labels)[0]).getValue(), "Person");

    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front().getDefiningOp(), scan.getOperation());
}

TEST_F(FuseScanByPropertyValueTest, plainScanCarriesNoLabels) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(integerEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module);
    ASSERT_TRUE(scan);
    EXPECT_FALSE(scan.getLabels());
}

// MATCH (n), (m) WHERE n.age = 32 RETURN n, m, as codegen emits it: filtered inside the left factor
const char* const equalityInFactor = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %k = db.constant(32 : i64)
    %mask = db.eq %age, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    %nf = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.yield %nf : !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  db.output(%p#0, %p#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, fusesInsideCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(equalityInFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanNodesByPropertyValue> scans = collect<mlir::db::ScanNodesByPropertyValue>(*module);
    ASSERT_EQ(scans.size(), 1u);
    mlir::db::ScanNodesByPropertyValue scan = scans.front();

    // The fused scan took the filter's place inside the left factor, which now yields it;
    // the right factor's scan is untouched.
    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);
    mlir::db::CrossProduct product = products.front();

    mlir::db::Yield leftYield = mlir::cast<mlir::db::Yield>(product.getLeftFactor().front().getTerminator());
    ASSERT_EQ(leftYield.getColumns().size(), 1u);
    EXPECT_EQ(leftYield.getColumns().front().getDefiningOp(), scan.getOperation());

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
}

// MATCH (n)-->(m) WHERE n.name = 'Remy' RETURN n, m, as codegen emits it: filtered after the hop
const char* const equalityAfterHop = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %name = db.get_node_properties(%s, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant("Remy" : !storage.string)
  %mask = db.eq %name, %k : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%mask, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, fusesFilterPushedDownToScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(equalityAfterHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDownThenFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanNodesByPropertyValue> scans = collect<mlir::db::ScanNodesByPropertyValue>(*module);
    ASSERT_EQ(scans.size(), 1u);
    mlir::db::ScanNodesByPropertyValue scan = scans.front();
    EXPECT_EQ(scan.getProperty(), "name");

    // The hop now expands the fused scan, and no filter is left anywhere.
    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getInputNodes().getDefiningOp(), scan.getOperation());

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 0u);
}

// MATCH (n)-->(m) WHERE m.name = 'Bio' RETURN n: m is born at the hop, not at a scan
const char* const hopTargetEquality = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %name = db.get_node_properties(%t, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant("Bio" : !storage.string)
  %mask = db.eq %name, %k : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %sf = db.filter(%mask, {%s}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(FuseScanByPropertyValueTest, leavesHopTargetEqualityAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(hopTargetEquality);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDownThenFuse(*module));

    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}
