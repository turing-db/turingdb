#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OpDefinition.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "NLDialect.h"
#include "NLOps.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

namespace {

// MATCH (n) WHERE id(n) = x for x of several types, each a column db.entity_id_of reads
const char* const nodeIDOfValues = R"mlir(
func.func @main(%d: !db.column<f64>, %i: !db.column<i64>, %c: !db.column<!storage.any>, %s: !db.column<!storage.string>, %u: !db.column<none>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %a = db.entity_id_of(%d) : (!db.column<f64>) -> !db.column<!storage.node_id>
  %b = db.entity_id_of(%i) : (!db.column<i64>) -> !db.column<!storage.node_id>
  %e = db.entity_id_of(%c) : (!db.column<!storage.any>) -> !db.column<!storage.node_id>
  %f = db.entity_id_of(%s) : (!db.column<!storage.string>) -> !db.column<!storage.node_id>
  %g = db.entity_id_of(%u) : (!db.column<none>) -> !db.column<!storage.node_id>
  %h = db.entity_id_of(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%a, %b, %e, %f, %g, %h) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const nodeIDOfAnEdge = R"mlir(
func.func @main(%r: !db.column<!storage.edge_id>) {
  %a = db.entity_id_of(%r) : (!db.column<!storage.edge_id>) -> !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const nodeIDOfAPath = R"mlir(
func.func @main(%p: !db.column<!storage.path>) {
  %a = db.entity_id_of(%p) : (!db.column<!storage.path>) -> !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const nodeIDOfIntoAnInteger = R"mlir(
func.func @main(%d: !db.column<f64>) {
  %a = db.entity_id_of(%d) : (!db.column<f64>) -> !db.column<i64>
  db.output(%a) : !db.column<i64>
  return
}
)mlir";

const char* const nlNodeIDOf = R"mlir(
func.func @main(%d: !nl.chunk<!storage.nullable<f64>>, %c: !nl.chunk<!storage.any>, %r: !nl.chunk<!storage.edge_id>) {
  %a = nl.entity_id_of %d : (!nl.chunk<!storage.nullable<f64>>) -> !nl.chunk<!storage.node_id>
  %b = nl.entity_id_of %c : (!nl.chunk<!storage.any>) -> !nl.chunk<!storage.node_id>
  %e = nl.entity_id_of %r : (!nl.chunk<!storage.edge_id>) -> !nl.chunk<!storage.node_id>
  func.return
}
)mlir";

const char* const nlNodeIDOfAPath = R"mlir(
func.func @main(%p: !nl.chunk<!storage.path>) {
  %a = nl.entity_id_of %p : (!nl.chunk<!storage.path>) -> !nl.chunk<!storage.node_id>
  func.return
}
)mlir";

const char* const nlNodeIDOfIntoAnInteger = R"mlir(
func.func @main(%d: !nl.chunk<f64>) {
  %a = nl.entity_id_of %d : (!nl.chunk<f64>) -> !nl.chunk<i64>
  func.return
}
)mlir";

// entity_id_of(n), entity_id_of(id(n)), entity_id_of(id(r)) and entity_id_of(x)
const char* const foldableNodeIDOf = R"mlir(
func.func @main(%r: !db.column<!storage.edge_id>, %x: !db.column<f64>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %edgeID = db.element_id(%r) : (!db.column<!storage.edge_id>) -> !db.column<none>
  %a = db.entity_id_of(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %b = db.entity_id_of(%id) : (!db.column<none>) -> !db.column<!storage.node_id>
  %c = db.entity_id_of(%edgeID) : (!db.column<none>) -> !db.column<!storage.node_id>
  %d = db.entity_id_of(%x) : (!db.column<f64>) -> !db.column<!storage.node_id>
  db.output(%a, %b, %c, %d) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE id(n) = $x RETURN n
const char* const filteredIDEquality = R"mlir(
func.func @main(%x: !db.column<f64>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %eq = db.eq %x, %id : (!db.column<f64>, !db.column<none>) -> !db.column<!storage.bool>
  %kept = db.filter(%eq, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%kept) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE id(n) = $x OR n = 5 RETURN n
const char* const disjoinedIDEquality = R"mlir(
func.func @main(%x: !db.column<!storage.any>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %eq = db.eq %id, %x : (!db.column<none>, !db.column<!storage.any>) -> !db.column<!storage.bool>
  %k = db.constant(5 : i64)
  %five = db.eq %n, %k : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %or = db.or %eq, %five : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %kept = db.filter(%or, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%kept) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) RETURN id(n) = $x
const char* const projectedIDEquality = R"mlir(
func.func @main(%x: !db.column<f64>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %eq = db.eq %id, %x : (!db.column<none>, !db.column<f64>) -> !db.column<!storage.bool>
  db.output(%eq) : !db.column<!storage.bool>
  return
}
)mlir";

// MATCH (n) WHERE NOT id(n) = $x RETURN n
const char* const negatedIDEquality = R"mlir(
func.func @main(%x: !db.column<f64>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %eq = db.eq %id, %x : (!db.column<none>, !db.column<f64>) -> !db.column<!storage.bool>
  %not = db.not %eq : (!db.column<!storage.bool>) -> !db.column<!storage.bool>
  %kept = db.filter(%not, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%kept) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE id(n) = 3.0 OR id(n) = 2.5 RETURN n
const char* const literalIDEqualities = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %three = db.constant(3.000000e+00 : f64)
  %e1 = db.eq %id, %three : (!db.column<none>, !db.column<f64>) -> !db.column<!storage.bool>
  %fraction = db.constant(2.500000e+00 : f64)
  %e2 = db.eq %id, %fraction : (!db.column<none>, !db.column<f64>) -> !db.column<!storage.bool>
  %or = db.or %e1, %e2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %kept = db.filter(%or, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%kept) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n), (m) WHERE id(n) = id(m) RETURN n
const char* const twoIDsEquality = R"mlir(
func.func @main(%m: !db.column<!storage.node_id>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %nID = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %mID = db.element_id(%m) : (!db.column<!storage.node_id>) -> !db.column<none>
  %eq = db.eq %nID, %mID : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
  %kept = db.filter(%eq, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%kept) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[r]->() WHERE id(n) = r RETURN n
const char* const idEqualToAnEdge = R"mlir(
func.func @main(%r: !db.column<!storage.edge_id>) {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %id = db.element_id(%n) : (!db.column<!storage.node_id>) -> !db.column<none>
  %eq = db.eq %id, %r : (!db.column<none>, !db.column<!storage.edge_id>) -> !db.column<!storage.bool>
  %kept = db.filter(%eq, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%kept) : !db.column<!storage.node_id>
  return
}
)mlir";

}

class NodeIDOfTest : public ::testing::Test {
protected:
    NodeIDOfTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(std::string_view programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool canonicalize(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createCanonicalizeIDEqualities());

        return mlir::succeeded(passManager.run(module));
    }

    mlir::OwningOpRef<mlir::ModuleOp> parseAndCanonicalize(const char* programText) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        if (!module || !canonicalize(*module) || mlir::failed(mlir::verify(*module))) {
            return nullptr;
        }

        return module;
    }

    static mlir::db::EqOp soleEquality(mlir::ModuleOp module) {
        llvm::SmallVector<mlir::db::EqOp> equalities = collect<mlir::db::EqOp>(module);
        if (equalities.size() != 1) {
            return nullptr;
        }

        return equalities.front();
    }

    static mlir::Value foldedValue(mlir::db::EntityIDOf nodeIDOf) {
        llvm::SmallVector<mlir::OpFoldResult> results;
        if (mlir::failed(nodeIDOf->fold(results)) || results.size() != 1) {
            return {};
        }

        return mlir::dyn_cast<mlir::Value>(results.front());
    }

    mlir::MLIRContext _context;
};

TEST_F(NodeIDOfTest, readsAValueOfAnyTypeAndANode) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nodeIDOfValues);
    ASSERT_TRUE(module);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));
    EXPECT_EQ(countOps<mlir::db::EntityIDOf>(*module), 6u);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    mlir::ModuleOp parsed = module.get();
    parsed.print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed);
    ASSERT_TRUE(reparsed) << printed;
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
    EXPECT_EQ(countOps<mlir::db::EntityIDOf>(*reparsed), 6u);
}

TEST_F(NodeIDOfTest, readsAnEdgeAndRejectsAPath) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nodeIDOfAnEdge);
    ASSERT_TRUE(module);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_FALSE(parse(nodeIDOfAPath));
}

TEST_F(NodeIDOfTest, rejectsAResultOtherThanANode) {
    EXPECT_FALSE(parse(nodeIDOfIntoAnInteger));
}

TEST_F(NodeIDOfTest, lowersToAnNLOpOfTheSameShape) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nlNodeIDOf);
    ASSERT_TRUE(module);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));
    EXPECT_EQ(countOps<mlir::nl::EntityIDOf>(*module), 3u);

    EXPECT_FALSE(parse(nlNodeIDOfAPath));
    EXPECT_FALSE(parse(nlNodeIDOfIntoAnInteger));
}

TEST_F(NodeIDOfTest, foldsANodeAndTheIDOfANodeToTheNode) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(foldableNodeIDOf);
    ASSERT_TRUE(module);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    const llvm::SmallVector<mlir::db::EntityIDOf> nodeIDOfs = collect<mlir::db::EntityIDOf>(*module);
    ASSERT_EQ(nodeIDOfs.size(), 4u);

    const mlir::Value nodes = collect<mlir::db::ScanNodes>(*module).front().getResult();
    EXPECT_EQ(foldedValue(nodeIDOfs[0]), nodes);
    EXPECT_EQ(foldedValue(nodeIDOfs[1]), nodes);
    EXPECT_FALSE(foldedValue(nodeIDOfs[2]));
    EXPECT_FALSE(foldedValue(nodeIDOfs[3]));
}

TEST_F(NodeIDOfTest, comparesAFilteredNodeToTheNodeAValueNames) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndCanonicalize(filteredIDEquality);
    ASSERT_TRUE(module);

    mlir::db::EqOp equality = soleEquality(*module);
    ASSERT_TRUE(equality);

    const mlir::Value nodes = collect<mlir::db::ScanNodes>(*module).front().getResult();
    EXPECT_EQ(equality.getLhs(), nodes);

    mlir::db::EntityIDOf nodeIDOf = equality.getRhs().getDefiningOp<mlir::db::EntityIDOf>();
    ASSERT_TRUE(nodeIDOf);
    EXPECT_TRUE(mlir::isa<mlir::BlockArgument>(nodeIDOf.getInput()));
    EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 0u);
}

TEST_F(NodeIDOfTest, comparesADisjoinedNodeToTheNodeAValueNames) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndCanonicalize(disjoinedIDEquality);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::EntityIDOf>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 0u);
}

TEST_F(NodeIDOfTest, keepsAnIDComparedOutsideAFilter) {
    const char* const programs[] = {projectedIDEquality, negatedIDEquality};

    for (const char* const program : programs) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndCanonicalize(program);
        ASSERT_TRUE(module) << program;

        EXPECT_EQ(countOps<mlir::db::EntityIDOf>(*module), 0u) << program;
        EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 1u) << program;
    }
}

TEST_F(NodeIDOfTest, comparesANodeToTheNodeAnEdgeNumbers) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndCanonicalize(idEqualToAnEdge);
    ASSERT_TRUE(module);

    mlir::db::EqOp equality = soleEquality(*module);
    ASSERT_TRUE(equality);

    mlir::db::EntityIDOf nodeIDOf = equality.getRhs().getDefiningOp<mlir::db::EntityIDOf>();
    ASSERT_TRUE(nodeIDOf);
    EXPECT_TRUE(mlir::isa<mlir::BlockArgument>(nodeIDOf.getInput()));
    EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 0u);
}

TEST_F(NodeIDOfTest, foldsALiteralToTheIDItNames) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndCanonicalize(literalIDEqualities);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::EqOp> equalities = collect<mlir::db::EqOp>(*module);
    ASSERT_EQ(equalities.size(), 2u);

    mlir::db::ConstantOp named = equalities[0].getRhs().getDefiningOp<mlir::db::ConstantOp>();
    ASSERT_TRUE(named);
    const mlir::IntegerAttr nodeID = mlir::dyn_cast<mlir::IntegerAttr>(named.getValue());
    ASSERT_TRUE(nodeID);
    EXPECT_EQ(nodeID.getInt(), 3);

    mlir::db::EntityIDOf unnamed = equalities[1].getRhs().getDefiningOp<mlir::db::EntityIDOf>();
    ASSERT_TRUE(unnamed);
    EXPECT_TRUE(unnamed.getInput().getDefiningOp<mlir::db::ConstantOp>());
    EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 0u);
}

TEST_F(NodeIDOfTest, comparesTwoIDsAsTheirNodes) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndCanonicalize(twoIDsEquality);
    ASSERT_TRUE(module);

    mlir::db::EqOp equality = soleEquality(*module);
    ASSERT_TRUE(equality);

    const mlir::Value nodes = collect<mlir::db::ScanNodes>(*module).front().getResult();
    EXPECT_EQ(equality.getLhs(), nodes);
    EXPECT_TRUE(mlir::isa<mlir::BlockArgument>(equality.getRhs()));
    EXPECT_EQ(countOps<mlir::db::EntityIDOf>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ElementID>(*module), 0u);
}
