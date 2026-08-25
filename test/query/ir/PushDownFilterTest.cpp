#include <gtest/gtest.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
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

class PushDownFilterTest : public ::testing::Test {
protected:
    PushDownFilterTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    // Runs the pushdown pass over the module and reports whether it succeeded.
    bool runPushDown(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createPushDownFilters());

        return mlir::succeeded(passManager.run(module));
    }

    mlir::MLIRContext _context;
};

// MATCH (a)-->(b)-->(c) WHERE a.age > 30 RETURN a, b, c
const char* const twoHopRootPredicate = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2, %ac = db.get_out_edges(%t1, {%s1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%ac, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %lim = db.constant(30 : i64)
  %mask = db.gt %age, %lim : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %af, %bf, %cf = db.filter(%mask, {%ac, %s2, %t2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%af, %bf, %cf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(PushDownFilterTest, sinksRootPredicatePastHops) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopRootPredicate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDown(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 2u);
    mlir::db::GetOutEdges firstHop = hops.front();

    // The filter now precedes both hops.
    EXPECT_TRUE(filter.getOperation()->isBeforeInBlock(firstHop.getOperation()));

    // The filter carries the single scanned column, and the first hop consumes the
    // filtered result rather than the raw scan.
    ASSERT_EQ(filter.getColumnsToFilter().size(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(filter.getColumnsToFilter().front().getDefiningOp()));
    EXPECT_EQ(firstHop.getInputNodes().getDefiningOp(), filter.getOperation());

    // The mask reads the property off the scanned column, not a hop output.
    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);
    mlir::db::GetNodeProperties read = reads.front();
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(read.getInputNodes().getDefiningOp()));
}

// MATCH (a) WHERE a.age > 30 RETURN a
const char* const rootOnlyPredicate = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %lim = db.constant(30 : i64)
  %mask = db.gt %age, %lim : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %af = db.filter(%mask, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(PushDownFilterTest, leavesRootAdjacentPredicateAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(rootOnlyPredicate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDown(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    // Still exactly one filter - the pass did not clone it.
    const llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    EXPECT_EQ(filters.size(), 1u);

    const llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    EXPECT_EQ(reads.size(), 1u);
}

// MATCH (a {age: 32, score: 5})-->(b) RETURN a, b
const char* const twoRootPredicatesStack = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%s1, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %c32 = db.constant(32 : i64)
  %m1 = db.eq %age, %c32 : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %a1, %b1 = db.filter(%m1, {%s1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %score = db.get_node_properties(%a1, "score") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %c5 = db.constant(5 : i64)
  %m2 = db.eq %score, %c5 : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %a2, %b2 = db.filter(%m2, {%a1, %b1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%a2, %b2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(PushDownFilterTest, stacksTwoRootPredicatesBeforeHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoRootPredicatesStack);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDown(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 2u);

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdges hop = hops.front();

    // Both filters precede the hop, and each carries a single column.
    for (mlir::db::FilterOp filter : filters) {
        EXPECT_TRUE(filter.getOperation()->isBeforeInBlock(hop.getOperation()));
        EXPECT_EQ(filter.getColumnsToFilter().size(), 1u);
    }

    // The hop consumes a filter's output: the two are stacked scan -> filter -> filter -> hop.
    EXPECT_TRUE(mlir::isa<mlir::db::FilterOp>(hop.getInputNodes().getDefiningOp()));
}

// MATCH (a)-[e]->(b) WHERE e.duration > 10 RETURN a
const char* const edgePredicate = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %dur = db.get_edge_properties(%e1, "duration") : (!db.column<!storage.edge_id>) -> !db.column<i64>
  %lim = db.constant(10 : i64)
  %mask = db.gt %dur, %lim : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%mask, {%s1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(PushDownFilterTest, leavesEdgePredicateAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgePredicate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDown(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);

    // The filter still sits after the hop.
    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_TRUE(hops.front().getOperation()->isBeforeInBlock(filters.front().getOperation()));
}

// MATCH (n), (m) WHERE n.age = 32 RETURN n, m
const char* const crossProductFactorPredicate = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %n : !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  %age = db.get_node_properties(%0#0, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %c32 = db.constant(32 : i64)
  %mask = db.eq %age, %c32 : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf, %mf = db.filter(%mask, {%0#0, %0#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%nf, %mf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(PushDownFilterTest, sinksPredicateIntoCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductFactorPredicate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDown(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();

    // The filter now lives inside a cross_product factor, not at the top level.
    mlir::db::CrossProduct product = filter.getOperation()->getParentOfType<mlir::db::CrossProduct>();
    ASSERT_TRUE(product);

    // It carries the single scanned column of that factor.
    ASSERT_EQ(filter.getColumnsToFilter().size(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(filter.getColumnsToFilter().front().getDefiningOp()));

    // The factor now yields the filtered column, so the product crosses filtered n.
    mlir::db::Yield leftYield = mlir::cast<mlir::db::Yield>(product.getLeftFactor().front().getTerminator());
    ASSERT_EQ(leftYield.getColumns().size(), 1u);
    EXPECT_EQ(leftYield.getColumns().front().getDefiningOp(), filter.getOperation());

    // The mask reads the property off the scanned column, not the product result.
    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(reads.front().getInputNodes().getDefiningOp()));
}

// MATCH (a)<--(n)-->(m) WHERE n.age = 32 RETURN a, n, m
const char* const inEdgeNeighbourPredicate = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2, %ac = db.get_out_edges(%s1, {%t1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%s2, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %k = db.constant(32 : i64)
  %mask = db.eq %age, %k : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %nf, %mf, %af = db.filter(%mask, {%s2, %t2, %ac}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%af, %nf, %mf) names ["a", "n", "m"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(PushDownFilterTest, sinksNeighbourPredicateToBindingHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inEdgeNeighbourPredicate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPushDown(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();

    llvm::SmallVector<mlir::db::GetInEdges> inHops = collect<mlir::db::GetInEdges>(*module);
    ASSERT_EQ(inHops.size(), 1u);
    llvm::SmallVector<mlir::db::GetOutEdges> outHops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(outHops.size(), 1u);
    mlir::db::GetInEdges inHop = inHops.front();
    mlir::db::GetOutEdges outHop = outHops.front();

    EXPECT_TRUE(inHop.getOperation()->isBeforeInBlock(filter.getOperation()));
    EXPECT_TRUE(filter.getOperation()->isBeforeInBlock(outHop.getOperation()));

    EXPECT_EQ(filter.getColumnsToFilter().size(), 2u);
    EXPECT_EQ(outHop.getInputNodes().getDefiningOp(), filter.getOperation());

    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);
    EXPECT_EQ(reads.front().getInputNodes().getDefiningOp(), inHop.getOperation());
}
