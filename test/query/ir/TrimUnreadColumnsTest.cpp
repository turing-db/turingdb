#include <gtest/gtest.h>

#include <iterator>

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
#include "StorageEnums.h"

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

class TrimUnreadColumnsTest : public ::testing::Test {
protected:
    TrimUnreadColumnsTest() {
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

// MATCH (a)-->(b)-->(c) RETURN c
const char* const twoHopTail = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2, %s1c, %e1c, %et1c = db.get_out_edges(%t1, {%s1, %e1, %et1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>)
  db.output(%t2) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, dropsTheCarrySetOfAHopNothingReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopTail);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 2u);
    mlir::db::GetOutEdges firstHop = hops[0];
    mlir::db::GetOutEdges secondHop = hops[1];

    // The second hop carries nothing and yields only its four fixed columns.
    EXPECT_EQ(secondHop.getColumnsToFilter().size(), 0u);
    EXPECT_EQ(secondHop->getNumResults(), 4u);
    EXPECT_EQ(secondHop.getInputNodes(), firstHop.getTgtids());

    // The first hop is left producing only the targets the second hop walks from.
    EXPECT_TRUE(firstHop.getSrcids().use_empty());
    EXPECT_TRUE(firstHop.getEids().use_empty());
    EXPECT_TRUE(firstHop.getEtypes().use_empty());
    EXPECT_TRUE(firstHop.getTgtids().hasOneUse());

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front(), secondHop.getTgtids());
}

// MATCH (n) WITH collect(n) AS l UNWIND l AS x MATCH (x)-->(b) RETURN b, with the list the
// unwind spread riding beside the elements it hands out
const char* const unwoundListCarry = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %list = db.collect(%n) keys 0 : (!db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.node_id>>
  %x, %lc = db.unwind(%list, {%list}) : (!db.column<!storage.list<!storage.node_id>>, !db.column<!storage.list<!storage.node_id>>) -> (!db.column<!storage.node_id>, !db.column<!storage.list<!storage.node_id>>)
  %s, %e, %et, %t = db.get_out_edges(%x, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%t) : !db.column<!storage.node_id>
  return
}
)mlir";

// An unwind replicates each carried cell once per element it hands out, so a list nothing
// reads past it is work done for no column.
TEST_F(TrimUnreadColumnsTest, dropsTheCarrySetOfAnUnwindNothingReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwoundListCarry);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::Unwind> unwinds = collect<mlir::db::Unwind>(*module);
    ASSERT_EQ(unwinds.size(), 1u);
    mlir::db::Unwind unwind = unwinds.front();

    // The elements it hands out are its one result; the list rides no further.
    EXPECT_EQ(unwind.getColumnsToFilter().size(), 0u);
    EXPECT_EQ(unwind->getNumResults(), 1u);
    EXPECT_TRUE(unwind.getElement().hasOneUse());
}

// MATCH (a)-->(b) WHERE b.age > 3 RETURN b
const char* const filteredTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%t, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %lim = db.constant(3 : i64)
  %mask = db.gt %age, %lim : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %tf, %ef, %sf, %etf = db.filter(%mask, {%t, %e, %s, %et}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
  db.output(%tf) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, trimsAFilterToTheColumnsReadAfterIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filteredTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdges hop = hops.front();

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();

    // The filter cuts only the target column, and the output reads its filtered version.
    ASSERT_EQ(filter.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(filter.getColumnsToFilter().front(), hop.getTgtids());
    ASSERT_EQ(filter.getFilteredColumns().size(), 1u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front(), filter.getFilteredColumns().front());

    // The hop is left producing targets alone.
    EXPECT_TRUE(hop.getSrcids().use_empty());
    EXPECT_TRUE(hop.getEids().use_empty());
    EXPECT_TRUE(hop.getEtypes().use_empty());
}

// MATCH (a)-[:KNOWS]->(b)-->(c) RETURN c: the type check reads the first hop's edge types,
// so those stay; everything else the filter carried into the second hop goes.
const char* const typedHopIntoUntyped = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %knows = db.check_edge_type_constraint(%et1, ["KNOWS"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %tf, %ef, %sf, %etf = db.filter(%knows, {%t1, %e1, %s1, %et1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
  %s2, %e2, %et2, %t2, %sc, %ec, %etc = db.get_out_edges(%tf, {%sf, %ef, %etf}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>)
  db.output(%t2) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, trimsThroughAFilterIntoTheHopBeforeIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedHopIntoUntyped);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 2u);
    mlir::db::GetOutEdges firstHop = hops[0];
    mlir::db::GetOutEdges secondHop = hops[1];

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();

    EXPECT_EQ(secondHop.getColumnsToFilter().size(), 0u);

    ASSERT_EQ(filter.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(filter.getColumnsToFilter().front(), firstHop.getTgtids());
    EXPECT_EQ(secondHop.getInputNodes(), filter.getFilteredColumns().front());

    // The edge types feed the type check and nothing else; sources and edges feed nothing.
    EXPECT_TRUE(firstHop.getEtypes().hasOneUse());
    EXPECT_TRUE(mlir::isa<mlir::db::CheckEdgeTypeConstraint>(*firstHop.getEtypes().getUsers().begin()));
    EXPECT_TRUE(firstHop.getSrcids().use_empty());
    EXPECT_TRUE(firstHop.getEids().use_empty());
}

// MATCH (a)-->(b) WHERE b.age > 3 RETURN 1: nothing reads the filter, yet its rows are what
// the constant is projected over.
const char* const filterNothingReads = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%t, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %lim = db.constant(3 : i64)
  %mask = db.gt %age, %lim : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %tf, %ef, %sf, %etf = db.filter(%mask, {%t, %e, %s, %et}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
  %one = db.constant(1 : i64)
  db.output(%one) : !db.column<i64>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, keepsOneRowCarryingColumnInAFilterNothingReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterNothingReads);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdges hop = hops.front();

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();

    ASSERT_EQ(filter.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(filter.getColumnsToFilter().front(), hop.getTgtids());

    EXPECT_TRUE(hop.getSrcids().use_empty());
    EXPECT_TRUE(hop.getEids().use_empty());
    EXPECT_TRUE(hop.getEtypes().use_empty());
}

// A filter carrying a constant ahead of a node column: the column kept is the one that
// carries rows, not the first one.
const char* const filterWithLeadingConstant = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %c = db.constant(3 : i64)
  %age = db.get_node_properties(%t, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %mask = db.gt %age, %c : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %cf, %tf = db.filter(%mask, {%c, %t}) : (!db.column<!storage.bool>, !db.column<i64>, !db.column<!storage.node_id>) -> (!db.column<i64>, !db.column<!storage.node_id>)
  db.output(%c) : !db.column<i64>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, prefersARowCarryingColumnWhenAFilterKeepsOne) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterWithLeadingConstant);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    mlir::db::FilterOp filter = filters.front();

    ASSERT_EQ(filter.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(filter.getColumnsToFilter().front(), hops.front().getTgtids());
}

// MATCH (a)-->(b)-->(c) RETURN a, b, c
const char* const twoHopAllRead = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2, %s1c = db.get_out_edges(%t1, {%s1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s1c, %s2, %t2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, leavesACarrySetThatIsReadAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopAllRead);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::GetOutEdges> before = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(before.size(), 2u);

    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    // Neither hop was rebuilt: the same ops are still there, carry set intact.
    llvm::SmallVector<mlir::db::GetOutEdges> after = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(after.size(), 2u);
    EXPECT_EQ(after[0].getOperation(), before[0].getOperation());
    EXPECT_EQ(after[1].getOperation(), before[1].getOperation());
    EXPECT_EQ(after[1].getColumnsToFilter().size(), 1u);
}

// MATCH (a)-[:KNOWS]->(b) from a typed second hop: the type survives the rebuild.
const char* const typedSecondHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2, %sc, %ec = db.get_out_edges_by_type(%t1, "KNOWS", {%s1, %e1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>)
  db.output(%t2) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, keepsTheEdgeTypeOfAByTypeHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedSecondHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByType> typedHops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(typedHops.size(), 1u);
    mlir::db::GetOutEdgesByType typedHop = typedHops.front();

    EXPECT_EQ(typedHop.getEdgeType(), "KNOWS");
    EXPECT_EQ(typedHop.getColumnsToFilter().size(), 0u);
    EXPECT_EQ(typedHop->getNumResults(), 4u);

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_TRUE(hops.front().getSrcids().use_empty());
    EXPECT_TRUE(hops.front().getEids().use_empty());
    EXPECT_EQ(typedHop.getInputNodes(), hops.front().getTgtids());
}

// MATCH (a)-->(b)<--(c)--(d) RETURN d: the root's column rides two hops before dying, so
// the third hop must be trimmed before the second, and the second before the first can
// be seen to produce targets alone.
const char* const threeHopChain = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2, %s1c = db.get_in_edges(%t1, {%s1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %s3, %e3, %et3, %t3, %s1cc, %t2c = db.get_edges(%s2, {%s1c, %t2}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%t3) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, trimsAChainOfHopsInOneRun) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(threeHopChain);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> outHops = collect<mlir::db::GetOutEdges>(*module);
    llvm::SmallVector<mlir::db::GetInEdges> inHops = collect<mlir::db::GetInEdges>(*module);
    llvm::SmallVector<mlir::db::GetEdges> anyHops = collect<mlir::db::GetEdges>(*module);
    ASSERT_EQ(outHops.size(), 1u);
    ASSERT_EQ(inHops.size(), 1u);
    ASSERT_EQ(anyHops.size(), 1u);

    EXPECT_EQ(inHops.front().getColumnsToFilter().size(), 0u);
    EXPECT_EQ(anyHops.front().getColumnsToFilter().size(), 0u);

    // Each hop keeps only the end the next one walks from.
    EXPECT_TRUE(outHops.front().getSrcids().use_empty());
    EXPECT_TRUE(outHops.front().getEids().use_empty());
    EXPECT_TRUE(outHops.front().getEtypes().use_empty());
    EXPECT_TRUE(outHops.front().getTgtids().hasOneUse());

    EXPECT_TRUE(inHops.front().getTgtids().use_empty());
    EXPECT_TRUE(inHops.front().getEids().use_empty());
    EXPECT_TRUE(inHops.front().getEtypes().use_empty());
    EXPECT_TRUE(inHops.front().getSrcids().hasOneUse());
    EXPECT_EQ(anyHops.front().getInputNodes(), inHops.front().getSrcids());
}

// MATCH (a)-->(b) WITH a, b LIMIT 3 MATCH (b)-->(c) RETURN c
const char* const limitInAWith = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %la, %lb = db.limit(%s1, %t1) count 3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2 = db.get_out_edges(%lb, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%t2) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, trimsALimitToTheColumnsReadAfterIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(limitInAWith);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 2u);

    llvm::SmallVector<mlir::db::Limit> limits = collect<mlir::db::Limit>(*module);
    ASSERT_EQ(limits.size(), 1u);
    mlir::db::Limit limit = limits.front();

    ASSERT_EQ(limit.getColumns().size(), 1u);
    EXPECT_EQ(limit.getColumns().front(), hops[0].getTgtids());
    EXPECT_EQ(limit.getCount(), 3u);
    ASSERT_EQ(limit.getResults().size(), 1u);
    EXPECT_EQ(hops[1].getInputNodes(), limit.getResults().front());

    EXPECT_TRUE(hops[0].getSrcids().use_empty());
}

// MATCH (a)-->(b) WITH a, b SKIP 1 MATCH (b)-->(c) RETURN c
const char* const skipInAWith = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ka, %kb = db.skip(%s1, %t1) count 1 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2 = db.get_out_edges(%kb, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%t2) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, trimsASkipToTheColumnsReadAfterIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(skipInAWith);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 2u);

    llvm::SmallVector<mlir::db::Skip> skips = collect<mlir::db::Skip>(*module);
    ASSERT_EQ(skips.size(), 1u);
    mlir::db::Skip skip = skips.front();

    ASSERT_EQ(skip.getColumns().size(), 1u);
    EXPECT_EQ(skip.getColumns().front(), hops[0].getTgtids());
    EXPECT_EQ(skip.getCount(), 1u);
    ASSERT_EQ(skip.getResults().size(), 1u);
    EXPECT_EQ(hops[1].getInputNodes(), skip.getResults().front());

    EXPECT_TRUE(hops[0].getSrcids().use_empty());
}

// MATCH (a)-->(b) WITH a, b LIMIT 3 RETURN 1: nothing reads the cut, yet its rows are what
// the constant is projected over.
const char* const limitNothingReads = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %la, %lb = db.limit(%s1, %t1) count 3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %one = db.constant(1 : i64)
  db.output(%one) : !db.column<i64>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, keepsOneRowCarryingColumnInALimitNothingReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(limitNothingReads);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);

    llvm::SmallVector<mlir::db::Limit> limits = collect<mlir::db::Limit>(*module);
    ASSERT_EQ(limits.size(), 1u);
    mlir::db::Limit limit = limits.front();

    ASSERT_EQ(limit.getColumns().size(), 1u);
    EXPECT_EQ(limit.getColumns().front(), hops.front().getSrcids());
}

// MATCH (a)-->(b) WITH a, b ORDER BY b.name, b.age MATCH (b)-->(c) RETURN c: the keys stay
// even though nothing reads them sorted, and their indices follow the surviving columns.
const char* const sortInAWith = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %age = db.get_node_properties(%t1, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %name = db.get_node_properties(%t1, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %sa, %sb, %sage, %sname = db.sort(%s1, %t1, %age, %name) keys [3, 2] ascending [false, true] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.string>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.string>)
  %s2, %e2, %et2, %t2 = db.get_out_edges(%sb, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%t2) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, trimsTheNonKeyColumnsOfASortAndRenumbersItsKeys) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sortInAWith);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 2u);

    llvm::SmallVector<mlir::db::Sort> sorts = collect<mlir::db::Sort>(*module);
    ASSERT_EQ(sorts.size(), 1u);
    mlir::db::Sort sort = sorts.front();

    // The root's column went; the walked column and both keys stay, in their order.
    ASSERT_EQ(sort.getColumns().size(), 3u);
    EXPECT_EQ(sort.getColumns()[0], hops[0].getTgtids());
    EXPECT_TRUE(mlir::isa<mlir::db::GetNodeProperties>(sort.getColumns()[1].getDefiningOp()));
    EXPECT_TRUE(mlir::isa<mlir::db::GetNodeProperties>(sort.getColumns()[2].getDefiningOp()));

    const llvm::SmallVector<int64_t> expectedKeys {2, 1};
    const llvm::SmallVector<bool> expectedAscending {false, true};
    EXPECT_EQ(sort.getKeyColumns(), llvm::ArrayRef<int64_t>(expectedKeys));
    EXPECT_EQ(sort.getKeyAscending(), llvm::ArrayRef<bool>(expectedAscending));

    ASSERT_EQ(sort.getResults().size(), 3u);
    EXPECT_EQ(hops[1].getInputNodes(), sort.getResults()[0]);
    EXPECT_TRUE(sort.getResults()[1].use_empty());
    EXPECT_TRUE(sort.getResults()[2].use_empty());

    EXPECT_TRUE(hops[0].getSrcids().use_empty());
}

// MATCH (a)-->(b) RETURN b.name ORDER BY b.age: the only unread column is the key.
const char* const sortOnAnUnreadKey = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %name = db.get_node_properties(%t, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %age = db.get_node_properties(%t, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %sname, %sage = db.sort(%name, %age) keys [1] ascending [true] : (!db.column<!storage.string>, !db.column<i64>) -> (!db.column<!storage.string>, !db.column<i64>)
  db.output(%sname) : !db.column<!storage.string>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, leavesASortWhoseOnlyUnreadColumnIsAKeyAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sortOnAnUnreadKey);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Sort> before = collect<mlir::db::Sort>(*module);
    ASSERT_EQ(before.size(), 1u);

    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::Sort> after = collect<mlir::db::Sort>(*module);
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after.front().getOperation(), before.front().getOperation());
    EXPECT_EQ(after.front().getColumns().size(), 2u);
}

// MATCH (a)-->(b), (c)-->(d) RETURN b, d
const char* const productOfTwoHops = R"mlir(
func.func @main() {
  %p:6 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s1, %e1, %t1 : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  } factor {
    %c = db.scan_nodes() : !db.column<!storage.node_id>
    %s2, %e2, %et2, %t2 = db.get_out_edges(%c, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s2, %e2, %t2 : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  }
  db.output(%p#2, %p#5) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, trimsTheYieldsOfACrossProductToTheColumnsReadAfterIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(productOfTwoHops);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);
    mlir::db::CrossProduct product = products.front();
    ASSERT_EQ(product->getNumResults(), 2u);

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 2u);

    llvm::SmallVector<mlir::db::Yield> yields = collect<mlir::db::Yield>(*module);
    ASSERT_EQ(yields.size(), 2u);
    for (size_t factor = 0; factor < 2; factor++) {
        ASSERT_EQ(yields[factor].getColumns().size(), 1u);
        EXPECT_EQ(yields[factor].getColumns().front(), hops[factor].getTgtids());

        EXPECT_TRUE(hops[factor].getSrcids().use_empty());
        EXPECT_TRUE(hops[factor].getEids().use_empty());
        EXPECT_TRUE(hops[factor].getEtypes().use_empty());
    }

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns()[0], product->getResult(0));
    EXPECT_EQ(outputs.front().getColumns()[1], product->getResult(1));
}

// MATCH (a), (b)-->(c) RETURN 1: nothing reads the product, yet each factor still sizes it.
const char* const productNothingReads = R"mlir(
func.func @main() {
  %p:4 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    %s, %e, %et, %t = db.get_out_edges(%b, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s, %e, %t : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  }
  %one = db.constant(1 : i64)
  db.output(%one) : !db.column<i64>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, keepsOneRowCarryingYieldPerFactorOfAProductNothingReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(productNothingReads);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);
    EXPECT_EQ(products.front()->getNumResults(), 2u);

    llvm::SmallVector<mlir::db::Yield> yields = collect<mlir::db::Yield>(*module);
    ASSERT_EQ(yields.size(), 2u);
    ASSERT_EQ(yields[0].getColumns().size(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(yields[0].getColumns().front().getDefiningOp()));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    ASSERT_EQ(yields[1].getColumns().size(), 1u);
    EXPECT_EQ(yields[1].getColumns().front(), hops.front().getSrcids());
    EXPECT_TRUE(hops.front().getEids().use_empty());
    EXPECT_TRUE(hops.front().getTgtids().use_empty());
}

// A factor yielding a constant beside its scan, with only the constant read: the scan stays
// too, since it is what carries the factor's rows.
const char* const productReadingAConstantOnly = R"mlir(
func.func @main() {
  %p:3 = db.cross_product factor {
    %k = db.constant(7 : i64)
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %k, %a : !db.column<i64>, !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  db.output(%p#0) : !db.column<i64>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, keepsARowCarryingYieldBesideAReadConstant) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(productReadingAConstantOnly);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::CrossProduct> before = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(before.size(), 1u);

    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CrossProduct> after = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after.front().getOperation(), before.front().getOperation());
    EXPECT_EQ(after.front()->getNumResults(), 3u);
}

// MATCH (n) WITH n.name AS name, count(*) AS c, sum(n.age) AS s RETURN name, s
const char* const groupAggregateWithAnUnreadAggregate = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %g:3 = db.group_aggregate(%name, %n, %age) keys 1 aggregates [count_rows, sum] : (!db.column<!storage.string>, !db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<!storage.string>, !db.column<none>, !db.column<none>)
  db.output(%g#0, %g#2) : !db.column<!storage.string>, !db.column<none>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, dropsTheUnreadAggregatesOfAGroupAggregate) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(groupAggregateWithAnUnreadAggregate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GroupAggregate> groups = collect<mlir::db::GroupAggregate>(*module);
    ASSERT_EQ(groups.size(), 1u);
    mlir::db::GroupAggregate group = groups.front();

    ASSERT_EQ(group.getColumns().size(), 2u);
    EXPECT_EQ(group.getKeyCount(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::db::GetNodeProperties>(group.getColumns()[1].getDefiningOp()));

    const llvm::SmallVector<int64_t> expectedKinds {static_cast<int64_t>(mlir::storage::GroupAggregateKind::Sum)};
    EXPECT_EQ(group.getKinds(), llvm::ArrayRef<int64_t>(expectedKinds));

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(group.getResults().size(), 2u);
    EXPECT_EQ(outputs.front().getColumns()[0], group.getResults()[0]);
    EXPECT_EQ(outputs.front().getColumns()[1], group.getResults()[1]);

    // The scanned node column fed only the dropped count.
    llvm::SmallVector<mlir::db::ScanNodes> scans = collect<mlir::db::ScanNodes>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_EQ(std::distance(scans.front().getResult().getUsers().begin(), scans.front().getResult().getUsers().end()), 2);
}

// MATCH (n) WITH n.name AS name, count(*) AS c, sum(n.age) AS s RETURN 1: the key groups the
// rows whether or not it is read, and one aggregate has to remain.
const char* const groupAggregateNothingReads = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %g:3 = db.group_aggregate(%name, %n, %age) keys 1 aggregates [count_rows, sum] : (!db.column<!storage.string>, !db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<!storage.string>, !db.column<none>, !db.column<none>)
  %one = db.constant(1 : i64)
  db.output(%one) : !db.column<i64>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, keepsTheKeyAndOneAggregateOfAGroupAggregateNothingReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(groupAggregateNothingReads);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GroupAggregate> groups = collect<mlir::db::GroupAggregate>(*module);
    ASSERT_EQ(groups.size(), 1u);
    mlir::db::GroupAggregate group = groups.front();

    ASSERT_EQ(group.getColumns().size(), 2u);
    EXPECT_EQ(group.getKeyCount(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanNodes>(group.getColumns()[1].getDefiningOp()));

    const llvm::SmallVector<int64_t> expectedKinds {static_cast<int64_t>(mlir::storage::GroupAggregateKind::CountRows)};
    EXPECT_EQ(group.getKinds(), llvm::ArrayRef<int64_t>(expectedKinds));
}

// MATCH (n) WITH n.name AS name, count(*) AS c RETURN c: the unread key still groups.
const char* const groupAggregateWithAnUnreadKey = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %g:2 = db.group_aggregate(%name, %n) keys 1 aggregates [count_rows] : (!db.column<!storage.string>, !db.column<!storage.node_id>) -> (!db.column<!storage.string>, !db.column<none>)
  db.output(%g#1) : !db.column<none>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, leavesAGroupAggregateWhoseOnlyUnreadColumnIsAKeyAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(groupAggregateWithAnUnreadKey);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::GroupAggregate> before = collect<mlir::db::GroupAggregate>(*module);
    ASSERT_EQ(before.size(), 1u);

    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GroupAggregate> after = collect<mlir::db::GroupAggregate>(*module);
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after.front().getOperation(), before.front().getOperation());
}

// WITH k, collect(DISTINCT v0), collect(v1), collect(DISTINCT v2), count(*) reading only the
// v1 and v2 lists: the dropped value takes its distinct entry with it, the kept one renumbers.
const char* const collectWithUnreadValuesAndAggregate = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k = db.get_node_properties(%n, "k") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %v0 = db.get_node_properties(%n, "v0") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %v1 = db.get_node_properties(%n, "v1") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %v2 = db.get_node_properties(%n, "v2") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %c:5 = db.collect(%k, %v0, %v1, %v2, %n) keys 1 aggregates [count_rows] distinct [0, 2] : (!db.column<!storage.string>, !db.column<i64>, !db.column<i64>, !db.column<i64>, !db.column<!storage.node_id>) -> (!db.column<!storage.string>, !db.column<!storage.list<i64>>, !db.column<!storage.list<i64>>, !db.column<!storage.list<i64>>, !db.column<none>)
  db.output(%c#2, %c#3) : !db.column<!storage.list<i64>>, !db.column<!storage.list<i64>>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, dropsTheUnreadValuesAndAggregatesOfACollectAndRenumbersDistinct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectWithUnreadValuesAndAggregate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::Collect> collects = collect<mlir::db::Collect>(*module);
    ASSERT_EQ(collects.size(), 1u);
    mlir::db::Collect collectOp = collects.front();

    ASSERT_EQ(collectOp.getColumns().size(), 3u);
    EXPECT_EQ(collectOp.getKeyCount(), 1u);
    EXPECT_FALSE(collectOp.getKinds().has_value());

    ASSERT_TRUE(collectOp.getDistinctValues().has_value());
    const llvm::SmallVector<int64_t> expectedDistinct {1};
    EXPECT_EQ(*collectOp.getDistinctValues(), llvm::ArrayRef<int64_t>(expectedDistinct));

    ASSERT_EQ(collectOp.getResults().size(), 3u);
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns()[0], collectOp.getResults()[1]);
    EXPECT_EQ(outputs.front().getColumns()[1], collectOp.getResults()[2]);
}

// WITH k, collect(v), count(*) reading only the count: the list column stays as the one value
// the collect gathers.
const char* const collectReadingTheCountOnly = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k = db.get_node_properties(%n, "k") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %v = db.get_node_properties(%n, "v") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %c:3 = db.collect(%k, %v, %n) keys 1 aggregates [count_rows] : (!db.column<!storage.string>, !db.column<i64>, !db.column<!storage.node_id>) -> (!db.column<!storage.string>, !db.column<!storage.list<i64>>, !db.column<none>)
  db.output(%c#2) : !db.column<none>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, leavesACollectWhoseOnlyUnreadColumnsAreItsKeyAndValueAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectReadingTheCountOnly);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Collect> before = collect<mlir::db::Collect>(*module);
    ASSERT_EQ(before.size(), 1u);

    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::Collect> after = collect<mlir::db::Collect>(*module);
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after.front().getOperation(), before.front().getOperation());
}

// MATCH (n) WITH collect(n.name) AS names, collect(n.age) AS ages RETURN ages
const char* const ungroupedCollectWithAnUnreadList = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.string>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %c:2 = db.collect(%name, %age) keys 0 : (!db.column<!storage.string>, !db.column<i64>) -> (!db.column<!storage.list<!storage.string>>, !db.column<!storage.list<i64>>)
  db.output(%c#1) : !db.column<!storage.list<i64>>
  return
}
)mlir";

TEST_F(TrimUnreadColumnsTest, dropsTheUnreadListOfAnUngroupedCollect) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(ungroupedCollectWithAnUnreadList);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::Collect> collects = collect<mlir::db::Collect>(*module);
    ASSERT_EQ(collects.size(), 1u);
    mlir::db::Collect collectOp = collects.front();

    ASSERT_EQ(collectOp.getColumns().size(), 1u);
    EXPECT_EQ(collectOp.getKeyCount(), 0u);
    EXPECT_FALSE(collectOp.getKinds().has_value());
    EXPECT_FALSE(collectOp.getDistinctValues().has_value());
    ASSERT_EQ(collectOp.getResults().size(), 1u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front(), collectOp.getResults().front());
}
