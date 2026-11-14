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

// fuse_explore_list_predicate drops the list predicate, and with it the expand_path and the
// to_nullable that only the predicate read. The fusion holds no handle to them afterwards.
class ExploreListPredicateErasedExpansionTest : public ::testing::Test {
protected:
    ExploreListPredicateErasedExpansionTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runFusion(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseExploreListPredicate());

        return mlir::succeeded(passManager.run(module));
    }

    mlir::MLIRContext _context;
};

// MATCH p = (n:Person)-[*1..2]->(m) WHERE none(x IN relationships(p) WHERE x.duration = 200) RETURN m
const char* const relationshipsReadByTheTestAlone = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %src, %tgt, %paths = db.explore_paths(%n, {}) forward hops 1 to 2 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %edges = db.expand_path(%paths) kind edges : (!db.column<!storage.path_ref>) -> !db.column<!storage.list<!storage.edge_id>>
  %widened = db.to_nullable %edges : (!db.column<!storage.list<!storage.edge_id>>) -> !db.column<!storage.nullable<!storage.list<!storage.edge_id>>>
  %ok = db.list_predicate(%widened, {}) kind none {
  ^bb0(%x: !db.column<none>, %tag: !db.column<ui64>):
    %duration = db.get_edge_properties(%x, "duration") : (!db.column<none>) -> !db.column<!storage.nullable<i64>>
    %c = db.constant(200 : i64)
    %eq = db.eq %duration, %c : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>
    db.comprehension_yield %tag, %eq : !db.column<!storage.bool>
  } : (!db.column<!storage.nullable<!storage.list<!storage.edge_id>>>) -> !db.column<!storage.bool>
  %m = db.filter(%ok, {%tgt}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%m) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH p = (n:Person)-[*1..2]->(m) WHERE all(x IN nodes(p) WHERE x.age > 20) RETURN m
const char* const nodesReadByTheTestAlone = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %src, %tgt, %paths = db.explore_paths(%n, {}) forward hops 1 to 2 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %nodes = db.expand_path(%paths, %src) kind nodes : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.node_id>>
  %widened = db.to_nullable %nodes : (!db.column<!storage.list<!storage.node_id>>) -> !db.column<!storage.nullable<!storage.list<!storage.node_id>>>
  %ok = db.list_predicate(%widened, {}) kind all {
  ^bb0(%x: !db.column<none>, %tag: !db.column<ui64>):
    %age = db.get_node_properties(%x, "age") : (!db.column<none>) -> !db.column<!storage.nullable<i64>>
    %c = db.constant(20 : i64)
    %gt = db.gt %age, %c : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>
    db.comprehension_yield %tag, %gt : !db.column<!storage.bool>
  } : (!db.column<!storage.nullable<!storage.list<!storage.node_id>>>) -> !db.column<!storage.bool>
  %m = db.filter(%ok, {%tgt}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%m) : !db.column<!storage.node_id>
  return
}
)mlir";

// The relationships are also returned, so the expansion outlives the test
const char* const relationshipsReadByTheOutput = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %src, %tgt, %paths = db.explore_paths(%n, {}) forward hops 1 to 2 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %edges = db.expand_path(%paths) kind edges : (!db.column<!storage.path_ref>) -> !db.column<!storage.list<!storage.edge_id>>
  %widened = db.to_nullable %edges : (!db.column<!storage.list<!storage.edge_id>>) -> !db.column<!storage.nullable<!storage.list<!storage.edge_id>>>
  %ok = db.list_predicate(%widened, {}) kind none {
  ^bb0(%x: !db.column<none>, %tag: !db.column<ui64>):
    %duration = db.get_edge_properties(%x, "duration") : (!db.column<none>) -> !db.column<!storage.nullable<i64>>
    %c = db.constant(200 : i64)
    %eq = db.eq %duration, %c : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>
    db.comprehension_yield %tag, %eq : !db.column<!storage.bool>
  } : (!db.column<!storage.nullable<!storage.list<!storage.edge_id>>>) -> !db.column<!storage.bool>
  %m:2 = db.filter(%ok, {%tgt, %edges}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>) -> (!db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>)
  db.output(%m#0, %m#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>
  return
}
)mlir";

TEST_F(ExploreListPredicateErasedExpansionTest, erasesTheRelationshipsOnlyTheTestRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(relationshipsReadByTheTestAlone);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFusion(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::ListPredicate>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ToNullable>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ExpandPath>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(ExploreListPredicateErasedExpansionTest, erasesTheNodesOnlyTheTestRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nodesReadByTheTestAlone);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFusion(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::ListPredicate>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ToNullable>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ExpandPath>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(ExploreListPredicateErasedExpansionTest, keepsTheRelationshipsTheOutputReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(relationshipsReadByTheOutput);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFusion(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::ListPredicate>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ToNullable>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ExpandPath>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}
