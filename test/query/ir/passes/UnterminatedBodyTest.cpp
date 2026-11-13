#include <gtest/gtest.h>

#include <stddef.h>

#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBInterfaces.h"
#include "DBOps.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

// Codegen asks what an op may raise while the body holding it, or one the op reads out of, is
// still being built and ends on no terminator. Whatever reads a region's yield then answers
// that it does not know: a carrier names no column, a key read may raise.
class UnterminatedBodyTest : public ::testing::Test {
protected:
    UnterminatedBodyTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(std::string_view programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    // Erases the terminator of every region of @param op, as if codegen had yet to end them
    static void unterminate(mlir::Operation* op) {
        for (mlir::Region& region : op->getRegions()) {
            mlir::Block& body = region.front();
            body.back().erase();
        }
    }

    static mlir::db::StaticMapKey keyReadOn(mlir::ModuleOp module, llvm::StringRef key) {
        mlir::db::StaticMapKey found;
        module.walk([&found, key](mlir::db::StaticMapKey keyRead) {
            if (keyRead.getKey() == key) {
                found = keyRead;
            }
        });

        return found;
    }

    mlir::MLIRContext _context;
};

// UNWIND [{s: {t: 1}}] AS row RETURN reduce(acc = row, x IN [1, 2] | CASE WHEN acc.s['t'] = 1 THEN acc ELSE row END)
const char* const keyReadOffAnAccumulator = R"mlir(
func.func @main() {
  %0 = db.unwind_const([{s = {t = 1 : i64}}]) : !db.column<!storage.any>
  %1 = db.constant([1, 2])
  %2 = db.reduce(%1, %0, {%0}) {
  ^bb0(%arg0: !db.column<none>, %arg1: !db.column<none>, %arg2: !db.column<!storage.any>):
    %3 = db.lazy_case({%arg2, %arg1}) {
    ^bb0(%arg3: !db.column<!storage.any>, %arg4: !db.column<none>):
      %4 = db.static_map_key %arg4 key "s" : (!db.column<none>) -> !db.column<!storage.any>
      %5 = db.constant("t" : !storage.string)
      %6 = db.dynamic_map_key %4, %5 : (!db.column<!storage.any>, !db.column<!storage.string>) -> !db.column<!storage.any>
      %7 = db.constant(1 : i64)
      %8 = db.eq %6, %7 : (!db.column<!storage.any>, !db.column<i64>) -> !db.column<!storage.bool>
      db.case_yield %8 : !db.column<!storage.bool>
    }, {
    ^bb0(%arg3: !db.column<!storage.any>, %arg4: !db.column<none>):
      db.case_yield %arg4 : !db.column<none>
    }, {
    ^bb0(%arg3: !db.column<!storage.any>, %arg4: !db.column<none>):
      db.case_yield %arg3 : !db.column<!storage.any>
    } : (!db.column<!storage.any>, !db.column<none>) -> !db.column<none>
    db.reduce_yield %3 : !db.column<none>
  } : (!db.column<!storage.list<i64>>, !db.column<!storage.any>, !db.column<!storage.any>) -> !db.column<none>
  db.output(%2) : !db.column<none>
  return
}
)mlir";

// UNWIND [{s: {t: 1}}] AS row RETURN allReduce(acc = row, x IN [1, 2] | acc, acc.s['t'] = 1)
const char* const keyReadOffAnAllReduceAccumulator = R"mlir(
func.func @main() {
  %0 = db.unwind_const([{s = {t = 1 : i64}}]) : !db.column<!storage.any>
  %1 = db.constant([1, 2])
  %2 = db.all_reduce(%1, %0, {}) {
  ^bb0(%arg0: !db.column<none>, %arg1: !db.column<none>):
    db.reduce_yield %arg1 : !db.column<none>
  } predicate {
  ^bb0(%arg0: !db.column<none>, %arg1: !db.column<none>):
    %3 = db.static_map_key %arg1 key "s" : (!db.column<none>) -> !db.column<!storage.any>
    %4 = db.constant("t" : !storage.string)
    %5 = db.dynamic_map_key %3, %4 : (!db.column<!storage.any>, !db.column<!storage.string>) -> !db.column<!storage.any>
    %6 = db.constant(1 : i64)
    %7 = db.eq %5, %6 : (!db.column<!storage.any>, !db.column<i64>) -> !db.column<!storage.bool>
    db.all_reduce_yield %7 : !db.column<!storage.bool>
  } : (!db.column<!storage.list<i64>>, !db.column<!storage.any>) -> !db.column<!storage.bool>
  db.output(%2) : !db.column<!storage.bool>
  return
}
)mlir";

// UNWIND [[{a: 1}]] AS l RETURN [x IN l | x][0].a
const char* const keyReadOffAListedValue = R"mlir(
func.func @main() {
  %0 = db.unwind_const([[{a = 1 : i64}]]) : !db.column<!storage.list<!storage.any>>
  %1 = db.list_comprehension(%0, {}) {
  ^bb0(%arg0: !db.column<none>, %arg1: !db.column<ui64>):
    db.comprehension_yield %arg1, %arg0 : !db.column<none>
  } : (!db.column<!storage.list<!storage.any>>) -> !db.column<none>
  %2 = db.constant(0 : i64)
  %3 = db.list_index %1, %2 : (!db.column<none>, !db.column<i64>) -> !db.column<none>
  %4 = db.static_map_key %3 key "a" : (!db.column<none>) -> !db.column<!storage.any>
  db.output(%4) : !db.column<!storage.any>
  return
}
)mlir";

// UNWIND [{a: 1}, null] AS m RETURN coalesce(m, {a: 2}).a
const char* const keyReadOffACoalesce = R"mlir(
func.func @main() {
  %0 = db.unwind_const([{a = 1 : i64}, unit]) : !db.column<!storage.any>
  %1 = db.coalesce({%0}) {
  ^bb0(%arg0: !db.column<!storage.any>):
    db.case_yield %arg0 : !db.column<!storage.any>
  }, {
  ^bb0(%arg0: !db.column<!storage.any>):
    %3 = db.constant({a = 2 : i64})
    db.case_yield %3 : !db.column<!storage.map>
  } : (!db.column<!storage.any>) -> !db.column<none>
  %2 = db.static_map_key %1 key "a" : (!db.column<none>) -> !db.column<!storage.any>
  db.output(%2) : !db.column<!storage.any>
  return
}
)mlir";

// MATCH (a:Person), (b:Interest) RETURN a, b
const char* const crossProduct = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  db.output(%p#0, %p#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person) OPTIONAL MATCH (n)-->(m) RETURN n, m, and the same pattern in a CALL body
// carrying its scope, and in a COLLECT and a pattern comprehension
const char* const subqueries = R"mlir(
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!etypes = !db.column<!storage.edge_type_id>
!tags = !db.column<ui64>
!names = !db.column<!storage.nullable<!storage.string>>

func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !nodes
  %on, %om = db.optional_match(%n) {
  ^bb0(%nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %mTag = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    db.optional_yield %mTag, {%s, %m} : {!nodes, !nodes}
  } : (!nodes) -> (!nodes, !nodes)
  %cn, %cm = db.call_subquery(%n) carries_scope {
  ^bb0(%nIn: !nodes):
    %s, %e, %et, %m = db.get_out_edges(%nIn, {}) : (!nodes) -> (!nodes, !edges, !etypes, !nodes)
    db.subquery_yield {%s, %m} : {!nodes, !nodes}
  } : (!nodes) -> (!nodes, !nodes)
  %collected = db.collect_subquery(%n) carries_scope {
  ^bb0(%nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %mTag = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %name = db.get_node_properties(%m, "name") : (!nodes) -> !names
    db.collect_subquery_yield tag %mTag, %name, {} : !names
  } : (!nodes) -> !db.column<none>
  %listed = db.pattern_comprehension(%n) {
  ^bb0(%nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %mTag = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %name = db.get_node_properties(%m, "name") : (!nodes) -> !names
    db.comprehension_yield %mTag, %name : !names
  } : (!nodes) -> !db.column<none>
  db.output(%on, %om, %cn, %cm, %collected, %listed) : !nodes, !nodes, !nodes, !nodes, !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(UnterminatedBodyTest, takesAKeyReadOffAnUnfinishedReduceAsMayRaise) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(keyReadOffAnAccumulator);
    ASSERT_TRUE(module);

    mlir::db::StaticMapKey keyRead = keyReadOn(*module, "s");
    ASSERT_TRUE(keyRead);
    EXPECT_TRUE(mlir::isSpeculatable(keyRead));

    llvm::SmallVector<mlir::db::Reduce> reduces = collect<mlir::db::Reduce>(*module);
    ASSERT_EQ(reduces.size(), 1u);
    mlir::db::Reduce reduce = reduces.front();

    reduce.getBody().front().back().erase();

    EXPECT_FALSE(mlir::isSpeculatable(keyRead));

    llvm::SmallVector<mlir::Value> accumulated;
    reduce.collectAccumulatedValues(reduce.getBody().front().getArgument(1), accumulated);
    EXPECT_TRUE(accumulated.empty());

    reduce.collectAccumulatedValues(reduce.getResult(), accumulated);
    EXPECT_TRUE(accumulated.empty());
}

TEST_F(UnterminatedBodyTest, takesAKeyReadOffAnUnfinishedAllReduceAsMayRaise) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(keyReadOffAnAllReduceAccumulator);
    ASSERT_TRUE(module);

    mlir::db::StaticMapKey keyRead = keyReadOn(*module, "s");
    ASSERT_TRUE(keyRead);
    EXPECT_TRUE(mlir::isSpeculatable(keyRead));

    llvm::SmallVector<mlir::db::AllReduce> allReduces = collect<mlir::db::AllReduce>(*module);
    ASSERT_EQ(allReduces.size(), 1u);
    mlir::db::AllReduce allReduce = allReduces.front();

    allReduce.getBody().front().back().erase();

    EXPECT_FALSE(mlir::isSpeculatable(keyRead));

    llvm::SmallVector<mlir::Value> accumulated;
    allReduce.collectAccumulatedValues(allReduce.getPredicate().front().getArgument(1), accumulated);
    EXPECT_TRUE(accumulated.empty());
}

TEST_F(UnterminatedBodyTest, takesAKeyReadOffAnUnfinishedListComprehensionAsMayRaise) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(keyReadOffAListedValue);
    ASSERT_TRUE(module);

    mlir::db::StaticMapKey keyRead = keyReadOn(*module, "a");
    ASSERT_TRUE(keyRead);
    EXPECT_TRUE(mlir::isSpeculatable(keyRead));

    llvm::SmallVector<mlir::db::ListComprehension> comprehensions = collect<mlir::db::ListComprehension>(*module);
    ASSERT_EQ(comprehensions.size(), 1u);
    mlir::db::ListComprehension comprehension = comprehensions.front();

    unterminate(comprehension);

    EXPECT_FALSE(mlir::isSpeculatable(keyRead));
    EXPECT_FALSE(comprehension.getListedValue(comprehension->getResult(0)));
}

TEST_F(UnterminatedBodyTest, takesAKeyReadOffAnUnfinishedCoalesceAsMayRaise) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(keyReadOffACoalesce);
    ASSERT_TRUE(module);

    mlir::db::StaticMapKey keyRead = keyReadOn(*module, "a");
    ASSERT_TRUE(keyRead);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);
    mlir::db::Coalesce coalesce = coalesces.front();

    coalesce.getArguments().back().front().back().erase();

    EXPECT_FALSE(mlir::isSpeculatable(keyRead));

    llvm::SmallVector<mlir::Value> carried;
    coalesce.collectCarriedColumns(coalesce->getResult(0), carried);
    EXPECT_TRUE(carried.empty());
}

TEST_F(UnterminatedBodyTest, findsNoYieldedColumnOfAnUnfinishedProduct) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProduct);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);
    mlir::db::CrossProduct product = products.front();

    product.getLeftFactor().front().back().erase();

    size_t resultOffset = 0;
    EXPECT_FALSE(product.getYieldedResultOffset(product.getRightFactor(), resultOffset));
    EXPECT_TRUE(mlir::db::yieldedResultsOf(product.getLeftFactor()).empty());
    EXPECT_TRUE(mlir::db::yieldedColumnsOf(product.getLeftFactor().front()).empty());

    for (mlir::OpResult result : product->getResults()) {
        EXPECT_FALSE(product.getRowSource(result));
        EXPECT_FALSE(mlir::db::yieldedColumnOf(result));
        EXPECT_EQ(mlir::db::yieldingRegionOf(result), nullptr);
    }
}

TEST_F(UnterminatedBodyTest, findsNoCarriedOrListedColumnOfAnUnfinishedSubquery) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(subqueries);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::OptionalMatch> optionals = collect<mlir::db::OptionalMatch>(*module);
    llvm::SmallVector<mlir::db::CallSubquery> calls = collect<mlir::db::CallSubquery>(*module);
    llvm::SmallVector<mlir::db::CollectSubquery> collects = collect<mlir::db::CollectSubquery>(*module);
    llvm::SmallVector<mlir::db::PatternComprehension> comprehensions = collect<mlir::db::PatternComprehension>(*module);
    ASSERT_EQ(optionals.size(), 1u);
    ASSERT_EQ(calls.size(), 1u);
    ASSERT_EQ(collects.size(), 1u);
    ASSERT_EQ(comprehensions.size(), 1u);

    mlir::db::OptionalMatch optional = optionals.front();
    mlir::db::CallSubquery call = calls.front();
    mlir::db::CollectSubquery collected = collects.front();
    mlir::db::PatternComprehension comprehension = comprehensions.front();

    unterminate(optional);
    unterminate(call);
    unterminate(collected);
    unterminate(comprehension);

    for (mlir::Operation* carrier : {optional.getOperation(), call.getOperation()}) {
        for (mlir::OpResult result : carrier->getResults()) {
            llvm::SmallVector<mlir::Value> carried;
            mlir::cast<mlir::db::ColumnCarrierOpInterface>(carrier).collectCarriedColumns(result, carried);
            EXPECT_TRUE(carried.empty()) << carrier->getName().getStringRef().str();
        }
    }

    EXPECT_FALSE(collected.getListedValue(collected->getResult(0)));
    EXPECT_FALSE(comprehension.getListedValue(comprehension->getResult(0)));
}
