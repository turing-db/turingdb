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

// A list comprehension, a list predicate and a reduce repeat every carried column once per
// element of its row, so a column the body does not read is copied for no reader. The trim
// drops it from the operands and from the body's arguments.
class TrimListComprehensionTest : public ::testing::Test {
protected:
    TrimListComprehensionTest() {
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

// MATCH (n) RETURN n.name, [x IN [1, 2] WHERE x > n.age | x]
const char* const comprehensionReadingTheAge = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %xs = db.constant([1, 2])
  %r = db.list_comprehension(%xs, {%n, %name, %age}) {
  ^bb0(%x: !db.column<none>, %tag: !db.column<ui64>, %nIn: !db.column<!storage.node_id>, %nameIn: !db.column<none>, %ageIn: !db.column<none>):
    %keep = db.gt %x, %ageIn : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
    %kept:2 = db.filter(%keep, {%x, %tag}) : (!db.column<!storage.bool>, !db.column<none>, !db.column<ui64>) -> (!db.column<none>, !db.column<ui64>)
    db.comprehension_yield %kept#1, %kept#0 : !db.column<none>
  } : (!db.column<!storage.list<i64>>, !db.column<!storage.node_id>, !db.column<none>, !db.column<none>) -> !db.column<none>
  db.output(%name, %r) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(TrimListComprehensionTest, dropsTheCarriedColumnsAComprehensionDoesNotRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(comprehensionReadingTheAge);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ListComprehension> comprehensions = collect<mlir::db::ListComprehension>(*module);
    ASSERT_EQ(comprehensions.size(), 1u);

    const mlir::OperandRange carried = comprehensions.front().getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_EQ(carried.front().getDefiningOp<mlir::db::GetNodeProperties>().getProperty(), "age");
}

// MATCH (n)-->(m) WHERE any(y IN [30, 32] WHERE y = m.age) RETURN n
const char* const predicateReadingTheTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ys = db.constant([30, 32])
  %ok = db.list_predicate(%ys, {%s, %e, %t}) kind any {
  ^bb0(%y: !db.column<none>, %tag: !db.column<ui64>, %sIn: !db.column<!storage.node_id>, %eIn: !db.column<!storage.edge_id>, %tIn: !db.column<!storage.node_id>):
    %age = db.get_node_properties(%tIn, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %p = db.eq %y, %age : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
    db.comprehension_yield %tag, %p : !db.column<!storage.bool>
  } : (!db.column<!storage.list<i64>>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %sk = db.filter(%ok, {%s}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sk) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(TrimListComprehensionTest, dropsTheCarriedColumnsAPredicateDoesNotRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(predicateReadingTheTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ListPredicate> predicates = collect<mlir::db::ListPredicate>(*module);
    ASSERT_EQ(predicates.size(), 1u);

    const mlir::OperandRange carried = predicates.front().getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);

    const mlir::OpResult target = mlir::dyn_cast<mlir::OpResult>(carried.front());
    ASSERT_TRUE(target);
    EXPECT_TRUE(mlir::isa<mlir::db::GetOutEdges>(target.getOwner()));
    EXPECT_EQ(target.getResultNumber(), 3u);
}

// MATCH (n) RETURN n.name, reduce(s = 0, z IN [1, 2] | s + z + n.age)
const char* const reduceReadingTheNode = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %zero = db.constant(0 : i64)
  %zs = db.constant([1, 2])
  %r = db.reduce(%zs, %zero, {%name, %n}) {
  ^bb0(%z: !db.column<none>, %s: !db.column<none>, %nameIn: !db.column<none>, %nIn: !db.column<!storage.node_id>):
    %sum = db.add %s, %z : (!db.column<none>, !db.column<none>) -> !db.column<none>
    %age = db.get_node_properties(%nIn, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %next = db.add %sum, %age : (!db.column<none>, !db.column<none>) -> !db.column<none>
    db.reduce_yield %next : !db.column<none>
  } : (!db.column<!storage.list<i64>>, !db.column<i64>, !db.column<none>, !db.column<!storage.node_id>) -> !db.column<none>
  db.output(%name, %r) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(TrimListComprehensionTest, dropsTheCarriedColumnsAReduceDoesNotRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(reduceReadingTheNode);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::Reduce> reduces = collect<mlir::db::Reduce>(*module);
    ASSERT_EQ(reduces.size(), 1u);

    const mlir::OperandRange carried = reduces.front().getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_TRUE(carried.front().getDefiningOp<mlir::db::ScanNodes>());
}

// MATCH (n) RETURN n.name, [x IN [n.name, 'x'] | x]
const char* const comprehensionOverARowList = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %x = db.constant("x" : !storage.string)
  %xs = db.make_list(%name, %x) : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.list<none>>
  %r = db.list_comprehension(%xs, {%n}) {
  ^bb0(%element: !db.column<none>, %tag: !db.column<ui64>, %nIn: !db.column<!storage.node_id>):
    db.comprehension_yield %tag, %element : !db.column<none>
  } : (!db.column<!storage.list<none>>, !db.column<!storage.node_id>) -> !db.column<none>
  db.output(%name, %r) : !db.column<none>, !db.column<none>
  return
}
)mlir";

// A source with one list per row sizes the op on its own, so nothing has to be carried
TEST_F(TrimListComprehensionTest, carriesNothingOverARowList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(comprehensionOverARowList);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ListComprehension> comprehensions = collect<mlir::db::ListComprehension>(*module);
    ASSERT_EQ(comprehensions.size(), 1u);

    mlir::db::ListComprehension comprehension = comprehensions.front();
    EXPECT_TRUE(comprehension.getColumnsToFilter().empty());
    EXPECT_EQ(comprehension.getBody().front().getNumArguments(), 2u);
}

// MATCH (n) RETURN n.name, [x IN [1, 2] | x * 10]
const char* const comprehensionOverAConstantList = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %xs = db.constant([1, 2])
  %r = db.list_comprehension(%xs, {%name, %n}) {
  ^bb0(%x: !db.column<none>, %tag: !db.column<ui64>, %nameIn: !db.column<none>, %nIn: !db.column<!storage.node_id>):
    %ten = db.constant(10 : i64)
    %times = db.mul %x, %ten : (!db.column<none>, !db.column<i64>) -> !db.column<none>
    db.comprehension_yield %tag, %times : !db.column<none>
  } : (!db.column<!storage.list<i64>>, !db.column<none>, !db.column<!storage.node_id>) -> !db.column<none>
  db.output(%name, %r) : !db.column<none>, !db.column<none>
  return
}
)mlir";

// A constant source is laid out over the rows of a carried column during lowering, so one
// survives even when the body reads none
TEST_F(TrimListComprehensionTest, keepsOneColumnToLayAConstantListOver) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(comprehensionOverAConstantList);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ListComprehension> comprehensions = collect<mlir::db::ListComprehension>(*module);
    ASSERT_EQ(comprehensions.size(), 1u);

    const mlir::OperandRange carried = comprehensions.front().getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_TRUE(carried.front().getDefiningOp<mlir::db::GetNodeProperties>());
}

// MATCH (n)-->(m) RETURN [x IN [1, 2] WHERE x > 1 | [y IN [3] WHERE y > x | n.name]], as
// the generator emits it: the outer WHERE carries every argument for the inner comprehension
const char* const nestedComprehensions = R"mlir(
func.func @main() {
  %0, %1, %2, %3 = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %9 = db.constant([1, 2])
  %10 = db.list_comprehension(%9, {%3, %1, %0, %2}) {
  ^bb0(%arg0: !db.column<none>, %arg1: !db.column<ui64>, %arg2: !db.column<!storage.node_id>, %arg3: !db.column<!storage.edge_id>, %arg4: !db.column<!storage.node_id>, %arg5: !db.column<!storage.edge_type_id>):
    %11 = db.constant(1 : i64)
    %12 = db.gt %arg0, %11 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    %13:6 = db.filter(%12, {%arg0, %arg1, %arg2, %arg3, %arg4, %arg5}) : (!db.column<!storage.bool>, !db.column<none>, !db.column<ui64>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<none>, !db.column<ui64>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
    %14 = db.constant([3])
    %15 = db.list_comprehension(%14, {%13#2, %13#3, %13#4, %13#5, %13#0}) {
    ^bb0(%arg6: !db.column<none>, %arg7: !db.column<ui64>, %arg8: !db.column<!storage.node_id>, %arg9: !db.column<!storage.edge_id>, %arg10: !db.column<!storage.node_id>, %arg11: !db.column<!storage.edge_type_id>, %arg12: !db.column<none>):
      %16 = db.gt %arg6, %arg12 : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
      %17:2 = db.filter(%16, {%arg7, %arg10}) : (!db.column<!storage.bool>, !db.column<ui64>, !db.column<!storage.node_id>) -> (!db.column<ui64>, !db.column<!storage.node_id>)
      %18 = db.get_node_properties(%17#1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
      db.comprehension_yield %17#0, %18 : !db.column<none>
    } : (!db.column<!storage.list<i64>>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<none>) -> !db.column<none>
    db.comprehension_yield %13#1, %15 : !db.column<none>
  } : (!db.column<!storage.list<i64>>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> !db.column<none>
  db.output(%10) : !db.column<none>
  return
}
)mlir";

// The inner comprehension reads n and x alone. Once it drops the rest, the outer WHERE
// carries them for no reader, and once that is cut the outer comprehension carries n alone,
// all in the same run
TEST_F(TrimListComprehensionTest, trimsNestedComprehensionsInOneRun) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nestedComprehensions);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ListComprehension> comprehensions = collect<mlir::db::ListComprehension>(*module);
    ASSERT_EQ(comprehensions.size(), 2u);

    mlir::db::ListComprehension inner = comprehensions[0];
    mlir::db::ListComprehension outer = comprehensions[1];

    EXPECT_EQ(inner.getColumnsToFilter().size(), 2u);

    const mlir::OperandRange outerCarried = outer.getColumnsToFilter();
    ASSERT_EQ(outerCarried.size(), 1u);

    const mlir::OpResult source = mlir::dyn_cast<mlir::OpResult>(outerCarried.front());
    ASSERT_TRUE(source);
    EXPECT_TRUE(mlir::isa<mlir::db::ScanEdges>(source.getOwner()));
    EXPECT_EQ(source.getResultNumber(), 0u);
}
