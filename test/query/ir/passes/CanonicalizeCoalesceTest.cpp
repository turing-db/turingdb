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

// A region of a db.coalesce yielding the null literal gives no row a value, so it goes. The
// regions after one yielding a literal that is not null see no row, so they go too where the
// result names its element. A coalesce left with one region is that region's value,
// wherever the value has its type.
class CanonicalizeCoalesceTest : public ::testing::Test {
protected:
    CanonicalizeCoalesceTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> canonicalize(const char* programText) {
        mlir::OwningOpRef<mlir::ModuleOp> module =
            mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        if (!module) {
            return {};
        }

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createCanonicalizeCoalesce());

        if (mlir::failed(passManager.run(*module)) || mlir::failed(mlir::verify(*module))) {
            return {};
        }

        return module;
    }

    // The op computing what region @param regionIndex of @param coalesce yields
    mlir::Operation* yieldingOp(mlir::db::Coalesce coalesce, size_t regionIndex) {
        mlir::Block& body = coalesce.getArguments()[regionIndex].front();
        mlir::db::CaseYield yield = mlir::cast<mlir::db::CaseYield>(body.getTerminator());

        return yield.getValue().getDefiningOp();
    }

    mlir::MLIRContext _context;
};

// MATCH (n) RETURN coalesce(null, n.age, null, n.dob, null)
const char* const coalesceAmongNullLiterals = R"mlir(
!nodes = !db.column<!storage.node_id>
!null = !db.column<!storage.nullable<none>>
!ages = !db.column<!storage.nullable<i64>>
!dates = !db.column<!storage.nullable<!storage.string>>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %r = db.coalesce({%n}) {
  ^bb0(%nIn: !nodes):
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  }, {
  ^bb0(%nIn: !nodes):
    %age = db.get_node_properties(%nIn, "age") : (!nodes) -> !ages
    db.case_yield %age : !ages
  }, {
  ^bb0(%nIn: !nodes):
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  }, {
  ^bb0(%nIn: !nodes):
    %dob = db.get_node_properties(%nIn, "dob") : (!nodes) -> !dates
    db.case_yield %dob : !dates
  }, {
  ^bb0(%nIn: !nodes):
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  } : (!nodes) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(CanonicalizeCoalesceTest, dropsTheRegionsYieldingTheNullLiteral) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = canonicalize(coalesceAmongNullLiterals);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);
    mlir::db::Coalesce coalesce = coalesces.front();
    ASSERT_EQ(coalesce.getArguments().size(), 2u);

    mlir::db::GetNodeProperties first = mlir::dyn_cast_or_null<mlir::db::GetNodeProperties>(yieldingOp(coalesce, 0));
    mlir::db::GetNodeProperties second = mlir::dyn_cast_or_null<mlir::db::GetNodeProperties>(yieldingOp(coalesce, 1));
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first.getProperty(), "age");
    EXPECT_EQ(second.getProperty(), "dob");
}

// MATCH (n) RETURN coalesce(null, n.dob)
const char* const coalesceLeftWithOneRegion = R"mlir(
!nodes = !db.column<!storage.node_id>
!null = !db.column<!storage.nullable<none>>
!dates = !db.column<!storage.nullable<!storage.string>>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %r = db.coalesce({%n}) {
  ^bb0(%nIn: !nodes):
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  }, {
  ^bb0(%nIn: !nodes):
    %dob = db.get_node_properties(%nIn, "dob") : (!nodes) -> !dates
    db.case_yield %dob : !dates
  } : (!nodes) -> !dates
  db.output(%r) : !dates
  return
}
)mlir";

TEST_F(CanonicalizeCoalesceTest, givesWayToTheValueOfItsOneRegion) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = canonicalize(coalesceLeftWithOneRegion);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::Coalesce>(*module), 0u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);

    mlir::db::GetNodeProperties read = outputs.front().getColumns().front().getDefiningOp<mlir::db::GetNodeProperties>();
    ASSERT_TRUE(read);
    EXPECT_EQ(read.getProperty(), "dob");
    EXPECT_TRUE(read.getInputNodes().getDefiningOp<mlir::db::ScanNodes>());
}

// RETURN coalesce(null, 1): the literal is no nullable column, unlike the coalesce
const char* const coalesceOfALiteralOfAnotherType = R"mlir(
!null = !db.column<!storage.nullable<none>>
!integers = !db.column<!storage.nullable<i64>>

func.func @main() {
  %r = db.coalesce({}) {
  ^bb0:
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  }, {
  ^bb0:
    %one = db.constant(1 : i64)
    db.case_yield %one : !db.column<i64>
  } : () -> !integers
  db.output(%r) : !integers
  return
}
)mlir";

TEST_F(CanonicalizeCoalesceTest, keepsTheOneRegionOfAValueOfAnotherType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = canonicalize(coalesceOfALiteralOfAnotherType);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);
    mlir::db::Coalesce coalesce = coalesces.front();
    ASSERT_EQ(coalesce.getArguments().size(), 1u);
    EXPECT_TRUE(mlir::isa_and_nonnull<mlir::db::ConstantOp>(yieldingOp(coalesce, 0)));
}

// MATCH (n) RETURN coalesce(n.dob, 'x', n.name)
const char* const coalesceBehindALiteral = R"mlir(
!nodes = !db.column<!storage.node_id>
!strings = !db.column<!storage.nullable<!storage.string>>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %r = db.coalesce({%n}) {
  ^bb0(%nIn: !nodes):
    %dob = db.get_node_properties(%nIn, "dob") : (!nodes) -> !strings
    db.case_yield %dob : !strings
  }, {
  ^bb0(%nIn: !nodes):
    %x = db.constant("x" : !storage.string)
    db.case_yield %x : !db.column<!storage.string>
  }, {
  ^bb0(%nIn: !nodes):
    %name = db.get_node_properties(%nIn, "name") : (!nodes) -> !strings
    db.case_yield %name : !strings
  } : (!nodes) -> !strings
  db.output(%r) : !strings
  return
}
)mlir";

TEST_F(CanonicalizeCoalesceTest, dropsTheRegionsAfterALiteralThatIsNotNull) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = canonicalize(coalesceBehindALiteral);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);
    mlir::db::Coalesce coalesce = coalesces.front();
    ASSERT_EQ(coalesce.getArguments().size(), 2u);
    EXPECT_TRUE(mlir::isa_and_nonnull<mlir::db::GetNodeProperties>(yieldingOp(coalesce, 0)));
    EXPECT_TRUE(mlir::isa_and_nonnull<mlir::db::ConstantOp>(yieldingOp(coalesce, 1)));
}

// MATCH (n) RETURN coalesce(n.age, 7, n.dob, 10 / n.age): lowering picks the element of the
// result out of every region's value, so the regions behind the literal stay
const char* const coalesceOfNoNamedElementBehindALiteral = R"mlir(
!nodes = !db.column<!storage.node_id>
!ages = !db.column<!storage.nullable<i64>>
!dates = !db.column<!storage.nullable<!storage.string>>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %ten = db.constant(10 : i64)
  %r = db.coalesce({%n}) {
  ^bb0(%nIn: !nodes):
    %age = db.get_node_properties(%nIn, "age") : (!nodes) -> !ages
    db.case_yield %age : !ages
  }, {
  ^bb0(%nIn: !nodes):
    %seven = db.constant(7 : i64)
    db.case_yield %seven : !db.column<i64>
  }, {
  ^bb0(%nIn: !nodes):
    %dob = db.get_node_properties(%nIn, "dob") : (!nodes) -> !dates
    db.case_yield %dob : !dates
  }, {
  ^bb0(%nIn: !nodes):
    %age = db.get_node_properties(%nIn, "age") : (!nodes) -> !ages
    %q = db.div %ten, %age : (!db.column<i64>, !ages) -> !db.column<none>
    db.case_yield %q : !db.column<none>
  } : (!nodes) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(CanonicalizeCoalesceTest, keepsTheRegionsAfterALiteralWhereTheResultNamesNoElement) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = canonicalize(coalesceOfNoNamedElementBehindALiteral);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);
    EXPECT_EQ(coalesces.front().getArguments().size(), 4u);
}

// RETURN coalesce(null, null): every region yields the null literal, so one of them stays
const char* const coalesceOfNullLiterals = R"mlir(
!null = !db.column<!storage.nullable<none>>

func.func @main() {
  %r = db.coalesce({}) {
  ^bb0:
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  }, {
  ^bb0:
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  } : () -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(CanonicalizeCoalesceTest, keepsOneRegionWhenEveryRegionYieldsTheNullLiteral) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = canonicalize(coalesceOfNullLiterals);
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);
    EXPECT_EQ(coalesces.front().getArguments().size(), 1u);
}

// UNWIND [0, 2] AS t RETURN coalesce(null, coalesce(null, 10 / t)): the inner coalesce is
// canonicalised in the region of the outer one, which then gives way to it
const char* const nestedCoalesce = R"mlir(
!null = !db.column<!storage.nullable<none>>
!integers = !db.column<i64>

func.func @main() {
  %t = db.unwind_const([0, 2]) : !integers
  %ten = db.constant(10 : i64)
  %r = db.coalesce({%t}) {
  ^bb0(%tOuter: !integers):
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null
  }, {
  ^bb0(%tOuter: !integers):
    %inner = db.coalesce({%tOuter}) {
    ^bb0(%tInner: !integers):
      %null = db.constant("" : !storage.nullable<none>)
      db.case_yield %null : !null
    }, {
    ^bb0(%tInner: !integers):
      %q = db.div %ten, %tInner : (!integers, !integers) -> !db.column<none>
      db.case_yield %q : !db.column<none>
    } : (!integers) -> !db.column<none>
    db.case_yield %inner : !db.column<none>
  } : (!integers) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(CanonicalizeCoalesceTest, canonicalizesANestedCoalesce) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = canonicalize(nestedCoalesce);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::Coalesce>(*module), 0u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);

    mlir::db::DivOp division = outputs.front().getColumns().front().getDefiningOp<mlir::db::DivOp>();
    ASSERT_TRUE(division);
    EXPECT_TRUE(division.getRhs().getDefiningOp<mlir::db::UnwindConst>());
}
