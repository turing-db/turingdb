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

// A db.lazy_case cuts every carried column down to the rows each branch sees, so a column
// no region reads is filtered for no reader. The trim drops it from the operands and from
// the arguments of every region.
class TrimLazyCaseTest : public ::testing::Test {
protected:
    TrimLazyCaseTest() {
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

    void expectRegionArguments(mlir::db::LazyCase caseOp) {
        const mlir::OperandRange carried = caseOp.getColumnsToFilter();

        for (mlir::Region& region : caseOp.getBranches()) {
            mlir::Block& block = region.front();
            ASSERT_EQ(block.getNumArguments(), carried.size());

            for (size_t argumentIndex = 0; argumentIndex < carried.size(); argumentIndex++) {
                EXPECT_EQ(block.getArgument(argumentIndex).getType(), carried[argumentIndex].getType());
            }
        }
    }

    mlir::MLIRContext _context;
};

// MATCH (n) RETURN n.name, CASE WHEN n.age = 0 THEN 0 ELSE 10 / n.age END
const char* const caseReadingTheAgeAlone = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %zero = db.constant(0 : i64)
  %ten = db.constant(10 : i64)
  %r = db.lazy_case({%n, %age, %name}) {
  ^bb0(%nIn: !db.column<!storage.node_id>, %ageIn: !db.column<none>, %nameIn: !db.column<none>):
    %c = db.eq %ageIn, %zero : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    db.case_yield %c : !db.column<!storage.bool>
  }, {
  ^bb0(%nIn: !db.column<!storage.node_id>, %ageIn: !db.column<none>, %nameIn: !db.column<none>):
    db.case_yield %zero : !db.column<i64>
  }, {
  ^bb0(%nIn: !db.column<!storage.node_id>, %ageIn: !db.column<none>, %nameIn: !db.column<none>):
    %q = db.div %ten, %ageIn : (!db.column<i64>, !db.column<none>) -> !db.column<none>
    db.case_yield %q : !db.column<none>
  } : (!db.column<!storage.node_id>, !db.column<none>, !db.column<none>) -> !db.column<none>
  db.output(%name, %r) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(TrimLazyCaseTest, dropsTheCarriedColumnsNoRegionReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(caseReadingTheAgeAlone);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::LazyCase> cases = collect<mlir::db::LazyCase>(*module);
    ASSERT_EQ(cases.size(), 1u);
    mlir::db::LazyCase caseOp = cases.front();

    const mlir::OperandRange carried = caseOp.getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_EQ(carried.front().getDefiningOp<mlir::db::GetNodeProperties>().getProperty(), "age");

    expectRegionArguments(caseOp);
}

// UNWIND [0, 2] AS t RETURN CASE t WHEN 0 THEN 0 ELSE 10 / t END, which carries t as the
// variable and again as the subject
const char* const caseCarryingItsSubjectTwice = R"mlir(
func.func @main() {
  %t = db.unwind_const([0, 2]) : !db.column<i64>
  %zero = db.constant(0 : i64)
  %ten = db.constant(10 : i64)
  %r = db.lazy_case({%t, %t}) {
  ^bb0(%tIn: !db.column<i64>, %subject: !db.column<i64>):
    %c = db.eq %subject, %zero : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
    db.case_yield %c : !db.column<!storage.bool>
  }, {
  ^bb0(%tIn: !db.column<i64>, %subject: !db.column<i64>):
    db.case_yield %zero : !db.column<i64>
  }, {
  ^bb0(%tIn: !db.column<i64>, %subject: !db.column<i64>):
    %q = db.div %ten, %tIn : (!db.column<i64>, !db.column<i64>) -> !db.column<none>
    db.case_yield %q : !db.column<none>
  } : (!db.column<i64>, !db.column<i64>) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(TrimLazyCaseTest, carriesAColumnOnce) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(caseCarryingItsSubjectTwice);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::LazyCase> cases = collect<mlir::db::LazyCase>(*module);
    ASSERT_EQ(cases.size(), 1u);
    mlir::db::LazyCase caseOp = cases.front();

    const mlir::OperandRange carried = caseOp.getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_TRUE(carried.front().getDefiningOp<mlir::db::UnwindConst>());

    expectRegionArguments(caseOp);
}

// MATCH (n) RETURN n.name, CASE WHEN true THEN 1 ELSE 2 END
const char* const caseReadingNoCarriedColumn = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %r = db.lazy_case({%n, %name}) {
  ^bb0(%nIn: !db.column<!storage.node_id>, %nameIn: !db.column<none>):
    %c = db.constant(true)
    db.case_yield %c : !db.column<i1>
  }, {
  ^bb0(%nIn: !db.column<!storage.node_id>, %nameIn: !db.column<none>):
    %one = db.constant(1 : i64)
    db.case_yield %one : !db.column<i64>
  }, {
  ^bb0(%nIn: !db.column<!storage.node_id>, %nameIn: !db.column<none>):
    %two = db.constant(2 : i64)
    db.case_yield %two : !db.column<i64>
  } : (!db.column<!storage.node_id>, !db.column<none>) -> !db.column<none>
  db.output(%name, %r) : !db.column<none>, !db.column<none>
  return
}
)mlir";

// The lowering sizes the CASE by a carried column, so one survives even when no region
// reads it, as a filter keeps one to be sized by
TEST_F(TrimLazyCaseTest, keepsOneColumnToBeSizedBy) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(caseReadingNoCarriedColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::LazyCase> cases = collect<mlir::db::LazyCase>(*module);
    ASSERT_EQ(cases.size(), 1u);
    mlir::db::LazyCase caseOp = cases.front();

    const mlir::OperandRange carried = caseOp.getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_TRUE(carried.front().getDefiningOp<mlir::db::ScanNodes>());

    expectRegionArguments(caseOp);
}

// MATCH (n) RETURN CASE WHEN n.age = 0 THEN 0 ELSE CASE WHEN n.age = 1 THEN 1 ELSE 10 / n.age END END
const char* const nestedCases = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %zero = db.constant(0 : i64)
  %one = db.constant(1 : i64)
  %ten = db.constant(10 : i64)
  %r = db.lazy_case({%n, %age}) {
  ^bb0(%nIn: !db.column<!storage.node_id>, %ageIn: !db.column<none>):
    %c = db.eq %ageIn, %zero : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    db.case_yield %c : !db.column<!storage.bool>
  }, {
  ^bb0(%nIn: !db.column<!storage.node_id>, %ageIn: !db.column<none>):
    db.case_yield %zero : !db.column<i64>
  }, {
  ^bb0(%nIn: !db.column<!storage.node_id>, %ageIn: !db.column<none>):
    %inner = db.lazy_case({%nIn, %ageIn}) {
    ^bb0(%nInner: !db.column<!storage.node_id>, %ageInner: !db.column<none>):
      %d = db.eq %ageInner, %one : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
      db.case_yield %d : !db.column<!storage.bool>
    }, {
    ^bb0(%nInner: !db.column<!storage.node_id>, %ageInner: !db.column<none>):
      db.case_yield %one : !db.column<i64>
    }, {
    ^bb0(%nInner: !db.column<!storage.node_id>, %ageInner: !db.column<none>):
      %q = db.div %ten, %ageInner : (!db.column<i64>, !db.column<none>) -> !db.column<none>
      db.case_yield %q : !db.column<none>
    } : (!db.column<!storage.node_id>, !db.column<none>) -> !db.column<none>
    db.case_yield %inner : !db.column<none>
  } : (!db.column<!storage.node_id>, !db.column<none>) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

// The node is carried into the inner CASE and read by nothing in it: once the inner one
// drops it, the outer one's copy is unread too, and the same run drops that
TEST_F(TrimLazyCaseTest, trimsANestedCaseInOneRun) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nestedCases);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::LazyCase> cases = collect<mlir::db::LazyCase>(*module);
    ASSERT_EQ(cases.size(), 2u);

    for (mlir::db::LazyCase caseOp : cases) {
        const mlir::OperandRange carried = caseOp.getColumnsToFilter();
        ASSERT_EQ(carried.size(), 1u);
        const mlir::db::ColumnType keptType = mlir::cast<mlir::db::ColumnType>(carried.front().getType());
        EXPECT_TRUE(mlir::isa<mlir::NoneType>(keptType.getType()));

        expectRegionArguments(caseOp);
    }
}
