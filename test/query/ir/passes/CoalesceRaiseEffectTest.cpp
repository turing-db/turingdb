#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "IRConstantColumn.h"
#include "IRRaiseEffect.h"
#include "StorageDialect.h"

// A db.coalesce has the effects of its regions: one nothing reads stays where a row may
// raise on an argument, and goes where no argument can raise. %integer is a column of
// nullable integers, which a division by it raises on where a row holds 0.
class CoalesceRaiseEffectTest : public ::testing::Test {
protected:
    CoalesceRaiseEffectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses an unread coalesce whose last region yields @param lastArgument, computed over
    // %in, the row's integer
    mlir::Operation* parseUnreadCoalesce(std::string_view lastArgument) {
        std::string program = R"mlir(
!integers = !db.column<!storage.nullable<i64>>
!any = !db.column<none>

func.func @main(%integer: !integers) {
  %three = db.constant(3 : i64)
  %r = db.coalesce({%integer}) {
  ^bb0(%in: !integers):
    db.case_yield %in : !integers
  }, {
  ^bb0(%in: !integers):
    )mlir";

        program += lastArgument;
        program += R"mlir(
    db.case_yield %last : !any
  } : (!integers) -> !any
  return
}
)mlir";

        _module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        if (!_module) {
            return nullptr;
        }

        mlir::func::FuncOp main = _module->lookupSymbol<mlir::func::FuncOp>("main");

        return main.getBody().front().getTerminator()->getPrevNode();
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
};

TEST_F(CoalesceRaiseEffectTest, keepsAnUnreadCoalesceWhoseArgumentMayRaise) {
    mlir::Operation* const coalesce = parseUnreadCoalesce("%last = db.div %three, %in : (!db.column<i64>, !integers) -> !any");
    ASSERT_TRUE(coalesce);

    EXPECT_FALSE(mlir::isOpTriviallyDead(coalesce));
    EXPECT_FALSE(mlir::isSpeculatable(coalesce));
    EXPECT_FALSE(db::runsWithoutRaising(coalesce));
    EXPECT_TRUE(db::hasNoEffectButRaising(coalesce));
}

TEST_F(CoalesceRaiseEffectTest, dropsAnUnreadCoalesceNoArgumentRaisesOn) {
    mlir::Operation* const coalesce = parseUnreadCoalesce("%last = db.div %in, %three : (!integers, !db.column<i64>) -> !any");
    ASSERT_TRUE(coalesce);

    EXPECT_TRUE(mlir::isOpTriviallyDead(coalesce));
    EXPECT_TRUE(mlir::isSpeculatable(coalesce));
    EXPECT_TRUE(db::runsWithoutRaising(coalesce));
    EXPECT_TRUE(db::hasNoEffectButRaising(coalesce));
}

// RETURN coalesce(null, 3)
const char* const coalesceOverConstants = R"mlir(
func.func @main() {
  %r = db.coalesce({}) {
  ^bb0:
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !db.column<!storage.nullable<none>>
  }, {
  ^bb0:
    %three = db.constant(3 : i64)
    db.case_yield %three : !db.column<i64>
  } : () -> !db.column<!storage.nullable<i64>>
  db.output(%r) : !db.column<!storage.nullable<i64>>
  return
}
)mlir";

TEST_F(CoalesceRaiseEffectTest, holdsOneValueForEveryRowOverConstants) {
    _module = mlir::parseSourceString<mlir::ModuleOp>(coalesceOverConstants, mlir::ParserConfig(&_context));
    ASSERT_TRUE(_module);

    mlir::Operation* coalesce = nullptr;
    _module->walk([&coalesce](mlir::db::Coalesce op) {
        coalesce = op.getOperation();
    });
    ASSERT_TRUE(coalesce);

    EXPECT_TRUE(db::yieldsConstantColumn(coalesce->getResult(0)));
}
