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
#include "StorageDialect.h"

using mlir::Speculation::NotSpeculatable;
using mlir::Speculation::Speculatable;

// The opposite of the lowest int64, of an unsigned count past it, and of the lowest
// duration is an overflow, which raises: a negation is speculatable over doubles alone.
class NegationSpeculatabilityTest : public ::testing::Test {
protected:
    NegationSpeculatabilityTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Operation* lastOp(std::string_view ops) {
        std::string program = R"mlir(
!i64s = !db.column<i64>
!u64s = !db.column<ui64>
!reals = !db.column<f64>
!durations = !db.column<!storage.duration>
!unresolved = !db.column<none>

func.func @main(%integer: !i64s, %count: !u64s, %real: !reals, %duration: !durations) {
  %lowest = db.constant(-9223372036854775808 : i64)
  )mlir";

        program += ops;
        program += "\n  return\n}\n";

        _module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(_module) << ops;
        if (!_module) {
            return nullptr;
        }

        mlir::func::FuncOp main = _module->lookupSymbol<mlir::func::FuncOp>("main");
        return main.getBody().front().getTerminator()->getPrevNode();
    }

    mlir::Speculation::Speculatability speculatabilityOf(std::string_view ops) {
        mlir::Operation* const tested = lastOp(ops);
        if (!tested) {
            return Speculatable;
        }

        return mlir::cast<mlir::ConditionallySpeculatable>(tested).getSpeculatability();
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
};

TEST_F(NegationSpeculatabilityTest, mayRaiseOverAnIntegerOrADuration) {
    EXPECT_EQ(speculatabilityOf("%r = db.negate(%integer) : (!i64s) -> !unresolved"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.negate(%count) : (!u64s) -> !unresolved"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.negate(%duration) : (!durations) -> !unresolved"), NotSpeculatable);
}

TEST_F(NegationSpeculatabilityTest, isNotComputedOnceOverTheLowestInteger) {
    mlir::Operation* const negation = lastOp("%r = db.negate(%lowest) : (!i64s) -> !unresolved");
    ASSERT_TRUE(negation);
    EXPECT_FALSE(db::runsWithoutRaising(negation));
}

TEST_F(NegationSpeculatabilityTest, isSpeculatableOverADouble) {
    EXPECT_EQ(speculatabilityOf("%r = db.negate(%real) : (!reals) -> !unresolved"), Speculatable);
}

TEST_F(NegationSpeculatabilityTest, plusNeverRaisesOverANumberOrADuration) {
    EXPECT_EQ(speculatabilityOf("%r = db.unary_plus(%integer) : (!i64s) -> !unresolved"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.unary_plus(%duration) : (!durations) -> !unresolved"), Speculatable);
}
