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
#include "IRRaiseEffect.h"
#include "StorageDialect.h"

// MLIR's dead-op test, which every greedy driver erases by, keeps an op nothing reads where
// a row may raise on it, and lets one go where none can. %cell is a column of type-erased
// cells, which may hold a value of any type
class RaiseEffectTest : public ::testing::Test {
protected:
    RaiseEffectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    void parseUnreadOp(std::string_view op) {
        _unread = nullptr;

        std::string program = R"mlir(
!bools = !db.column<!storage.bool>
!strings = !db.column<!storage.nullable<!storage.string>>
!texts = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!integers = !db.column<!storage.nullable<i64>>
!reals = !db.column<f64>
!lists = !db.column<!storage.list<i64>>
!i64s = !db.column<i64>
!any = !db.column<none>

func.func @main(%bool: !bools, %string: !strings, %cell: !cells, %integer: !integers,
                %real: !reals, %list: !lists) {
  %zero = db.constant(0 : i64)
  %three = db.constant(3 : i64)
  %negative = db.constant(-1 : i64)
  )mlir";

        program += op;
        program += "\n  return\n}\n";

        _module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        ASSERT_TRUE(_module) << op;

        mlir::func::FuncOp main = _module->lookupSymbol<mlir::func::FuncOp>("main");
        _unread = main.getBody().front().getTerminator()->getPrevNode();
    }

    bool isDead(std::string_view op) {
        parseUnreadOp(op);
        return _unread && mlir::isOpTriviallyDead(_unread);
    }

    bool hasNoEffectButRaising(std::string_view op) {
        parseUnreadOp(op);
        return _unread && db::hasNoEffectButRaising(_unread);
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
    mlir::Operation* _unread {nullptr};
};

TEST_F(RaiseEffectTest, keepsALogicalOperatorOverCells) {
    EXPECT_FALSE(isDead("%r = db.and %cell, %bool : (!cells, !bools) -> !bools"));
    EXPECT_FALSE(isDead("%r = db.or %bool, %cell : (!bools, !cells) -> !bools"));
    EXPECT_FALSE(isDead("%r = db.xor %cell, %cell : (!cells, !cells) -> !bools"));
    EXPECT_FALSE(isDead("%r = db.not %cell : (!cells) -> !bools"));

    EXPECT_TRUE(isDead("%r = db.and %bool, %bool : (!bools, !bools) -> !bools"));
    EXPECT_TRUE(isDead("%r = db.or %bool, %bool : (!bools, !bools) -> !bools"));
    EXPECT_TRUE(isDead("%r = db.xor %bool, %bool : (!bools, !bools) -> !bools"));
    EXPECT_TRUE(isDead("%r = db.not %bool : (!bools) -> !bools"));
}

TEST_F(RaiseEffectTest, keepsArithmeticThatMayRaise) {
    EXPECT_FALSE(isDead("%r = db.add %cell, %integer : (!cells, !integers) -> !any"));
    EXPECT_FALSE(isDead("%r = db.mul %integer, %cell : (!integers, !cells) -> !any"));
    EXPECT_FALSE(isDead("%r = db.div %integer, %integer : (!integers, !integers) -> !integers"));
    EXPECT_FALSE(isDead("%r = db.mod %integer, %zero : (!integers, !i64s) -> !integers"));
    EXPECT_FALSE(isDead("%r = db.concat %cell, %string : (!cells, !strings) -> !any"));

    EXPECT_TRUE(isDead("%r = db.add %integer, %integer : (!integers, !integers) -> !integers"));
    EXPECT_TRUE(isDead("%r = db.sub %integer, %real : (!integers, !reals) -> !db.column<!storage.nullable<f64>>"));
    EXPECT_TRUE(isDead("%r = db.div %integer, %three : (!integers, !i64s) -> !integers"));
}

TEST_F(RaiseEffectTest, keepsAFunctionThatMayRaise) {
    EXPECT_FALSE(isDead("%r = db.abs(%cell) : (!cells) -> !cells"));
    EXPECT_FALSE(isDead("%r = db.to_integer(%cell) : (!cells) -> !integers"));
    EXPECT_FALSE(isDead("%r = db.substring(%string, %negative) : (!strings, !i64s) -> !strings"));
    EXPECT_FALSE(isDead("%r = db.left(%cell, %three) : (!cells, !i64s) -> !texts"));

    EXPECT_TRUE(isDead("%r = db.abs(%integer) : (!integers) -> !integers"));
    EXPECT_TRUE(isDead("%r = db.to_integer(%string) : (!strings) -> !integers"));
    EXPECT_TRUE(isDead("%r = db.substring(%string, %zero, %three) : (!strings, !i64s, !i64s) -> !strings"));
    EXPECT_TRUE(isDead("%r = db.to_integer_or_null(%cell) : (!cells) -> !integers"));
}

TEST_F(RaiseEffectTest, keepsAListOpThatMayRaise) {
    EXPECT_FALSE(isDead("%r = db.list_slice(%list, from %cell) : (!lists, !cells) -> !any"));
    EXPECT_FALSE(isDead("%r = db.range(%integer, %integer) : (!integers, !integers) -> !lists"));
    EXPECT_FALSE(isDead("%r = db.percentile_cont(%list, %real) : (!lists, !reals) -> !any"));

    EXPECT_TRUE(isDead("%r = db.list_slice(%list, from %integer) : (!lists, !integers) -> !any"));
    EXPECT_TRUE(isDead("%r = db.range(%zero, %three) : (!i64s, !i64s) -> !lists"));
}

TEST_F(RaiseEffectTest, countsARaiseAsNoOtherEffect) {
    EXPECT_TRUE(hasNoEffectButRaising("%r = db.and %cell, %bool : (!cells, !bools) -> !bools"));
    EXPECT_TRUE(hasNoEffectButRaising("%r = db.percentile_cont(%list, %real) : (!lists, !reals) -> !any"));
    EXPECT_TRUE(hasNoEffectButRaising("%r = db.eq %cell, %bool : (!cells, !bools) -> !bools"));
}
