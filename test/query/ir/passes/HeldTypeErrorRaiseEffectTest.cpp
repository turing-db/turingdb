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
#include "StorageDialect.h"

using mlir::Speculation::NotSpeculatable;
using mlir::Speculation::Speculatable;

// A string function or an arithmetic operator raises on a type-erased cell holding a value of
// a type it does not read, so nothing reading it is no reason to erase it. Over typed columns
// it cannot raise, and goes. %cell is a column of type-erased cells.
class HeldTypeErrorRaiseEffectTest : public ::testing::Test {
protected:
    HeldTypeErrorRaiseEffectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Operation* parseUnreadOp(std::string_view op) {
        std::string program = R"mlir(
!strings = !db.column<!storage.nullable<!storage.string>>
!texts = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!integers = !db.column<!storage.nullable<i64>>
!reals = !db.column<f64>
!any = !db.column<none>

func.func @main(%string: !strings, %cell: !cells, %integer: !integers, %real: !reals) {
  )mlir";

        program += op;
        program += "\n  return\n}\n";

        _module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        if (!_module) {
            return nullptr;
        }

        mlir::func::FuncOp main = _module->lookupSymbol<mlir::func::FuncOp>("main");

        return main.getBody().front().getTerminator()->getPrevNode();
    }

    void expectKept(std::string_view op) {
        mlir::Operation* const unread = parseUnreadOp(op);
        ASSERT_TRUE(unread) << op;

        EXPECT_FALSE(mlir::isOpTriviallyDead(unread)) << op;
        EXPECT_EQ(mlir::cast<mlir::ConditionallySpeculatable>(unread).getSpeculatability(), NotSpeculatable) << op;
    }

    void expectErased(std::string_view op) {
        mlir::Operation* const unread = parseUnreadOp(op);
        ASSERT_TRUE(unread) << op;

        EXPECT_TRUE(mlir::isOpTriviallyDead(unread)) << op;
        EXPECT_EQ(mlir::cast<mlir::ConditionallySpeculatable>(unread).getSpeculatability(), Speculatable) << op;
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
};

TEST_F(HeldTypeErrorRaiseEffectTest, keepsAStringFunctionOfACell) {
    expectKept("%r = db.normalize(%cell, nfc) : (!cells) -> !texts");
    expectKept("%r = db.to_upper(%cell) : (!cells) -> !texts");
    expectKept("%r = db.trim(%cell) : (!cells) -> !texts");
}

TEST_F(HeldTypeErrorRaiseEffectTest, erasesAStringFunctionOfAString) {
    expectErased("%r = db.to_upper(%string) : (!strings) -> !strings");
    expectErased("%r = db.trim(%string) : (!strings) -> !strings");
}

TEST_F(HeldTypeErrorRaiseEffectTest, keepsArithmeticOverACell) {
    expectKept("%r = db.pow %cell, %integer : (!cells, !integers) -> !any");
    expectKept("%r = db.pow %real, %cell : (!reals, !cells) -> !any");
    expectKept("%r = db.sub %cell, %integer : (!cells, !integers) -> !any");
    expectKept("%r = db.mul %real, %cell : (!reals, !cells) -> !any");
}

TEST_F(HeldTypeErrorRaiseEffectTest, erasesArithmeticOverNumbers) {
    expectErased("%r = db.pow %integer, %real : (!integers, !reals) -> !reals");
    expectErased("%r = db.pow %real, %real : (!reals, !reals) -> !reals");
    expectErased("%r = db.sub %integer, %real : (!integers, !reals) -> !any");
    expectErased("%r = db.mul %real, %integer : (!reals, !integers) -> !any");
}
