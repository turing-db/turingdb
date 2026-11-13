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

// A db function names the element it answers on its result column, so a function reading
// another one's result reads that element. abs(abs(9)) reads an integer.
class ChainedFunctionSpeculatabilityTest : public ::testing::Test {
protected:
    ChainedFunctionSpeculatabilityTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // The op @param ops ends with, after @param ops ran over the arguments %cell and
    // %unresolved and the constants %nine and %text
    mlir::Operation* lastOp(std::string_view ops) {
        std::string program = R"mlir(
!i64s = !db.column<i64>
!texts = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!integers = !db.column<!storage.nullable<i64>>
!reals = !db.column<!storage.nullable<f64>>
!strings = !db.column<!storage.nullable<!storage.string>>

func.func @main(%cell: !cells, %unresolved: !unresolved) {
  %nine = db.constant(9 : i64)
  %text = db.constant("nine" : !storage.string)
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
            return NotSpeculatable;
        }

        return mlir::cast<mlir::ConditionallySpeculatable>(tested).getSpeculatability();
    }

    bool yieldsConstant(std::string_view ops) {
        mlir::Operation* const tested = lastOp(ops);
        return tested && db::yieldsConstantColumn(tested->getResult(0));
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
};

TEST_F(ChainedFunctionSpeculatabilityTest, readsTheIntegerAnInnerFunctionAnswers) {
    EXPECT_EQ(speculatabilityOf("%a = db.abs(%nine) : (!i64s) -> !integers\n"
                                "  %r = db.abs(%a) : (!integers) -> !integers"),
              Speculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.unary_plus(%nine) : (!i64s) -> !integers\n"
                                "  %r = db.unary_plus(%a) : (!integers) -> !integers"),
              Speculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.sqrt(%nine) : (!i64s) -> !reals\n"
                                "  %b = db.negate(%a) : (!reals) -> !reals\n"
                                "  %r = db.negate(%b) : (!reals) -> !reals"),
              Speculatable);
}

TEST_F(ChainedFunctionSpeculatabilityTest, readsTheElementAnInnerFunctionMapsTo) {
    EXPECT_EQ(speculatabilityOf("%a = db.sqrt(%nine) : (!i64s) -> !reals\n"
                                "  %r = db.abs(%a) : (!reals) -> !reals"),
              Speculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.size(%text) : (!texts) -> !integers\n"
                                "  %r = db.abs(%a) : (!integers) -> !integers"),
              Speculatable);
}

// A string is no number, a chain starting at a cell holds cells, and one starting at an
// unresolved column names no element: the function may meet a value it raises on
TEST_F(ChainedFunctionSpeculatabilityTest, keepsAChainOverAnotherTypeNotSpeculatable) {
    EXPECT_EQ(speculatabilityOf("%a = db.to_upper(%text) : (!texts) -> !strings\n"
                                "  %r = db.abs(%a) : (!strings) -> !strings"),
              NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.abs(%cell) : (!cells) -> !cells\n"
                                "  %r = db.abs(%a) : (!cells) -> !cells"),
              NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.abs(%unresolved) : (!unresolved) -> !unresolved\n"
                                "  %r = db.abs(%a) : (!unresolved) -> !unresolved"),
              NotSpeculatable);
}

TEST_F(ChainedFunctionSpeculatabilityTest, computesAChainOverAConstantOnce) {
    EXPECT_TRUE(yieldsConstant("%a = db.unary_plus(%nine) : (!i64s) -> !integers\n"
                               "  %r = db.unary_plus(%a) : (!integers) -> !integers"));
    EXPECT_TRUE(yieldsConstant("%a = db.abs(%nine) : (!i64s) -> !integers\n"
                               "  %r = db.abs(%a) : (!integers) -> !integers"));
}
