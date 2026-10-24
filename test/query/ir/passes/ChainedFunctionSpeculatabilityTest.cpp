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

// A db function names no element on its result column, so a function reading another one's
// result reads the element the chain below it answers. abs(abs(9)) reads an integer.
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
    EXPECT_EQ(speculatabilityOf("%a = db.abs(%nine) : (!i64s) -> !unresolved\n"
                                "  %r = db.abs(%a) : (!unresolved) -> !unresolved"),
              Speculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.unary_plus(%nine) : (!i64s) -> !unresolved\n"
                                "  %r = db.unary_plus(%a) : (!unresolved) -> !unresolved"),
              Speculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.sqrt(%nine) : (!i64s) -> !unresolved\n"
                                "  %b = db.negate(%a) : (!unresolved) -> !unresolved\n"
                                "  %r = db.negate(%b) : (!unresolved) -> !unresolved"),
              Speculatable);
}

TEST_F(ChainedFunctionSpeculatabilityTest, readsTheElementAnInnerFunctionMapsTo) {
    EXPECT_EQ(speculatabilityOf("%a = db.sqrt(%nine) : (!i64s) -> !unresolved\n"
                                "  %r = db.abs(%a) : (!unresolved) -> !unresolved"),
              Speculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.size(%text) : (!texts) -> !unresolved\n"
                                "  %r = db.abs(%a) : (!unresolved) -> !unresolved"),
              Speculatable);
}

// A string is no number, and a chain starting at a cell or at an unresolved column still
// names no element: the function may meet a value it raises on
TEST_F(ChainedFunctionSpeculatabilityTest, keepsAChainOverAnotherTypeNotSpeculatable) {
    EXPECT_EQ(speculatabilityOf("%a = db.to_upper(%text) : (!texts) -> !unresolved\n"
                                "  %r = db.abs(%a) : (!unresolved) -> !unresolved"),
              NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.abs(%cell) : (!cells) -> !unresolved\n"
                                "  %r = db.abs(%a) : (!unresolved) -> !unresolved"),
              NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%a = db.abs(%unresolved) : (!unresolved) -> !unresolved\n"
                                "  %r = db.abs(%a) : (!unresolved) -> !unresolved"),
              NotSpeculatable);
}

TEST_F(ChainedFunctionSpeculatabilityTest, computesAChainOverAConstantOnce) {
    EXPECT_TRUE(yieldsConstant("%a = db.unary_plus(%nine) : (!i64s) -> !unresolved\n"
                               "  %r = db.unary_plus(%a) : (!unresolved) -> !unresolved"));
    EXPECT_TRUE(yieldsConstant("%a = db.abs(%nine) : (!i64s) -> !unresolved\n"
                               "  %r = db.abs(%a) : (!unresolved) -> !unresolved"));
}
