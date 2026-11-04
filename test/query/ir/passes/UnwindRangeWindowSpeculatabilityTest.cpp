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

// The window a db.unwind_range keeps is bounded by the slice it was folded from, and raises
// as that slice does on a type-erased cell holding no integer
class UnwindRangeWindowSpeculatabilityTest : public ::testing::Test {
protected:
    UnwindRangeWindowSpeculatabilityTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Speculation::Speculatability speculatabilityOf(std::string_view unwind) {
        std::string program = R"mlir(
!i64s = !db.column<i64>
!cells = !db.column<!storage.any>
!elements = !db.column<none>

func.func @main(%cell: !cells, %integer: !i64s) {
  %zero = db.constant(0 : i64)
  %three = db.constant(3 : i64)
  %one = db.constant(1 : i64)
  )mlir";

        program += unwind;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << unwind;
        if (!module) {
            return NotSpeculatable;
        }

        mlir::func::FuncOp main = module->lookupSymbol<mlir::func::FuncOp>("main");
        mlir::Operation* tested = main.getBody().front().getTerminator()->getPrevNode();

        return mlir::cast<mlir::ConditionallySpeculatable>(tested).getSpeculatability();
    }

    mlir::MLIRContext _context;
};

TEST_F(UnwindRangeWindowSpeculatabilityTest, countsOutAWindowOfIntegerBounds) {
    EXPECT_EQ(speculatabilityOf("%x = db.unwind_range(%zero, %three, %one, from %integer, to %integer, {}) "
                                ": (!i64s, !i64s, !i64s, !i64s, !i64s) -> !elements"),
              Speculatable);
}

TEST_F(UnwindRangeWindowSpeculatabilityTest, raisesOnAWindowBoundOfTypeErasedCells) {
    EXPECT_EQ(speculatabilityOf("%x = db.unwind_range(%zero, %three, %one, from %cell, to %integer, {}) "
                                ": (!i64s, !i64s, !i64s, !cells, !i64s) -> !elements"),
              NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%x = db.unwind_range(%zero, %three, %one, from %integer, to %cell, {}) "
                                ": (!i64s, !i64s, !i64s, !i64s, !cells) -> !elements"),
              NotSpeculatable);
}

TEST_F(UnwindRangeWindowSpeculatabilityTest, countsOutAWindowBoundByAnIntegerAMapHolds) {
    EXPECT_EQ(speculatabilityOf(R"mlir(
  %m = db.constant({from = 1 : i64})
  %from = db.static_map_key %m key "from" : (!db.column<!storage.map>) -> !cells
  %x = db.unwind_range(%zero, %three, %one, from %from, to %integer, {}) : (!i64s, !i64s, !i64s, !cells, !i64s) -> !elements
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOf(R"mlir(
  %m = db.constant({from = "a" : !storage.string})
  %from = db.static_map_key %m key "from" : (!db.column<!storage.map>) -> !cells
  %x = db.unwind_range(%zero, %three, %one, from %from, to %integer, {}) : (!i64s, !i64s, !i64s, !cells, !i64s) -> !elements
)mlir"),
              NotSpeculatable);
}
