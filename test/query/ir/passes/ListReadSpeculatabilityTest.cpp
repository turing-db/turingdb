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

// An index or a slice raises on a row holding a value that is no list, and a slice on a bound
// that is no integer, so each is speculatable only over columns no row of which holds one
class ListReadSpeculatabilityTest : public ::testing::Test {
protected:
    ListReadSpeculatabilityTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Speculation::Speculatability speculatabilityOfTheListRead(std::string_view ops) {
        std::string program = R"mlir(
!integers = !db.column<i64>
!lists = !db.column<!storage.list<i64>>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!maps = !db.column<!storage.map>

func.func @main(%integer: !integers, %list: !lists, %cell: !cells) {
  )mlir";

        program += ops;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << ops;
        if (!module) {
            return NotSpeculatable;
        }

        mlir::Operation* listRead = nullptr;
        module->walk([&listRead](mlir::Operation* op) {
            if (mlir::isa<mlir::db::ListIndex, mlir::db::ListSlice>(op)) {
                listRead = op;
            }
        });

        EXPECT_TRUE(listRead) << ops;
        if (!listRead) {
            return NotSpeculatable;
        }

        return mlir::cast<mlir::ConditionallySpeculatable>(listRead).getSpeculatability();
    }

    mlir::MLIRContext _context;
};

TEST_F(ListReadSpeculatabilityTest, readsAColumnOfLists) {
    EXPECT_EQ(speculatabilityOfTheListRead("%r = db.list_index %list, %integer : (!lists, !integers) -> !unresolved"), Speculatable);
    EXPECT_EQ(speculatabilityOfTheListRead("%r = db.list_slice(%list, from %integer) : (!lists, !integers) -> !unresolved"), Speculatable);
}

TEST_F(ListReadSpeculatabilityTest, raisesOnACell) {
    EXPECT_EQ(speculatabilityOfTheListRead("%r = db.list_index %cell, %integer : (!cells, !integers) -> !unresolved"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOfTheListRead("%r = db.list_slice(%list, from %cell) : (!lists, !cells) -> !unresolved"), NotSpeculatable);
}

TEST_F(ListReadSpeculatabilityTest, readsAnUnwoundListOfListsAndNulls) {
    EXPECT_EQ(speculatabilityOfTheListRead(R"mlir(
  %rows = db.unwind_const([[1 : i64], unit, ["a" : !storage.string]]) : !cells
  %r = db.list_index %rows, %integer : (!cells, !integers) -> !unresolved
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheListRead(R"mlir(
  %rows = db.unwind_const([[1 : i64], {a = 1 : i64}]) : !cells
  %r = db.list_index %rows, %integer : (!cells, !integers) -> !unresolved
)mlir"),
              NotSpeculatable);
}

TEST_F(ListReadSpeculatabilityTest, readsAListAndABoundHeldByAConstantMap) {
    EXPECT_EQ(speculatabilityOfTheListRead(R"mlir(
  %m = db.constant({f = 1 : i64, xs = [1, 2]})
  %xs = db.static_map_key %m key "xs" : (!maps) -> !cells
  %f = db.static_map_key %m key "f" : (!maps) -> !cells
  %r = db.list_slice(%xs, from %f) : (!cells, !cells) -> !unresolved
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheListRead(R"mlir(
  %m = db.constant({f = "a" : !storage.string, xs = [1, 2]})
  %xs = db.static_map_key %m key "xs" : (!maps) -> !cells
  %f = db.static_map_key %m key "f" : (!maps) -> !cells
  %r = db.list_slice(%xs, from %f) : (!cells, !cells) -> !unresolved
)mlir"),
              NotSpeculatable);

    EXPECT_EQ(speculatabilityOfTheListRead(R"mlir(
  %m = db.constant({xs = "abc" : !storage.string})
  %xs = db.static_map_key %m key "xs" : (!maps) -> !cells
  %r = db.list_index %xs, %integer : (!cells, !integers) -> !unresolved
)mlir"),
              NotSpeculatable);
}

TEST_F(ListReadSpeculatabilityTest, readsAListCarriedPastAFilter) {
    EXPECT_EQ(speculatabilityOfTheListRead(R"mlir(
  %rows = db.unwind_const([[1 : i64], unit, ["a" : !storage.string]]) : !cells
  %held = db.is_not_null %rows : (!cells) -> !db.column<!storage.bool>
  %kept = db.filter(%held, {%rows}) : (!db.column<!storage.bool>, !cells) -> !cells
  %r = db.list_index %kept, %integer : (!cells, !integers) -> !unresolved
)mlir"),
              Speculatable);
}
