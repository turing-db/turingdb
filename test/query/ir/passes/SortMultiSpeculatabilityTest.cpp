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

// A sort raises on an element that is no map, a field that is no string, a negative skip and
// a sort key holding an embedding, so it is speculatable only over operands that hold none
class SortMultiSpeculatabilityTest : public ::testing::Test {
protected:
    SortMultiSpeculatabilityTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Speculation::Speculatability speculatabilityOfTheSort(std::string_view ops) {
        std::string program = R"mlir(
!integers = !db.column<i64>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!fields = !db.column<!storage.list<!storage.string>>
!maps = !db.column<!storage.list<!storage.map>>
!mixed = !db.column<!storage.list<!storage.any>>

func.func @main(%integer: !integers, %cell: !cells, %names: !fields, %map: !db.column<!storage.map>) {
  )mlir";

        program += ops;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << ops;
        if (!module) {
            return NotSpeculatable;
        }

        mlir::Operation* sort = nullptr;
        module->walk([&sort](mlir::db::SortMulti op) {
            sort = op;
        });

        EXPECT_TRUE(sort) << ops;
        if (!sort) {
            return NotSpeculatable;
        }

        return mlir::cast<mlir::ConditionallySpeculatable>(sort).getSpeculatability();
    }

    mlir::MLIRContext _context;
};

TEST_F(SortMultiSpeculatabilityTest, sortsALiteralListOfMapsOnLiteralFields) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}, {a = 1 : i64}, {b = "x" : !storage.string}])
  %fields = db.constant(["^a" : !storage.string, "b" : !storage.string])
  %r = db.sort_multi(%maps) fields %fields limit %integer : (!maps, !fields, !integers) -> !unresolved
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}, {a = 1 : i64}])
  %r = db.sort_multi(%maps) : (!maps) -> !unresolved
)mlir"),
              Speculatable);
}

TEST_F(SortMultiSpeculatabilityTest, raisesOnAnElementThatIsNoMap) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}, 1 : i64])
  %fields = db.constant(["a" : !storage.string])
  %r = db.sort_multi(%maps) fields %fields : (!mixed, !fields) -> !unresolved
)mlir"),
              NotSpeculatable);

    EXPECT_EQ(speculatabilityOfTheSort("%r = db.sort_multi(%cell) : (!cells) -> !unresolved"), NotSpeculatable);
}

TEST_F(SortMultiSpeculatabilityTest, sortsANullElement) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}, unit])
  %fields = db.constant(["a" : !storage.string])
  %r = db.sort_multi(%maps) fields %fields : (!maps, !fields) -> !unresolved
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %one = db.constant(1 : i64)
  %m = db.make_map(%one) keys ["a"] : !integers
  %null = db.constant("" : !storage.nullable<none>)
  %maps = db.make_list(%m, %null) : (!db.column<!storage.map>, !db.column<!storage.nullable<none>>) -> !maps
  %fields = db.constant(["a" : !storage.string])
  %r = db.sort_multi(%maps) fields %fields : (!maps, !fields) -> !unresolved
)mlir"),
              Speculatable);
}

TEST_F(SortMultiSpeculatabilityTest, keepsAnyListWithoutAField) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}, unit, 1 : i64])
  %r = db.sort_multi(%maps) : (!mixed) -> !unresolved
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}, unit, 1 : i64])
  %fields = db.constant([])
  %r = db.sort_multi(%maps) fields %fields : (!mixed, !db.column<!storage.list<none>>) -> !unresolved
)mlir"),
              Speculatable);
}

TEST_F(SortMultiSpeculatabilityTest, sortsTheResultOfAnotherSort) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}, {a = 1 : i64}])
  %fields = db.constant(["a" : !storage.string])
  %inner = db.sort_multi(%maps) : (!maps) -> !unresolved
  %r = db.sort_multi(%inner) fields %fields : (!unresolved, !fields) -> !unresolved
)mlir"),
              Speculatable);
}

TEST_F(SortMultiSpeculatabilityTest, raisesOnAKeyHoldingAValueWithNoOrder) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = [1 : i64]}, {a = 1 : i64}])
  %fields = db.constant(["a" : !storage.string])
  %r = db.sort_multi(%maps) fields %fields : (!maps, !fields) -> !unresolved
)mlir"),
              NotSpeculatable);
}

TEST_F(SortMultiSpeculatabilityTest, raisesOnFieldsItCannotRead) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}])
  %r = db.sort_multi(%maps) fields %names : (!maps, !fields) -> !unresolved
)mlir"),
              NotSpeculatable);

    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}])
  %fields = db.constant([1 : i64])
  %r = db.sort_multi(%maps) fields %fields : (!maps, !db.column<!storage.list<i64>>) -> !unresolved
)mlir"),
              NotSpeculatable);
}

TEST_F(SortMultiSpeculatabilityTest, raisesOnASkipThatMayBeNegative) {
    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}])
  %two = db.constant(2 : i64)
  %r = db.sort_multi(%maps) skip %two : (!maps, !integers) -> !unresolved
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}])
  %minus = db.constant(-1 : i64)
  %r = db.sort_multi(%maps) skip %minus : (!maps, !integers) -> !unresolved
)mlir"),
              NotSpeculatable);

    EXPECT_EQ(speculatabilityOfTheSort(R"mlir(
  %maps = db.constant([{a = 2 : i64}])
  %r = db.sort_multi(%maps) skip %integer : (!maps, !integers) -> !unresolved
)mlir"),
              NotSpeculatable);
}
