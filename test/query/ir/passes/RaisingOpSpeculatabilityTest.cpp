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

// %cell is a column of type-erased cells and %unresolved one whose type lowering resolves:
// either may hold a value of any type, which a function reading some types raises on
class RaisingOpSpeculatabilityTest : public ::testing::Test {
protected:
    RaisingOpSpeculatabilityTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Speculation::Speculatability speculatabilityOf(std::string_view op) {
        std::string program = R"mlir(
!strings = !db.column<!storage.nullable<!storage.string>>
!texts = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!integers = !db.column<!storage.nullable<i64>>
!reals = !db.column<f64>
!lists = !db.column<!storage.list<i64>>
!maps = !db.column<!storage.map>
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!i64s = !db.column<i64>
!any = !db.column<none>

func.func @main(%string: !strings, %cell: !cells, %unresolved: !unresolved, %integer: !integers,
                %real: !reals, %list: !lists, %map: !maps, %node: !nodes, %edge: !edges) {
  %zero = db.constant(0 : i64)
  %three = db.constant(3 : i64)
  %negative = db.constant(-1 : i64)
  %largest = db.constant(2147483647 : i64)
  %past = db.constant(2147483648 : i64)
  )mlir";

        program += op;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << op;
        if (!module) {
            return NotSpeculatable;
        }

        mlir::func::FuncOp main = module->lookupSymbol<mlir::func::FuncOp>("main");
        mlir::Operation* tested = main.getBody().front().getTerminator()->getPrevNode();

        return mlir::cast<mlir::ConditionallySpeculatable>(tested).getSpeculatability();
    }

    mlir::MLIRContext _context;
};

TEST_F(RaisingOpSpeculatabilityTest, takesASubstringOfAStringAtLiteralPositionsInRange) {
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%string, %zero, %three) : (!strings, !i64s, !i64s) -> !strings"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%string, %three) : (!strings, !i64s) -> !strings"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%string, %largest, %largest) : (!strings, !i64s, !i64s) -> !strings"), Speculatable);

    EXPECT_EQ(speculatabilityOf("%r = db.substring(%string, %negative) : (!strings, !i64s) -> !strings"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%string, %zero, %past) : (!strings, !i64s, !i64s) -> !strings"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%string, %integer) : (!strings, !integers) -> !strings"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%string, %zero, %integer) : (!strings, !i64s, !integers) -> !strings"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%cell, %zero) : (!cells, !i64s) -> !strings"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.substring(%unresolved, %zero) : (!unresolved, !i64s) -> !strings"), NotSpeculatable);
}

TEST_F(RaisingOpSpeculatabilityTest, takesAnEndOfAStringAtALiteralLengthInRange) {
    EXPECT_EQ(speculatabilityOf("%r = db.left(%string, %three) : (!strings, !i64s) -> !texts"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.right(%string, %zero) : (!strings, !i64s) -> !texts"), Speculatable);

    EXPECT_EQ(speculatabilityOf("%r = db.left(%string, %negative) : (!strings, !i64s) -> !texts"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.right(%string, %integer) : (!strings, !integers) -> !texts"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.left(%cell, %three) : (!cells, !i64s) -> !texts"), NotSpeculatable);
}

TEST_F(RaisingOpSpeculatabilityTest, readsTextOutOfAStringColumnAlone) {
    EXPECT_EQ(speculatabilityOf("%r = db.to_upper(%string) : (!strings) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.trim(%string) : (!strings) -> !any"), Speculatable);

    EXPECT_EQ(speculatabilityOf("%r = db.to_lower(%cell) : (!cells) -> !any"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.rtrim(%unresolved) : (!unresolved) -> !any"), NotSpeculatable);
}

TEST_F(RaisingOpSpeculatabilityTest, readsAStringOrAListOutOfAColumnOfEither) {
    EXPECT_EQ(speculatabilityOf("%r = db.reverse(%string) : (!strings) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.reverse(%list) : (!lists) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.size(%string) : (!strings) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.size(%list) : (!lists) -> !any"), Speculatable);

    EXPECT_EQ(speculatabilityOf("%r = db.reverse(%cell) : (!cells) -> !any"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.size(%cell) : (!cells) -> !any"), NotSpeculatable);
}

TEST_F(RaisingOpSpeculatabilityTest, readsAListOutOfAListColumnAlone) {
    EXPECT_EQ(speculatabilityOf("%r = db.head(%list) : (!lists) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.tail(%list) : (!lists) -> !any"), Speculatable);

    EXPECT_EQ(speculatabilityOf("%r = db.last(%cell) : (!cells) -> !any"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.head(%string) : (!strings) -> !any"), NotSpeculatable);
}

TEST_F(RaisingOpSpeculatabilityTest, readsAnEntityOrAMapOutOfAColumnOfOne) {
    EXPECT_EQ(speculatabilityOf("%r = db.element_id(%node) : (!nodes) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.element_id(%edge) : (!edges) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.keys(%node) : (!nodes) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.keys(%map) : (!maps) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.properties(%edge) : (!edges) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.start_node(%edge) : (!edges) -> !nodes"), Speculatable);

    EXPECT_EQ(speculatabilityOf("%r = db.element_id(%cell) : (!cells) -> !any"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.keys(%cell) : (!cells) -> !any"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.properties(%unresolved) : (!unresolved) -> !any"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.end_node(%cell) : (!cells) -> !nodes"), NotSpeculatable);
}

TEST_F(RaisingOpSpeculatabilityTest, readsANumberOutOfANumberColumnAlone) {
    EXPECT_EQ(speculatabilityOf("%r = db.abs(%integer) : (!integers) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.sign(%real) : (!reals) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.sqrt(%integer) : (!integers) -> !any"), Speculatable);

    EXPECT_EQ(speculatabilityOf("%r = db.abs(%cell) : (!cells) -> !any"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.haversin(%unresolved) : (!unresolved) -> !any"), NotSpeculatable);
}

TEST_F(RaisingOpSpeculatabilityTest, keepsTheFunctionsNoRowRaisesOnSpeculatable) {
    EXPECT_EQ(speculatabilityOf("%r = db.labels(%node) : (!nodes) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.to_integer(%string) : (!strings) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.to_integer_or_null(%cell) : (!cells) -> !any"), Speculatable);
    EXPECT_EQ(speculatabilityOf("%r = db.to_string_or_null(%unresolved) : (!unresolved) -> !any"), Speculatable);
}
