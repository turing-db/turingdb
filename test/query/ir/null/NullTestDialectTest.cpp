#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "NLDialect.h"
#include "NLOps.h"
#include "StorageDialect.h"

namespace {

// A test for a null is db.is_null or nl.is_null. An equality against the null literal is null
// on every row, so neither dialect takes one: a pass matching an equality never meets a null
// test in disguise.
class NullTestDialectTest : public ::testing::Test {
protected:
    NullTestDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    bool parses(std::string_view program) {
        return static_cast<bool>(mlir::parseSourceString<mlir::ModuleOp>(std::string(program), mlir::ParserConfig(&_context)));
    }

    mlir::MLIRContext _context;
};

constexpr std::string_view DB_NULL_TESTS = R"mlir(
func.func @main(%x: !db.column<!storage.nullable<i64>>, %n: !db.column<!storage.node_id>) {
  %a = db.is_null %x : (!db.column<!storage.nullable<i64>>) -> !db.column<!storage.bool>
  %b = db.is_not_null %n : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>
  return
}
)mlir";

constexpr std::string_view NL_NULL_TESTS = R"mlir(
func.func @main(%x: !nl.chunk<!storage.nullable<i64>>, %p: !nl.chunk<!storage.path>) {
  %a = nl.is_null %x : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<i1>
  %b = nl.is_not_null %p : (!nl.chunk<!storage.path>) -> !nl.chunk<i1>
  return
}
)mlir";

constexpr std::string_view DB_EQUALITY_AGAINST_NULL = R"mlir(
func.func @main(%x: !db.column<!storage.nullable<i64>>, %null: !db.column<!storage.nullable<none>>) {
  %a = db.eq %x, %null : (!db.column<!storage.nullable<i64>>, !db.column<!storage.nullable<none>>) -> !db.column<!storage.bool>
  return
}
)mlir";

constexpr std::string_view DB_INEQUALITY_AGAINST_NULL = R"mlir(
func.func @main(%x: !db.column<!storage.nullable<i64>>, %null: !db.column<!storage.nullable<none>>) {
  %a = db.neq %null, %x : (!db.column<!storage.nullable<none>>, !db.column<!storage.nullable<i64>>) -> !db.column<!storage.bool>
  return
}
)mlir";

constexpr std::string_view NL_EQUALITY_AGAINST_NULL = R"mlir(
func.func @main(%x: !nl.chunk<!storage.nullable<i64>>, %null: !nl.chunk<!storage.nullable<none>>) {
  %a = nl.eq %x, %null : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<none>>) -> !nl.chunk<i1>
  return
}
)mlir";

}

TEST_F(NullTestDialectTest, parsesTheNullTests) {
    EXPECT_TRUE(parses(DB_NULL_TESTS));
    EXPECT_TRUE(parses(NL_NULL_TESTS));
}

TEST_F(NullTestDialectTest, rejectsAnEqualityAgainstTheNullLiteral) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(DB_EQUALITY_AGAINST_NULL));
    EXPECT_FALSE(parses(DB_INEQUALITY_AGAINST_NULL));
    EXPECT_FALSE(parses(NL_EQUALITY_AGAINST_NULL));
}
