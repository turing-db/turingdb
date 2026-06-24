#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"

namespace {

// A context with just the dialects a db-dialect program needs: db for the query
// ops and func for the enclosing function.
class DBDialectTest : public ::testing::Test {
protected:
    DBDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses a db-dialect module, returning a null module on failure. The
    // diagnostics are swallowed so a deliberately malformed program does not
    // print to the test log.
    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    // The single db.cross_product in the module's only function.
    static mlir::db::CrossProduct findCrossProduct(mlir::ModuleOp module) {
        mlir::db::CrossProduct product;
        module.walk([&](mlir::db::CrossProduct op) {
            product = op;
        });

        return product;
    }

    mlir::MLIRContext _context;
};

// MATCH (a), (b) RETURN a, b: two disconnected scans, one column yielded per
// factor, two columns in the product.
const char* const crossProductProgram = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<"a">
    db.yield %a : !db.column<"a">
  } factor {
    %b = db.scan_nodes() : !db.column<"b">
    db.yield %b : !db.column<"b">
  }
  db.output(%0#0, %0#1) : !db.column<"a">, !db.column<"b">
  return
}
)mlir";

// MATCH (a), (b) RETURN a: the right factor surfaces no column, so its db.yield
// is empty - which the verifier rejects, since a side's row count is read from
// its first yielded column.
const char* const zeroYieldProgram = R"mlir(
func.func @main() {
  %0 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<"a">
    db.yield %a : !db.column<"a">
  } factor {
    %b = db.scan_nodes() : !db.column<"b">
    db.yield
  }
  db.output(%0) : !db.column<"a">
  return
}
)mlir";

// A factor whose region does not end in a db.yield: the parser cannot recover
// the result types and rejects it.
const char* const missingYieldProgram = R"mlir(
func.func @main() {
  %0 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<"a">
  } factor {
    %b = db.scan_nodes() : !db.column<"b">
    db.yield %b : !db.column<"b">
  }
  return
}
)mlir";

// MATCH (a) RETURN a LIMIT 3: a scan capped by db.limit, one pass-through column.
const char* const limitProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  %la = db.limit(%a) count 3 : (!db.column<"a">) -> !db.column<"a">
  db.output(%la) : !db.column<"a">
  return
}
)mlir";

// db.limit's count is a ui64 attribute, so a negative literal cannot parse.
const char* const negativeLimitProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  %la = db.limit(%a) count -1 : (!db.column<"a">) -> !db.column<"a">
  db.output(%la) : !db.column<"a">
  return
}
)mlir";

TEST_F(DBDialectTest, parsesCrossProductOfTwoScans) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductProgram);
    ASSERT_TRUE(module);

    mlir::db::CrossProduct product = findCrossProduct(*module);
    ASSERT_TRUE(product);

    // The results are the left factor's yielded column followed by the right's.
    const mlir::Operation::result_range results = product.getResults();
    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].getType(), mlir::db::ColumnType::get(&_context, "a"));
    EXPECT_EQ(results[1].getType(), mlir::db::ColumnType::get(&_context, "b"));

    // Each factor is one self-contained block ending in a db.yield.
    EXPECT_TRUE(mlir::isa<mlir::db::Yield>(product.getLeftFactor().front().getTerminator()));
    EXPECT_TRUE(mlir::isa<mlir::db::Yield>(product.getRightFactor().front().getTerminator()));
}

TEST_F(DBDialectTest, roundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // custom printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, rejectsZeroYieldFactor) {
    // An empty db.yield surfaces no column, so the factor cannot be sized; the
    // verifier rejects it and the module fails to verify during parsing.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(zeroYieldProgram);
    EXPECT_FALSE(module);
}

TEST_F(DBDialectTest, rejectsFactorWithoutYield) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(missingYieldProgram);
    EXPECT_FALSE(module);
}

// Builds a cross_product whose declared results do not match the columns its
// factors yield, exercising the verifier's backstop on the programmatic builder
// path (the parser path always derives the results from the yields).
TEST_F(DBDialectTest, verifierRejectsResultsNotMatchingYields) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    // Hold the op in a module so it lives in a block and is cleaned up with it.
    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type colA = mlir::db::ColumnType::get(&_context, "a");
    const mlir::Type colB = mlir::db::ColumnType::get(&_context, "b");

    // Declare a single result but make the factors yield two columns in total:
    // the left yields `a` and the right yields `b`, so the result count is wrong.
    auto product = builder.create<mlir::db::CrossProduct>(loc, mlir::TypeRange {colA});

    mlir::Block* leftFactor = &product.getLeftFactor().front();
    builder.setInsertionPointToStart(leftFactor);
    auto scanA = builder.create<mlir::db::ScanNodes>(loc, colA);
    builder.create<mlir::db::Yield>(loc, mlir::ValueRange {scanA.getResult()});

    mlir::Block* rightFactor = &product.getRightFactor().front();
    builder.setInsertionPointToStart(rightFactor);
    auto scanB = builder.create<mlir::db::ScanNodes>(loc, colB);
    builder.create<mlir::db::Yield>(loc, mlir::ValueRange {scanB.getResult()});

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(product.getOperation())));
}

TEST_F(DBDialectTest, parsesLimit) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(limitProgram);
    ASSERT_TRUE(module);

    mlir::db::Limit limit;
    module.get().walk([&](mlir::db::Limit op) {
        limit = op;
    });
    ASSERT_TRUE(limit);

    // The count is the parsed ui64, and the single column passes through as the
    // single result, same type.
    EXPECT_EQ(limit.getCount(), 3u);
    ASSERT_EQ(limit.getColumns().size(), 1u);
    ASSERT_EQ(limit.getResults().size(), 1u);
    EXPECT_EQ(limit.getColumns()[0].getType(), mlir::db::ColumnType::get(&_context, "a"));
    EXPECT_EQ(limit.getResults()[0].getType(), mlir::db::ColumnType::get(&_context, "a"));
}

TEST_F(DBDialectTest, limitRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(limitProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.limit printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, rejectsNegativeLimitCount) {
    // count is a ui64 attribute, so a negative literal fails to parse and the
    // module is null.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(negativeLimitProgram);
    EXPECT_FALSE(module);
}

// Builds a db.limit whose result count does not match its columns, exercising
// the verifier's pass-through arity check on the programmatic builder path (the
// parser path always derives the results from the printed functional type).
TEST_F(DBDialectTest, verifierRejectsLimitArityMismatch) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type colA = mlir::db::ColumnType::get(&_context, "a");
    const mlir::Type colB = mlir::db::ColumnType::get(&_context, "b");

    auto scanA = builder.create<mlir::db::ScanNodes>(loc, colA);

    // One input column but two declared results: the pass-through arity is wrong.
    auto limit = builder.create<mlir::db::Limit>(loc,
                                                 mlir::TypeRange {colA, colB},
                                                 mlir::ValueRange {scanA.getResult()},
                                                 /*count=*/3u);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(limit.getOperation())));
}

}
