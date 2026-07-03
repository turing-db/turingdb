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
#include "StorageDialect.h"
#include "StorageTypes.h"

namespace {

// A context with just the dialects a db-dialect program needs: db for the query
// ops and func for the enclosing function.
class DBDialectTest : public ::testing::Test {
protected:
    DBDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
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
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a), (b) RETURN a: the right factor surfaces no column, so its db.yield
// is empty - which the verifier rejects, since a side's row count is read from
// its first yielded column.
const char* const zeroYieldProgram = R"mlir(
func.func @main() {
  %0 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield
  }
  db.output(%0) : !db.column<!storage.node_id>
  return
}
)mlir";

// A factor whose region does not end in a db.yield: the parser cannot recover
// the result types and rejects it.
const char* const missingYieldProgram = R"mlir(
func.func @main() {
  %0 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  return
}
)mlir";

// MATCH (a) RETURN a LIMIT 3: a scan capped by db.limit, one pass-through column.
const char* const limitProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %la = db.limit(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%la) : !db.column<!storage.node_id>
  return
}
)mlir";

// db.limit's count is a ui64 attribute, so a negative literal cannot parse.
const char* const negativeLimitProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %la = db.limit(%a) count -1 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%la) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a) RETURN a SKIP 3: a scan whose first three rows are dropped by db.skip,
// one pass-through column.
const char* const skipProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %sa = db.skip(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sa) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score ORDER BY a.score DESC: two columns passed through,
// sorted by the second one (the score), descending.
const char* const sortProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %sa, %sscore = db.sort(%a, %score) keys [1] ascending [false] : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%sa, %sscore) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// A sort key indexing a column that does not exist: only one column, key 5.
const char* const sortKeyOutOfRangeProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %sa = db.sort(%a) keys [5] ascending [true] : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sa) : !db.column<!storage.node_id>
  return
}
)mlir";

// db.skip's count is a ui64 attribute, so a negative literal cannot parse.
const char* const negativeSkipProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %sa = db.skip(%a) count -1 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sa) : !db.column<!storage.node_id>
  return
}
)mlir";

// Two keys but one direction: the parallel key arrays disagree on length.
const char* const sortMismatchedKeysProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %sa = db.sort(%a) keys [0, 0] ascending [true] : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sa) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a) RETURN DISTINCT a, a.score: two columns passed through, deduped on the
// whole (node, score) row - no key attribute, since every column is part of the
// dedup key.
const char* const removeDuplicatesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %da, %dscore = db.remove_duplicates(%a, %score) : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%da, %dscore) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN count(a): one column in, one integer column out. Unlike the
// pass-through ops, count changes both arity (only ever one result) and type (an
// integer, whatever the input column's type).
const char* const countProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %n = db.count(%a) : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%n) : !db.column<ui64>
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
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(results[0].getType(), nodeIDColumnType);
    EXPECT_EQ(results[1].getType(), nodeIDColumnType);

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

    const mlir::Type nodeIDType = mlir::storage::NodeIDType::get(&_context);
    const mlir::Type colA = mlir::db::ColumnType::get(&_context, nodeIDType);
    const mlir::Type colB = mlir::db::ColumnType::get(&_context, nodeIDType);

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
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(limit.getColumns()[0].getType(), nodeIDColumnType);
    EXPECT_EQ(limit.getResults()[0].getType(), nodeIDColumnType);
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

    const mlir::Type colA = mlir::db::ColumnType::get(&_context);
    const mlir::Type colB = mlir::db::ColumnType::get(&_context);

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

// Builds a db.limit with no columns. Its row count is read from the first column
// during lowering, so the verifier rejects it here rather than leaving the empty
// case to the lowering pass.
TEST_F(DBDialectTest, verifierRejectsLimitWithoutColumns) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    // No input columns and no results: nothing to limit.
    auto limit = builder.create<mlir::db::Limit>(loc,
                                                 mlir::TypeRange {},
                                                 mlir::ValueRange {},
                                                 /*count=*/3u);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(limit.getOperation())));
}

// Builds a db.limit whose single result type differs from its single input
// column: same arity, wrong type. This exercises the per-column type check the
// arity-mismatch test never reaches.
TEST_F(DBDialectTest, verifierRejectsLimitColumnTypeMismatch) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type nodeIDType = mlir::storage::NodeIDType::get(&_context);
    const mlir::Type colA = mlir::db::ColumnType::get(&_context, nodeIDType);
    const mlir::Type colB = mlir::db::ColumnType::get(&_context);

    auto scanA = builder.create<mlir::db::ScanNodes>(loc, colA);

    // One input column and one result, but the result type does not match it.
    auto limit = builder.create<mlir::db::Limit>(loc,
                                                 mlir::TypeRange {colB},
                                                 mlir::ValueRange {scanA.getResult()},
                                                 /*count=*/3u);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(limit.getOperation())));
}

TEST_F(DBDialectTest, parsesSkip) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(skipProgram);
    ASSERT_TRUE(module);

    mlir::db::Skip skip;
    module.get().walk([&](mlir::db::Skip op) {
        skip = op;
    });
    ASSERT_TRUE(skip);

    // The count is the parsed ui64, and the single column passes through as the
    // single result, same type.
    EXPECT_EQ(skip.getCount(), 3u);
    ASSERT_EQ(skip.getColumns().size(), 1u);
    ASSERT_EQ(skip.getResults().size(), 1u);
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(skip.getColumns()[0].getType(), nodeIDColumnType);
    EXPECT_EQ(skip.getResults()[0].getType(), nodeIDColumnType);
}

TEST_F(DBDialectTest, skipRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(skipProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.skip printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, parsesSort) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sortProgram);
    ASSERT_TRUE(module);

    mlir::db::Sort sort;
    module.get().walk([&](mlir::db::Sort op) {
        sort = op;
    });
    ASSERT_TRUE(sort);

    // Two columns pass through as two results of the same types.
    ASSERT_EQ(sort.getColumns().size(), 2u);
    ASSERT_EQ(sort.getResults().size(), 2u);
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(sort.getColumns()[0].getType(), nodeIDColumnType);
    EXPECT_EQ(sort.getResults()[0].getType(), nodeIDColumnType);

    // The single key is column 1 (the score), descending.
    ASSERT_EQ(sort.getKeyColumns().size(), 1u);
    EXPECT_EQ(sort.getKeyColumns()[0], 1);
    ASSERT_EQ(sort.getKeyAscending().size(), 1u);
    EXPECT_FALSE(sort.getKeyAscending()[0]);
}

TEST_F(DBDialectTest, sortRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sortProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.sort printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, rejectsNegativeSkipCount) {
    // count is a ui64 attribute, so a negative literal fails to parse and the
    // module is null.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(negativeSkipProgram);
    EXPECT_FALSE(module);
}

// Builds a db.skip whose result count does not match its columns, exercising the
// verifier's pass-through arity check on the programmatic builder path (the parser
// path always derives the results from the printed functional type).
TEST_F(DBDialectTest, verifierRejectsSkipArityMismatch) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type colA = mlir::db::ColumnType::get(&_context);
    const mlir::Type colB = mlir::db::ColumnType::get(&_context);

    auto scanA = builder.create<mlir::db::ScanNodes>(loc, colA);

    // One input column but two declared results: the pass-through arity is wrong.
    auto skip = builder.create<mlir::db::Skip>(loc,
                                               mlir::TypeRange {colA, colB},
                                               mlir::ValueRange {scanA.getResult()},
                                               /*count=*/3u);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(skip.getOperation())));
}

// Builds a db.skip with no columns. Its row count is read from the first column
// during lowering, so the verifier rejects it here rather than leaving the empty
// case to the lowering pass.
TEST_F(DBDialectTest, verifierRejectsSkipWithoutColumns) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    // No input columns and no results: nothing to skip.
    auto skip = builder.create<mlir::db::Skip>(loc,
                                               mlir::TypeRange {},
                                               mlir::ValueRange {},
                                               /*count=*/3u);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(skip.getOperation())));
}

// Builds a db.skip whose single result type differs from its single input column:
// same arity, wrong type. This exercises the per-column type check the
// arity-mismatch test never reaches.
TEST_F(DBDialectTest, verifierRejectsSkipColumnTypeMismatch) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type nodeIDType = mlir::storage::NodeIDType::get(&_context);
    const mlir::Type colA = mlir::db::ColumnType::get(&_context, nodeIDType);
    const mlir::Type colB = mlir::db::ColumnType::get(&_context);

    auto scanA = builder.create<mlir::db::ScanNodes>(loc, colA);

    // One input column and one result, but the result type does not match it.
    auto skip = builder.create<mlir::db::Skip>(loc,
                                               mlir::TypeRange {colB},
                                               mlir::ValueRange {scanA.getResult()},
                                               /*count=*/3u);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(skip.getOperation())));
}

TEST_F(DBDialectTest, rejectsSortKeyOutOfRange) {
    // The verifier runs during parsing, so a key that indexes no column makes the
    // module fail to parse.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sortKeyOutOfRangeProgram);
    EXPECT_FALSE(module);
}

TEST_F(DBDialectTest, rejectsSortMismatchedKeyArrays) {
    // Two key indices but one direction: the parallel arrays disagree, so the
    // verifier rejects it during parsing.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sortMismatchedKeysProgram);
    EXPECT_FALSE(module);
}

// Builds a db.sort with no columns and no keys. Its row order is read from its
// columns during lowering, so the verifier rejects the empty case here rather
// than leaving it to the lowering pass.
TEST_F(DBDialectTest, verifierRejectsSortWithoutColumns) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    // No columns, no results, no keys: nothing to sort.
    auto sort = builder.create<mlir::db::Sort>(loc,
                                               mlir::TypeRange {},
                                               mlir::ValueRange {},
                                               builder.getDenseI64ArrayAttr({}),
                                               builder.getDenseBoolArrayAttr({}));

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(sort.getOperation())));
}

TEST_F(DBDialectTest, parsesRemoveDuplicates) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(removeDuplicatesProgram);
    ASSERT_TRUE(module);

    mlir::db::RemoveDuplicates distinct;
    module.get().walk([&](mlir::db::RemoveDuplicates op) {
        distinct = op;
    });
    ASSERT_TRUE(distinct);

    // Two columns pass through as two results of the same types; the whole row is
    // the dedup key, so there is no key attribute to read.
    ASSERT_EQ(distinct.getColumns().size(), 2u);
    ASSERT_EQ(distinct.getResults().size(), 2u);
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(distinct.getColumns()[0].getType(), nodeIDColumnType);
    EXPECT_EQ(distinct.getResults()[0].getType(), nodeIDColumnType);
}

TEST_F(DBDialectTest, removeDuplicatesRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(removeDuplicatesProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.remove_duplicates printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

// Builds a db.remove_duplicates whose result count does not match its columns,
// exercising the pass-through arity check on the programmatic builder path (the
// parser path always derives the results from the printed functional type).
TEST_F(DBDialectTest, verifierRejectsRemoveDuplicatesArityMismatch) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type colA = mlir::db::ColumnType::get(&_context);
    const mlir::Type colB = mlir::db::ColumnType::get(&_context);

    auto scanA = builder.create<mlir::db::ScanNodes>(loc, colA);

    // One input column but two declared results: the pass-through arity is wrong.
    auto distinct = builder.create<mlir::db::RemoveDuplicates>(loc,
                                                               mlir::TypeRange {colA, colB},
                                                               mlir::ValueRange {scanA.getResult()});

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(distinct.getOperation())));
}

// Builds a db.remove_duplicates with no columns. Its row set is read from the
// first column during lowering, so the verifier rejects the empty case here rather
// than leaving it to the lowering pass.
TEST_F(DBDialectTest, verifierRejectsRemoveDuplicatesWithoutColumns) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    // No input columns and no results: nothing to deduplicate.
    auto distinct = builder.create<mlir::db::RemoveDuplicates>(loc,
                                                               mlir::TypeRange {},
                                                               mlir::ValueRange {});

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(distinct.getOperation())));
}

TEST_F(DBDialectTest, parsesCount) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(countProgram);
    ASSERT_TRUE(module);

    mlir::db::Count count;
    module.get().walk([&](mlir::db::Count op) {
        count = op;
    });
    ASSERT_TRUE(count);

    // One column in, one integer column out - the input and result types are
    // unrelated (a node ID column counted into a ui64 column).
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type countColumnType = mlir::db::ColumnType::get(&_context, mlir::IntegerType::get(&_context, 64, mlir::IntegerType::Unsigned));
    EXPECT_EQ(count.getInput().getType(), nodeIDColumnType);
    EXPECT_EQ(count.getResult().getType(), countColumnType);
}

TEST_F(DBDialectTest, countRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(countProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the db.count
    // printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

}
