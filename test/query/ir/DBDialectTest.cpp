#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"
#include "StorageTypes.h"

namespace {

// Defined below with the other program strings; declared here so the fixture's
// checkAggregateOp helper (an inline member) can name it.
std::string aggregateProgram(const char* op);

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

    // Parses one aggregate op program (db.sum / db.min / db.max / db.avg), asserts
    // the op is present with the spelled input/result column types, and that printing
    // it and re-parsing still verifies (the printer and parser are inverses). One
    // typed helper covers all four ops, which differ only in their name.
    template <typename OpType>
    void checkAggregateOp(const char* op) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(aggregateProgram(op).c_str());
        ASSERT_TRUE(module) << op;

        OpType aggregate;
        module.get().walk([&](OpType found) {
            aggregate = found;
        });
        ASSERT_TRUE(aggregate) << op;

        // One property column in, one reduced column out - the two spelled i64 columns.
        const mlir::Type int64ColumnType = mlir::db::ColumnType::get(&_context, mlir::IntegerType::get(&_context, 64));
        EXPECT_EQ(aggregate.getInput().getType(), int64ColumnType) << op;
        EXPECT_EQ(aggregate.getResult().getType(), int64ColumnType) << op;

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module.get().print(stream);

        const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
        ASSERT_TRUE(reparsed) << op;
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed))) << op;
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

// MATCH (a), (b), (c) RETURN a, b, c: a three-way product. The db dialect has no
// n-ary cross_product; a third factor is expressed by nesting, so the outer
// product's left factor is itself a db.cross_product crossing (a) with (b), and
// the outer product crosses that pair with (c). The factor region holds another
// db.cross_product - the op nests - and the outer product surfaces three columns.
const char* const nestedCrossProductProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %1:2 = db.cross_product factor {
      %a = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %a : !db.column<!storage.node_id>
    } factor {
      %b = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %b : !db.column<!storage.node_id>
    }
    db.yield %1#0, %1#1 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %c = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %c : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
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

// MATCH (a) RETURN a AS person: the projection names the column it emits.
const char* const namedOutputProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  db.output(%a) names ["person"] : !db.column<!storage.node_id>
  return
}
)mlir";

// Two names for one emitted column: the second names a column that is not there.
const char* const mismatchedOutputNamesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  db.output(%a) names ["person", "extra"] : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person) SET n.age = n.age
const char* const setNodePropertyProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %v = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.set_node_property(%n, "age", %v) : (!db.column<!storage.node_id>, !db.column<none>) -> ()
  return
}
)mlir";

// MATCH ()-[e]->() SET e.weight = e.weight
const char* const setEdgePropertyProgram = R"mlir(
func.func @main() {
  %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %v = db.get_edge_properties(%e, "weight") : (!db.column<!storage.edge_id>) -> !db.column<none>
  db.set_edge_property(%e, "weight", %v) : (!db.column<!storage.edge_id>, !db.column<none>) -> ()
  return
}
)mlir";

const char* const emptyPropertySetProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %v = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.set_node_property(%n, "", %v) : (!db.column<!storage.node_id>, !db.column<none>) -> ()
  return
}
)mlir";

// MATCH (n:Person) DETACH DELETE n
const char* const detachDeleteNodeProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  db.delete_node(%n) detach : (!db.column<!storage.node_id>) -> ()
  return
}
)mlir";

// MATCH (n:Person) DELETE n
const char* const deleteNodeProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  db.delete_node(%n) : (!db.column<!storage.node_id>) -> ()
  return
}
)mlir";

// MATCH ()-[e]->() DELETE e
const char* const deleteEdgeProgram = R"mlir(
func.func @main() {
  %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.delete_edge(%e) : (!db.column<!storage.edge_id>) -> ()
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

// MATCH (a) RETURN count(DISTINCT a): the same shape as countProgram with the distinct
// flag set, so the tally charges each distinct value once. The flag is a unit
// attribute printed as its own keyword ahead of the colon, which is what the round
// trip has to preserve.
const char* const countDistinctProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %n = db.count(%a) distinct : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%n) : !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN sum(a.score): a property column reduced to a single-row result.
// Each reduction is its own op - db.sum / db.min / db.max / db.avg - the way count(*)
// is db.count. Like db.count they change arity and type, so the input and result
// types are unrelated; only the op name differs, which lets one program string cover
// all four ops.
std::string aggregateProgram(const char* op) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %s = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<i64>\n"
                       "  %r = db.")
           + op
           + "(%s) : (!db.column<i64>) -> !db.column<i64>\n"
             "  db.output(%r) : !db.column<i64>\n"
             "  return\n"
             "}\n";
}

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

TEST_F(DBDialectTest, parsesNestedCrossProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nestedCrossProductProgram);
    ASSERT_TRUE(module);

    // Two products in the module: the outer one and the inner one nested in its
    // left factor.
    llvm::SmallVector<mlir::db::CrossProduct, 2> products;
    module.get().walk([&](mlir::db::CrossProduct op) {
        products.push_back(op);
    });
    ASSERT_EQ(products.size(), 2u);

    // The outer product surfaces three columns (a, b, c); the inner one two
    // (a, b) - which tells the two apart independently of walk order.
    mlir::db::CrossProduct outer;
    mlir::db::CrossProduct inner;
    for (mlir::db::CrossProduct product : products) {
        if (product.getResults().size() == 3u) {
            outer = product;
        } else {
            inner = product;
        }
    }
    ASSERT_TRUE(outer);
    ASSERT_TRUE(inner);

    // The inner product sits inside the outer's left factor, so a factor region
    // does hold another db.cross_product - the op nests.
    EXPECT_EQ(inner->getParentOfType<mlir::db::CrossProduct>().getOperation(), outer.getOperation());

    // All three of the outer product's columns are node ID columns.
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Operation::result_range outerResults = outer.getResults();
    ASSERT_EQ(outerResults.size(), 3u);
    for (const mlir::Value result : outerResults) {
        EXPECT_EQ(result.getType(), nodeIDColumnType);
    }
}

TEST_F(DBDialectTest, nestedCrossProductRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nestedCrossProductProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing a nested product yields a module that still
    // verifies, so the custom printer and parser are inverses through nesting.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
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

TEST_F(DBDialectTest, parsesOutputColumnNames) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(namedOutputProgram);
    ASSERT_TRUE(module);

    mlir::db::Output output;
    module.get().walk([&](mlir::db::Output op) {
        output = op;
    });
    ASSERT_TRUE(output);

    const mlir::ArrayAttr columnNames = output.getColumnNamesAttr();
    ASSERT_TRUE(columnNames);
    ASSERT_EQ(columnNames.size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(columnNames[0]).getValue(), "person");
}

// The names are optional, so a program that names nothing carries no array at all -
// which is what every hand-written db.output above relies on.
TEST_F(DBDialectTest, parsesOutputWithoutColumnNames) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(limitProgram);
    ASSERT_TRUE(module);

    mlir::db::Output output;
    module.get().walk([&](mlir::db::Output op) {
        output = op;
    });
    ASSERT_TRUE(output);

    EXPECT_FALSE(output.getColumnNamesAttr());
}

TEST_F(DBDialectTest, outputColumnNamesRoundTripThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(namedOutputProgram);
    ASSERT_TRUE(module);

    // The names print after the `names` keyword and re-parse into a module that still
    // verifies, so the printer and parser are inverses over them too.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    EXPECT_NE(printed.find("names [\"person\"]"), std::string::npos) << printed;

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, rejectsOutputNameCountMismatch) {
    // One name per emitted column: the verifier rejects the extra name and the module
    // fails to verify during parsing.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(mismatchedOutputNamesProgram);
    EXPECT_FALSE(module);
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

TEST_F(DBDialectTest, parsesCountDistinct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(countDistinctProgram);
    ASSERT_TRUE(module);

    mlir::db::Count count;
    module.get().walk([&](mlir::db::Count op) {
        count = op;
    });
    ASSERT_TRUE(count);

    // The keyword sets the flag; the columns are the same as a plain count's, since
    // deduplicating the input changes how many rows are charged, not the result type.
    EXPECT_TRUE(count.getDistinct());

    const mlir::OwningOpRef<mlir::ModuleOp> plainModule = parse(countProgram);
    ASSERT_TRUE(plainModule);

    mlir::db::Count plainCount;
    plainModule.get().walk([&](mlir::db::Count op) {
        plainCount = op;
    });
    ASSERT_TRUE(plainCount);
    EXPECT_FALSE(plainCount.getDistinct());
}

TEST_F(DBDialectTest, countDistinctRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(countDistinctProgram);
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    // The flag survives the round trip as its keyword, so re-parsing yields a count
    // that is still distinct - a printer dropping it would silently turn the query
    // back into a plain count.
    EXPECT_NE(printed.find("distinct"), std::string::npos);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));

    mlir::db::Count count;
    reparsed.get().walk([&](mlir::db::Count op) {
        count = op;
    });
    ASSERT_TRUE(count);
    EXPECT_TRUE(count.getDistinct());
}

TEST_F(DBDialectTest, parsesAndRoundTripsAggregateOps) {
    // Each aggregate is its own op - db.sum / db.min / db.max / db.avg - so each
    // parses to its own type, exposes the spelled input/result columns, and survives
    // a print/re-parse round trip.
    checkAggregateOp<mlir::db::Sum>("sum");
    checkAggregateOp<mlir::db::Min>("min");
    checkAggregateOp<mlir::db::Max>("max");
    checkAggregateOp<mlir::db::Avg>("avg");
}

// MATCH (a:Person:Employee) RETURN a: a scan restricted to the nodes carrying
// both labels.
const char* const scanByLabelProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person", "Employee"]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// A label scan with no labels: rejected by the verifier.
const char* const emptyLabelScanProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label([]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE id(n) IN [0, 3, 7] RETURN n: a scan of a fixed, listed set of
// node IDs.
const char* const constScanNodesProgram = R"mlir(
func.func @main() {
  %a = db.const_scan_nodes([0, 3, 7]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// A const scan with no node IDs: a valid, empty (zero-row) scan.
const char* const emptyConstScanNodesProgram = R"mlir(
func.func @main() {
  %a = db.const_scan_nodes([]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// UNWIND [1, 2, 3] AS x RETURN x: a source over a homogeneous literal list, so the
// elements share one type and the result is a typed i64 column.
const char* const unwindConstProgram = R"mlir(
func.func @main() {
  %x = db.unwind_const([1, 2, 3]) : !db.column<i64>
  db.output(%x) : !db.column<i64>
  return
}
)mlir";

// UNWIND [true, "string", 10] AS x RETURN x: a heterogeneous list, so the elements
// share no single type and the result is a type-erased list_element column.
const char* const heterogeneousUnwindConstProgram = R"mlir(
func.func @main() {
  %x = db.unwind_const([true, "string", 10]) : !db.column<!storage.list_element>
  db.output(%x) : !db.column<!storage.list_element>
  return
}
)mlir";

// UNWIND [] AS x: an empty list is the type-erased form and yields no row - valid.
const char* const emptyUnwindConstProgram = R"mlir(
func.func @main() {
  %x = db.unwind_const([]) : !db.column<!storage.list_element>
  db.output(%x) : !db.column<!storage.list_element>
  return
}
)mlir";

// A mixed-type list typed as a homogeneous i64 column: the verifier rejects it, since
// a homogeneous unwind_const requires every element to share one type.
const char* const mixedHomogeneousUnwindConstProgram = R"mlir(
func.func @main() {
  %x = db.unwind_const([1, "string"]) : !db.column<i64>
  db.output(%x) : !db.column<i64>
  return
}
)mlir";

// MATCH ()-[e]->() RETURN a, b: scan every edge, exposing the four edge columns
// (source node, edge, edge type, target node); the output keeps the endpoints.
const char* const scanEdgesProgram = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(DBDialectTest, parsesScanNodesByLabel) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(scanByLabelProgram);
    ASSERT_TRUE(module);

    mlir::db::ScanNodesByLabel scan;
    module.get().walk([&](mlir::db::ScanNodesByLabel op) {
        scan = op;
    });
    ASSERT_TRUE(scan);

    // The two spelled label names come back in order, and the result is a single
    // node ID column.
    const mlir::ArrayAttr labels = scan.getLabels();
    ASSERT_EQ(labels.size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Person");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[1]).getValue(), "Employee");

    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(scan.getResult().getType(), nodeIDColumnType);
}

TEST_F(DBDialectTest, scanNodesByLabelRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(scanByLabelProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.scan_nodes_by_label printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, verifierRejectsScanNodesByLabelWithoutLabels) {
    // The op parses - an empty array is well-formed syntax - but the verifier
    // rejects a label-free label scan, so the module fails to build.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(emptyLabelScanProgram);
    EXPECT_FALSE(module);
}

TEST_F(DBDialectTest, parsesConstScanNodes) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(constScanNodesProgram);
    ASSERT_TRUE(module);

    mlir::db::ConstScanNodes scan;
    module.get().walk([&](mlir::db::ConstScanNodes op) {
        scan = op;
    });
    ASSERT_TRUE(scan);

    // The three listed node IDs come back in order, and the result is a single
    // node ID column.
    const llvm::ArrayRef<int64_t> nodeIDs = scan.getNodeIDs();
    ASSERT_EQ(nodeIDs.size(), 3u);
    EXPECT_EQ(nodeIDs[0], 0);
    EXPECT_EQ(nodeIDs[1], 3);
    EXPECT_EQ(nodeIDs[2], 7);

    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(scan.getResult().getType(), nodeIDColumnType);
}

TEST_F(DBDialectTest, constScanNodesRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(constScanNodesProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.const_scan_nodes printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, constScanNodesWithoutNodeIDsIsValid) {
    // An empty node ID list is a valid const scan - it simply yields no row, the
    // same set-membership semantics as a list whose IDs are all absent - so the
    // module builds and verifies.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(emptyConstScanNodesProgram);
    ASSERT_TRUE(module);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));
}

TEST_F(DBDialectTest, parsesUnwindConst) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindConstProgram);
    ASSERT_TRUE(module);

    mlir::db::UnwindConst unwind;
    module.get().walk([&](mlir::db::UnwindConst op) {
        unwind = op;
    });
    ASSERT_TRUE(unwind);

    // The three literals come back in order, and a homogeneous list resolves to a
    // typed i64 column - the result element type carries the homogeneity verdict.
    const mlir::ArrayAttr elements = unwind.getElements();
    ASSERT_EQ(elements.size(), 3u);

    const mlir::Type int64ColumnType = mlir::db::ColumnType::get(&_context, mlir::IntegerType::get(&_context, 64));
    EXPECT_EQ(unwind.getResult().getType(), int64ColumnType);
}

TEST_F(DBDialectTest, unwindConstRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindConstProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.unwind_const printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, parsesHeterogeneousUnwindConst) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(heterogeneousUnwindConstProgram);
    ASSERT_TRUE(module);

    mlir::db::UnwindConst unwind;
    module.get().walk([&](mlir::db::UnwindConst op) {
        unwind = op;
    });
    ASSERT_TRUE(unwind);

    // A heterogeneous list keeps every element but resolves to a type-erased
    // list_element column, since the elements share no single type.
    const mlir::ArrayAttr elements = unwind.getElements();
    ASSERT_EQ(elements.size(), 3u);

    const mlir::Type listElementColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::ListElementType::get(&_context));
    EXPECT_EQ(unwind.getResult().getType(), listElementColumnType);
}

TEST_F(DBDialectTest, heterogeneousUnwindConstRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(heterogeneousUnwindConstProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing a mixed-type list yields a module that still verifies,
    // so the printer and parser are inverses through the list_element column too.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, unwindConstWithEmptyListIsValid) {
    // An empty list is the type-erased list_element form: it simply yields no row, so
    // the module builds and verifies.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(emptyUnwindConstProgram);
    ASSERT_TRUE(module);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));
}

TEST_F(DBDialectTest, verifierRejectsHomogeneousUnwindConstWithMixedElements) {
    // A mixed-type list typed as a homogeneous i64 column fails the verifier during
    // parsing, so the module is null.
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(mixedHomogeneousUnwindConstProgram);
    EXPECT_FALSE(module);
}

// MATCH (a)-[:KNOWS]->(b)<-[:LIKES]-(c): one by-type out-edge hop and one by-type
// in-edge hop, so both ops' assembly formats (the type name inside the parens)
// are exercised.
const char* const edgesByTypeProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s0, %e0, %et0, %b = db.get_out_edges_by_type(%a, "KNOWS", {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s1, %e1, %et1, %c = db.get_in_edges_by_type(%b, "LIKES", {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%s0, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(DBDialectTest, parsesGetEdgesByType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgesByTypeProgram);
    ASSERT_TRUE(module);

    // The type name spelled inside each op's parens comes back on the edge_type
    // attribute.
    mlir::db::GetOutEdgesByType outByType;
    module.get().walk([&](mlir::db::GetOutEdgesByType op) {
        outByType = op;
    });
    ASSERT_TRUE(outByType);
    EXPECT_EQ(outByType.getEdgeType(), "KNOWS");

    mlir::db::GetInEdgesByType inByType;
    module.get().walk([&](mlir::db::GetInEdgesByType op) {
        inByType = op;
    });
    ASSERT_TRUE(inByType);
    EXPECT_EQ(inByType.getEdgeType(), "LIKES");
}

TEST_F(DBDialectTest, edgesByTypeRoundTripThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgesByTypeProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // by-type edge op printers and parsers are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, parsesScanEdges) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(scanEdgesProgram);
    ASSERT_TRUE(module);

    mlir::db::ScanEdges scan;
    module.get().walk([&](mlir::db::ScanEdges op) {
        scan = op;
    });
    ASSERT_TRUE(scan);

    // The four results are the source node, the edge, its type and the target
    // node - the same shape and order db.get_out_edges exposes.
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type edgeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::EdgeIDType::get(&_context));
    const mlir::Type edgeTypeColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::EdgeTypeIDType::get(&_context));
    EXPECT_EQ(scan.getSrcids().getType(), nodeIDColumnType);
    EXPECT_EQ(scan.getEids().getType(), edgeIDColumnType);
    EXPECT_EQ(scan.getEtypes().getType(), edgeTypeColumnType);
    EXPECT_EQ(scan.getTgtids().getType(), nodeIDColumnType);
}

TEST_F(DBDialectTest, scanEdgesRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(scanEdgesProgram);
    ASSERT_TRUE(module);

    // Printing then re-parsing yields a module that still verifies, so the
    // db.scan_edges printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, parsesSetNodeProperty) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(setNodePropertyProgram);
    ASSERT_TRUE(module);

    mlir::db::SetNodeProperty setNode;
    module.get().walk([&](mlir::db::SetNodeProperty op) {
        setNode = op;
    });
    ASSERT_TRUE(setNode);

    EXPECT_EQ(setNode.getProperty(), "age");
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(setNode.getInputNodes().getType(), nodeIDColumnType);
    EXPECT_EQ(setNode->getNumResults(), 0U);
}

TEST_F(DBDialectTest, setNodePropertyRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(setNodePropertyProgram);
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, parsesSetEdgeProperty) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(setEdgePropertyProgram);
    ASSERT_TRUE(module);

    mlir::db::SetEdgeProperty setEdge;
    module.get().walk([&](mlir::db::SetEdgeProperty op) {
        setEdge = op;
    });
    ASSERT_TRUE(setEdge);

    EXPECT_EQ(setEdge.getProperty(), "weight");
    const mlir::Type edgeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::EdgeIDType::get(&_context));
    EXPECT_EQ(setEdge.getInputEdges().getType(), edgeIDColumnType);
    EXPECT_EQ(setEdge->getNumResults(), 0U);
}

TEST_F(DBDialectTest, setEdgePropertyRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(setEdgePropertyProgram);
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, verifierRejectsEmptySetProperty) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(emptyPropertySetProgram);
    EXPECT_FALSE(module);
}

TEST_F(DBDialectTest, parsesDetachDeleteNode) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(detachDeleteNodeProgram);
    ASSERT_TRUE(module);

    mlir::db::DeleteNode deleteNode;
    module.get().walk([&](mlir::db::DeleteNode op) {
        deleteNode = op;
    });
    ASSERT_TRUE(deleteNode);

    // The `detach` keyword parsed to a set unit attribute; the input is a node
    // column and the op has no result.
    EXPECT_TRUE(deleteNode.getDetach());
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    EXPECT_EQ(deleteNode.getInputNodes().getType(), nodeIDColumnType);
    EXPECT_EQ(deleteNode->getNumResults(), 0u);
}

TEST_F(DBDialectTest, parsesDeleteNodeWithoutDetach) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(deleteNodeProgram);
    ASSERT_TRUE(module);

    mlir::db::DeleteNode deleteNode;
    module.get().walk([&](mlir::db::DeleteNode op) {
        deleteNode = op;
    });
    ASSERT_TRUE(deleteNode);

    // No `detach` keyword, so the unit attribute is absent.
    EXPECT_FALSE(deleteNode.getDetach());
}

TEST_F(DBDialectTest, deleteNodeRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(detachDeleteNodeProgram);
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

TEST_F(DBDialectTest, parsesDeleteEdge) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(deleteEdgeProgram);
    ASSERT_TRUE(module);

    mlir::db::DeleteEdge deleteEdge;
    module.get().walk([&](mlir::db::DeleteEdge op) {
        deleteEdge = op;
    });
    ASSERT_TRUE(deleteEdge);

    const mlir::Type edgeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::EdgeIDType::get(&_context));
    EXPECT_EQ(deleteEdge.getInputEdges().getType(), edgeIDColumnType);
    EXPECT_EQ(deleteEdge->getNumResults(), 0u);
}

TEST_F(DBDialectTest, deleteEdgeRoundTripsThroughTextualForm) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(deleteEdgeProgram);
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

// `MATCH (n:Person) RETURN n` with no optimisation applied
const char* const labelScanChainProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_label_set(%0) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %2 = db.check_label_constraint(%1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %3 = db.filter(%2, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%3) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(DBDialectTest, fuseScanByLabelCollapsesLabelFilterChain) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelScanChainProgram);
    ASSERT_TRUE(module);

    mlir::PassManager passManager(&_context);
    passManager.addPass(mlir::db::createFuseScanByLabel());
    ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));

    // The chain collapsed to one scan_nodes_by_label carrying the label.
    mlir::db::ScanNodesByLabel scanByLabel;
    module.get().walk([&](mlir::db::ScanNodesByLabel op) {
        scanByLabel = op;
    });
    ASSERT_TRUE(scanByLabel);

    const mlir::ArrayAttr labels = scanByLabel.getLabels();
    ASSERT_EQ(labels.size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Person");

    // None of the original chain survives.
    size_t scans = 0;
    size_t labelSets = 0;
    size_t checks = 0;
    size_t filters = 0;
    module.get().walk([&](mlir::Operation* op) {
        if (mlir::isa<mlir::db::ScanNodes>(op)) {
            scans++;
        } else if (mlir::isa<mlir::db::GetNodeLabelSet>(op)) {
            labelSets++;
        } else if (mlir::isa<mlir::db::CheckLabelConstraint>(op)) {
            checks++;
        } else if (mlir::isa<mlir::db::FilterOp>(op)) {
            filters++;
        }
    });

    EXPECT_EQ(scans, 0u);
    EXPECT_EQ(labelSets, 0u);
    EXPECT_EQ(checks, 0u);
    EXPECT_EQ(filters, 0u);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));
}

}
