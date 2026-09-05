#include <gtest/gtest.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"

#include "NLDialect.h"
#include "NLOps.h"
#include "NLTypes.h"
#include "StorageDialect.h"

namespace {

// A context with just the dialects an nl-dialect program needs: nl for the
// nested-loop ops and func for the enclosing function.
class NLDialectTest : public ::testing::Test {
protected:
    NLDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    // A function whose entry block takes one node-ID chunk argument - the chunk
    // an nl.output emits and an nl.limit_update charges. Leaves the builder at the
    // start of that block, ready to append the ops under test.
    mlir::func::FuncOp buildOneChunkFunction(mlir::OpBuilder& builder, mlir::ModuleOp module) {
        const mlir::Location loc = builder.getUnknownLoc();
        const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::NodeIDType::get(&_context));

        builder.setInsertionPointToEnd(module.getBody());
        auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {chunkType}, {}));
        builder.setInsertionPointToStart(function.addEntryBlock());

        return function;
    }

    // Parses one nl program and reports whether it verified.
    bool parses(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText,
                                                                                                mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    mlir::MLIRContext _context;
};

// The columns an nl.collect_update carries are the buffer's grouping keys, then at least
// one collected value, then one input per reduction the buffer names.
constexpr const char* collectsOneValueUnderOneKey = R"mlir(
func.func @main() {
  %buffer = nl.collect_buffer keys 1 distinct [0]
  %nodes = nl.scan_nodes()
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.collect_update %buffer, (%node, %node) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// A key count past the columns leaves nothing to collect, and summing it with the
// aggregate count would wrap in unsigned 64-bit before the shortfall is noticed.
constexpr const char* collectsUnderMoreKeysThanColumns = R"mlir(
func.func @main() {
  %buffer = nl.collect_buffer keys 18446744073709551615
  %nodes = nl.scan_nodes()
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.collect_update %buffer, (%node, %node) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// A distinct index naming no collected column would dedupe nothing, leaving the plain
// list a collect(DISTINCT x) asked to charge each value once.
constexpr const char* collectsDistinctPastTheValues = R"mlir(
func.func @main() {
  %buffer = nl.collect_buffer keys 0 distinct [3]
  %nodes = nl.scan_nodes()
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.collect_update %buffer, (%node) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

constexpr const char* searchesForThreeNeighbours = R"mlir(
func.func @main() {
  %neighbours = nl.vector_search("vectors", 3, [1.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  func.return
}
)mlir";

constexpr const char* searchesForNoNeighbour = R"mlir(
func.func @main() {
  %neighbours = nl.vector_search("vectors", 0, [1.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  func.return
}
)mlir";

constexpr const char* searchesForAVectorOfNoDimension = R"mlir(
func.func @main() {
  %neighbours = nl.vector_search("vectors", 3, []) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  func.return
}
)mlir";

// An nl.limit_truncate passes its column types through unchanged - results mirror
// the operand chunk types - and verifies in a limit/update/truncate/output chain.
TEST_F(NLDialectTest, verifierAcceptsLimitTruncate) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Limit>(loc, /*count=*/3u).getState();
    builder.create<mlir::nl::LimitUpdate>(loc, handle, chunk);
    mlir::nl::LimitTruncate truncate = builder.create<mlir::nl::LimitTruncate>(loc, handle, mlir::ValueRange {chunk});
    builder.create<mlir::nl::Output>(loc, truncate.getResults(), mlir::Value(), mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(truncate.getResult(0).getType(), chunk.getType());
}

// An nl.skip_truncate passes its column types through unchanged - results mirror
// the operand chunk types - and verifies in a skip/update/truncate/output chain.
TEST_F(NLDialectTest, verifierAcceptsSkipTruncate) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Skip>(loc, /*count=*/3u).getState();
    builder.create<mlir::nl::SkipUpdate>(loc, handle, chunk);
    mlir::nl::SkipTruncate truncate = builder.create<mlir::nl::SkipTruncate>(loc, handle, mlir::ValueRange {chunk});
    builder.create<mlir::nl::Output>(loc, truncate.getResults(), mlir::Value(), mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(truncate.getResult(0).getType(), chunk.getType());
}

// nl.output with no limit handle is unbounded and verifies on its own.
TEST_F(NLDialectTest, verifierAcceptsOutput) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, mlir::Value(), mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// nl.output may carry an optional limit handle (the folded terminal-LIMIT form),
// emitting the prefix the preceding nl.limit_update sized.
TEST_F(NLDialectTest, verifierAcceptsOutputWithLimit) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Limit>(loc, /*count=*/3u).getState();
    builder.create<mlir::nl::LimitUpdate>(loc, handle, chunk);
    mlir::nl::Output output = builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, handle, mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(output.getLimit(), handle);
}

// nl.output may carry an optional skip handle (the folded terminal-SKIP form),
// emitting the surviving suffix at the offset the preceding nl.skip_update sized.
// The skip sibling of verifierAcceptsOutputWithLimit.
TEST_F(NLDialectTest, verifierAcceptsOutputWithSkip) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Skip>(loc, /*count=*/3u).getState();
    builder.create<mlir::nl::SkipUpdate>(loc, handle, chunk);
    // The skip handle goes in the third operand (no limit), so the columns, limit
    // and skip operand segments are all distinct.
    mlir::nl::Output output = builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, mlir::Value(), handle, mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(output.getSkip(), handle);
    EXPECT_FALSE(output.getLimit());
}

// The sort chain verifies: an nl.sort_buffer carrying the key spec, an
// nl.sort_collect appending the chunk, and an nl.sort yielding a sorted iterator
// over that chunk type, all naming the one accumulator handle.
TEST_F(NLDialectTest, verifierAcceptsSortChain) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::SortBuffer>(loc,
                                                                    builder.getDenseI64ArrayAttr({0}),
                                                                    builder.getDenseBoolArrayAttr({false}),
                                                                    /*topK=*/mlir::IntegerAttr()).getState();
    builder.create<mlir::nl::SortCollect>(loc, handle, mlir::ValueRange {chunk});

    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {chunk.getType()});
    mlir::nl::Sort sort = builder.create<mlir::nl::Sort>(loc, iteratorType, handle);
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(sort.getState(), handle);
    EXPECT_EQ(sort.getResult().getType(), iteratorType);
}

// The distinct chain verifies: an nl.distinct producing the seen-set handle and an
// nl.distinct_filter naming it, whose result mirrors the filtered column's chunk
// type (results inferred, like nl.limit_truncate), feeding an nl.output.
TEST_F(NLDialectTest, verifierAcceptsDistinctChain) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Distinct>(loc).getState();
    mlir::nl::DistinctFilter filter = builder.create<mlir::nl::DistinctFilter>(loc, handle, mlir::ValueRange {chunk});
    builder.create<mlir::nl::Output>(loc, filter.getResults(), mlir::Value(), mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(filter.getState(), handle);
    EXPECT_EQ(filter.getResult(0).getType(), chunk.getType());
}

// The count chain verifies: an nl.count producing the tally handle, an
// nl.count_update charging the chunk against it, and an nl.count_result that
// materializes the single ui64 tally chunk, emitted by a function-scope nl.output -
// all naming the one handle.
TEST_F(NLDialectTest, verifierAcceptsCountChain) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Count>(loc).getState();
    builder.create<mlir::nl::CountUpdate>(loc, handle, chunk);

    // The count result is a single non-nullable unsigned-i64 chunk, emitted at
    // function scope (no loop) since the count collapses to one row.
    const mlir::Type countChunkType = mlir::nl::ChunkType::get(&_context,
                                                               mlir::IntegerType::get(&_context, 64, mlir::IntegerType::Unsigned));
    mlir::nl::CountResult result = builder.create<mlir::nl::CountResult>(loc, countChunkType, handle);
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {result.getResult()}, mlir::Value(), mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(result.getState(), handle);
    EXPECT_EQ(result.getResult().getType(), countChunkType);
}

// The aggregate chain verifies: an nl.aggregate producing the accumulator handle,
// an nl.aggregate_update folding the value chunk into it, and an nl.aggregate_result
// that materializes the single reduced chunk, emitted by a function-scope nl.output -
// all naming the one handle and carrying the same reduction. The value sibling of
// the count-chain test.
TEST_F(NLDialectTest, verifierAcceptsAggregateChain) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);

    // A function whose entry block takes one nullable-int64 value chunk - the
    // property column an aggregate folds (unlike count, an aggregate reduces values,
    // so its input is a value chunk, not an ID chunk).
    const mlir::Type int64Type = mlir::IntegerType::get(&_context, 64);
    const mlir::Type valueChunkType = mlir::nl::ChunkType::get(&_context,
                                                               mlir::storage::NullableType::get(&_context, int64Type));
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {valueChunkType}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());
    const mlir::Value chunk = function.getBody().front().getArgument(0);

    // sum over an i64 column: the accumulator (state) and the result are both nullable
    // int64, so the state carries the i64 element type.
    const mlir::nl::AggregateStateType stateType = mlir::nl::AggregateStateType::get(&_context, int64Type);
    const mlir::Value handle = builder.create<mlir::nl::Aggregate>(loc, stateType, mlir::storage::AggregateKind::Sum).getState();
    builder.create<mlir::nl::AggregateUpdate>(loc, handle, chunk, mlir::storage::AggregateKind::Sum);

    const mlir::Type resultChunkType = mlir::nl::ChunkType::get(&_context,
                                                                mlir::storage::NullableType::get(&_context, int64Type));
    mlir::nl::AggregateResult result = builder.create<mlir::nl::AggregateResult>(loc, resultChunkType, handle, mlir::storage::AggregateKind::Sum);
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {result.getResult()}, mlir::Value(), mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(result.getState(), handle);
    EXPECT_EQ(result.getResult().getType(), resultChunkType);
    EXPECT_EQ(result.getKind(), mlir::storage::AggregateKind::Sum);
}

// The update's fold handler is picked from its own kind, but the accumulator was
// reset by the producing nl.aggregate's kind - so a differing kind folds into a
// wrongly-initialized accumulator (a min-reset null read as a present sum, which
// crashes at runtime). The verifier must reject the mismatch before execution.
TEST_F(NLDialectTest, verifierRejectsMismatchedAggregateKind) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);

    const mlir::Type int64Type = mlir::IntegerType::get(&_context, 64);
    const mlir::Type valueChunkType = mlir::nl::ChunkType::get(&_context,
                                                               mlir::storage::NullableType::get(&_context, int64Type));
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {valueChunkType}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());
    const mlir::Value chunk = function.getBody().front().getArgument(0);

    // Reset for a sum, but fold as a min against the same handle.
    const mlir::nl::AggregateStateType stateType = mlir::nl::AggregateStateType::get(&_context, int64Type);
    const mlir::Value handle = builder.create<mlir::nl::Aggregate>(loc, stateType, mlir::storage::AggregateKind::Sum).getState();
    builder.create<mlir::nl::AggregateUpdate>(loc, handle, chunk, mlir::storage::AggregateKind::Min);
    builder.create<mlir::func::ReturnOp>(loc);

    // Swallow the verifier's diagnostic so the deliberate failure does not print.
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(function)));
}

// avg accumulates a running sum as f64, so an nl.aggregate avg whose state is not an
// f64 accumulator (here an i64) would type-confuse the avg handler at runtime; the
// verifier rejects it at the source op.
TEST_F(NLDialectTest, verifierRejectsAvgWithNonFloatState) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type int64Type = mlir::IntegerType::get(&_context, 64);
    const mlir::nl::AggregateStateType i64State = mlir::nl::AggregateStateType::get(&_context, int64Type);
    builder.create<mlir::nl::Aggregate>(loc, i64State, mlir::storage::AggregateKind::Avg);
    builder.create<mlir::func::ReturnOp>(loc);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(function)));
}

// A folded nl.output carries the single handle of the truncate adjacent to it,
// never both: an output with both a limit and a skip handle fails verification.
TEST_F(NLDialectTest, verifierRejectsOutputWithBothLimitAndSkip) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value limitHandle = builder.create<mlir::nl::Limit>(loc, /*count=*/3u).getState();
    const mlir::Value skipHandle = builder.create<mlir::nl::Skip>(loc, /*count=*/3u).getState();
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, limitHandle, skipHandle, mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(function)));
}

// nl.sort_buffer requires one direction per key: two key indices with one
// direction is malformed and the verifier rejects it.
TEST_F(NLDialectTest, verifierRejectsSortBufferMismatchedKeys) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);

    // Position the builder inside a function body; the op under test is verified
    // on its own, so the returned function handle is not needed.
    buildOneChunkFunction(builder, *module);

    auto buffer = builder.create<mlir::nl::SortBuffer>(loc,
                                                       builder.getDenseI64ArrayAttr({0, 1}),
                                                       builder.getDenseBoolArrayAttr({true}),
                                                       /*topK=*/mlir::IntegerAttr());

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(buffer.getOperation())));
}

// nl.sort_buffer may carry an optional top-K bound (the fused ORDER BY ... LIMIT
// form); the accessor reports it and the op still verifies.
TEST_F(NLDialectTest, verifierAcceptsBoundedSortBuffer) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    buildOneChunkFunction(builder, *module);

    const mlir::IntegerAttr topK = builder.getIntegerAttr(builder.getIntegerType(64, /*isSigned=*/false), 5);
    mlir::nl::SortBuffer buffer = builder.create<mlir::nl::SortBuffer>(loc,
                                                                      builder.getDenseI64ArrayAttr({0}),
                                                                      builder.getDenseBoolArrayAttr({true}),
                                                                      topK);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(buffer.getOperation())));
    ASSERT_TRUE(buffer.getTopK().has_value());
    EXPECT_EQ(*buffer.getTopK(), 5u);
}

// nl.scan_nodes_by_label infers a single node-ID chunk iterator - the same
// result shape as nl.scan_nodes - since the label list filters rows, not
// columns. The two spelled labels are carried on the op verbatim.
TEST_F(NLDialectTest, scanNodesByLabelInfersNodeIDIterator) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::ArrayAttr labels = builder.getStrArrayAttr({"Person", "Employee"});
    mlir::nl::ScanNodesByLabel scan = builder.create<mlir::nl::ScanNodesByLabel>(loc, labels);
    builder.create<mlir::func::ReturnOp>(loc);

    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {chunkType});
    EXPECT_EQ(scan.getResult().getType(), iteratorType);
    EXPECT_EQ(scan.getLabels().size(), 2u);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// nl.const_scan_nodes infers a single node-ID chunk iterator - the same result
// shape as nl.scan_nodes - since the ID list picks which rows are emitted, not
// their columns. The listed IDs are carried on the op verbatim.
TEST_F(NLDialectTest, constScanNodesInfersNodeIDIterator) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::DenseI64ArrayAttr nodeIDs = builder.getDenseI64ArrayAttr({0, 3, 7});
    mlir::nl::ConstScanNodes scan = builder.create<mlir::nl::ConstScanNodes>(loc, nodeIDs);
    builder.create<mlir::func::ReturnOp>(loc);

    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {chunkType});
    EXPECT_EQ(scan.getResult().getType(), iteratorType);
    EXPECT_EQ(scan.getNodeIDs().size(), 3u);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// nl.unwind_const spells its iterator type rather than inferring it - the chunk
// element type depends on the literals, not on a fixed shape - so a homogeneous i64
// list is a chunk of i64. The literals ride the op as a typed attribute array.
TEST_F(NLDialectTest, unwindConstBuildsTypedChunkIterator) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type int64Type = mlir::IntegerType::get(&_context, 64);
    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, int64Type);
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {chunkType});

    const mlir::ArrayAttr elements = builder.getArrayAttr({builder.getI64IntegerAttr(1),
                                                           builder.getI64IntegerAttr(2),
                                                           builder.getI64IntegerAttr(3)});
    mlir::nl::UnwindConst unwind = builder.create<mlir::nl::UnwindConst>(loc, iteratorType, elements);
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_EQ(unwind.getResult().getType(), iteratorType);
    EXPECT_EQ(unwind.getElements().size(), 3u);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// Building an nl.unwind_const, printing the module and re-parsing it yields a module
// that still verifies, so the nl.unwind_const printer and parser are inverses.
TEST_F(NLDialectTest, unwindConstRoundTripsThroughTextualForm) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type int64Type = mlir::IntegerType::get(&_context, 64);
    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, int64Type);
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {chunkType});

    const mlir::ArrayAttr elements = builder.getArrayAttr({builder.getI64IntegerAttr(1),
                                                           builder.getI64IntegerAttr(2),
                                                           builder.getI64IntegerAttr(3)});
    builder.create<mlir::nl::UnwindConst>(loc, iteratorType, elements);
    builder.create<mlir::func::ReturnOp>(loc);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed =
        mlir::parseSourceString<mlir::ModuleOp>(printed, mlir::ParserConfig(&_context));
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

// nl.load_csv spells its iterator type rather than inferring it - one chunk per field the
// load produces, however many the query named - and every field rides an owning string
// chunk, since the characters were parsed here rather than borrowed from the graph.
TEST_F(NLDialectTest, loadCSVBuildsOneOwningStringChunkPerField) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::OwnedStringType::get(&_context));
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {chunkType, chunkType});

    const mlir::ArrayAttr fields = builder.getArrayAttr({builder.getStringAttr("name"),
                                                         builder.getStringAttr("city")});
    mlir::nl::LoadCSV load = builder.create<mlir::nl::LoadCSV>(loc,
                                                               iteratorType,
                                                               builder.getStringAttr("people.csv"),
                                                               fields,
                                                               builder.getUnitAttr(),
                                                               mlir::UnitAttr());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_EQ(load.getResult().getType(), iteratorType);
    EXPECT_EQ(load.getPath(), "people.csv");
    EXPECT_EQ(load.getFields().size(), 2u);
    EXPECT_TRUE(load.getWithHeaders());
    EXPECT_FALSE(load.getSkipOnError());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// Building an nl.load_csv, printing the module and re-parsing it yields a module that
// still verifies, so the nl.load_csv printer and parser are inverses - the flags and the
// field list included.
TEST_F(NLDialectTest, loadCSVRoundTripsThroughTextualForm) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::OwnedStringType::get(&_context));
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {chunkType});

    const mlir::IntegerAttr position = mlir::IntegerAttr::get(builder.getIntegerType(64, /*isSigned=*/false), 2);
    builder.create<mlir::nl::LoadCSV>(loc,
                                      iteratorType,
                                      builder.getStringAttr("people.csv"),
                                      builder.getArrayAttr({position}),
                                      mlir::UnitAttr(),
                                      builder.getUnitAttr());
    builder.create<mlir::func::ReturnOp>(loc);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed =
        mlir::parseSourceString<mlir::ModuleOp>(printed, mlir::ParserConfig(&_context));
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

// nl.unwind spells its iterator type too: the element chunk follows the source's element
// type - a list drains into the type-erased list_element chunk - and each carried chunk
// comes back after it, keeping the type it had.
TEST_F(NLDialectTest, unwindBuildsElementAndCarriedChunkIterator) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::ArrayAttr elements = builder.getArrayAttr({builder.getI64IntegerAttr(1),
                                                           builder.getI64IntegerAttr(2)});
    mlir::nl::Constant source = builder.create<mlir::nl::Constant>(loc, elements);
    mlir::nl::Constant carried = builder.create<mlir::nl::Constant>(loc, builder.getI64IntegerAttr(7));

    const mlir::Type elementChunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::ListElementType::get(&_context));
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context,
                                                                {elementChunkType, carried.getResult().getType()});

    mlir::nl::Unwind unwind = builder.create<mlir::nl::Unwind>(loc,
                                                               iteratorType,
                                                               source.getResult(),
                                                               mlir::ValueRange {carried.getResult()});
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_EQ(unwind.getResult().getType(), iteratorType);
    EXPECT_EQ(unwind.getColumnsToFilter().size(), 1u);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));

    // Printing then re-parsing yields a module that still verifies, so the nl.unwind
    // printer and parser are inverses.
    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed =
        mlir::parseSourceString<mlir::ModuleOp>(printed, mlir::ParserConfig(&_context));
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

// A list literal is an nl.constant value: it holds the whole list rather than spreading it
// over rows, so it produces a chunk of that list type - inferred from the elements, since an
// array attribute carries none of its own. A nested list rides one element as an array.
TEST_F(NLDialectTest, listConstantBuildsListChunk) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type int64Type = mlir::IntegerType::get(&_context, 64);
    const mlir::Type listType = mlir::storage::ListType::get(&_context, int64Type);
    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, listType);

    const mlir::ArrayAttr elements = builder.getArrayAttr({builder.getI64IntegerAttr(1),
                                                           builder.getI64IntegerAttr(2)});
    mlir::nl::Constant constant = builder.create<mlir::nl::Constant>(loc, elements);
    builder.create<mlir::func::ReturnOp>(loc);

    // The chunk is inferred from the elements, not given: these two agree on i64.
    EXPECT_EQ(constant.getResult().getType(), chunkType);
    EXPECT_EQ(mlir::cast<mlir::ArrayAttr>(constant.getValue()).size(), 2u);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// Building a list nl.constant, printing the module and re-parsing it yields a module that
// still verifies, so the printer and parser are inverses - and since the chunk is never
// printed, the round trip re-runs the inference too.
TEST_F(NLDialectTest, constListRoundTripsThroughTextualForm) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    const mlir::Type listElementType = mlir::storage::ListElementType::get(&_context);
    const mlir::Type listType = mlir::storage::ListType::get(&_context, listElementType);
    const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, listType);

    const mlir::ArrayAttr nested = builder.getArrayAttr({builder.getI64IntegerAttr(2),
                                                         builder.getI64IntegerAttr(3)});
    const mlir::ArrayAttr elements = builder.getArrayAttr({builder.getI64IntegerAttr(1), nested});
    mlir::nl::Constant constant = builder.create<mlir::nl::Constant>(loc, elements);

    // The nested element carries no type, so the verdict is the type-erased form.
    EXPECT_EQ(constant.getResult().getType(), chunkType);
    builder.create<mlir::func::ReturnOp>(loc);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed =
        mlir::parseSourceString<mlir::ModuleOp>(printed, mlir::ParserConfig(&_context));
    ASSERT_TRUE(reparsed);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));
}

// nl.get_out_edges_by_type infers the same four-chunk edge iterator as
// nl.get_out_edges - sources, edge IDs, edge type IDs, targets - since the edge
// type filters rows, not columns. The type is a resolved nl.get_edge_type handle,
// not a name on the op.
TEST_F(NLDialectTest, getOutEdgesByTypeInfersEdgeIterator) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value nodes = entryBlock.getArgument(0);

    mlir::nl::GetEdgeType handle = builder.create<mlir::nl::GetEdgeType>(loc, builder.getStringAttr("KNOWS"));
    mlir::nl::GetOutEdgesByType edges = builder.create<mlir::nl::GetOutEdgesByType>(loc,
                                                                                   nodes,
                                                                                   handle.getResult(),
                                                                                   mlir::ValueRange {});
    builder.create<mlir::func::ReturnOp>(loc);

    const mlir::Type nodeChunk = mlir::nl::ChunkType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type edgeIDChunk = mlir::nl::ChunkType::get(&_context, mlir::storage::EdgeIDType::get(&_context));
    const mlir::Type edgeTypeIDChunk = mlir::nl::ChunkType::get(&_context, mlir::storage::EdgeTypeIDType::get(&_context));
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {nodeChunk, edgeIDChunk, edgeTypeIDChunk, nodeChunk});

    EXPECT_EQ(edges.getResult().getType(), iteratorType);
    // The hop's edge_type operand is the get_edge_type handle, which carries the name.
    EXPECT_EQ(edges.getEdgeType(), handle.getResult());
    EXPECT_EQ(handle.getName(), "KNOWS");
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// nl.scan_edges infers a four-chunk edge iterator - source node IDs, edge IDs,
// edge type IDs and target node IDs - the same chunk shape nl.get_out_edges
// produces, since a full edge scan opens the dataflow with no input or carry set.
TEST_F(NLDialectTest, scanEdgesInfersEdgeIterator) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());

    mlir::nl::ScanEdges scan = builder.create<mlir::nl::ScanEdges>(loc);
    builder.create<mlir::func::ReturnOp>(loc);

    const mlir::Type nodeChunk = mlir::nl::ChunkType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type edgeChunk = mlir::nl::ChunkType::get(&_context, mlir::storage::EdgeIDType::get(&_context));
    const mlir::Type edgeTypeChunk = mlir::nl::ChunkType::get(&_context, mlir::storage::EdgeTypeIDType::get(&_context));
    const mlir::Type iteratorType = mlir::nl::IteratorType::get(&_context, {nodeChunk, edgeChunk, edgeTypeChunk, nodeChunk});

    EXPECT_EQ(scan.getResult().getType(), iteratorType);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// nl.set_node_property takes a node-ID chunk, a property name and a value chunk,
// has no result, and verifies with a non-empty property name.
TEST_F(NLDialectTest, verifierAcceptsSetNodeProperty) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);

    const mlir::Type nodeChunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type valueChunkType = mlir::nl::ChunkType::get(&_context, builder.getI64Type());

    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {nodeChunkType, valueChunkType}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value nodeChunk = entryBlock.getArgument(0);
    const mlir::Value valueChunk = entryBlock.getArgument(1);

    mlir::nl::SetNodeProperty setNode = builder.create<mlir::nl::SetNodeProperty>(loc, nodeChunk, builder.getStringAttr("age"), valueChunk, mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(setNode.getProperty(), "age");
    EXPECT_EQ(setNode.getInputNodes().getType(), nodeChunkType);
    EXPECT_EQ(setNode.getValue().getType(), valueChunkType);
    EXPECT_EQ(setNode->getNumResults(), 0U);
}

// The edge counterpart: an edge-ID chunk, a property name and a value chunk.
TEST_F(NLDialectTest, verifierAcceptsSetEdgeProperty) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);

    const mlir::Type edgeChunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::EdgeIDType::get(&_context));
    const mlir::Type valueChunkType = mlir::nl::ChunkType::get(&_context, builder.getF64Type());

    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {edgeChunkType, valueChunkType}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value edgeChunk = entryBlock.getArgument(0);
    const mlir::Value valueChunk = entryBlock.getArgument(1);

    mlir::nl::SetEdgeProperty setEdge = builder.create<mlir::nl::SetEdgeProperty>(loc, edgeChunk, builder.getStringAttr("weight"), valueChunk, mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(setEdge.getProperty(), "weight");
    EXPECT_EQ(setEdge.getInputEdges().getType(), edgeChunkType);
    EXPECT_EQ(setEdge.getValue().getType(), valueChunkType);
    EXPECT_EQ(setEdge->getNumResults(), 0u);
}

// An empty property name is malformed, so the nl.set_node_property verifier fails.
TEST_F(NLDialectTest, verifierRejectsEmptySetProperty) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);

    const mlir::Type nodeChunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::NodeIDType::get(&_context));
    const mlir::Type valueChunkType = mlir::nl::ChunkType::get(&_context, builder.getI64Type());

    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {nodeChunkType, valueChunkType}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value nodeChunk = entryBlock.getArgument(0);
    const mlir::Value valueChunk = entryBlock.getArgument(1);

    builder.create<mlir::nl::SetNodeProperty>(loc, nodeChunk, builder.getStringAttr(""), valueChunk, mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    // The diagnostics are swallowed so the deliberate verifier failure does not
    // print to the test log.
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(function)));
}

// nl.delete_node takes a node-ID chunk and an optional detach flag, has no result,
// and verifies. With `detach` set it carries the DETACH semantics.
TEST_F(NLDialectTest, verifierAcceptsDeleteNodeDetach) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    mlir::nl::DeleteNode deleteNode = builder.create<mlir::nl::DeleteNode>(loc, chunk, /*detach=*/true, mlir::Value {});
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_TRUE(deleteNode.getDetach());
    EXPECT_EQ(deleteNode.getInputNodes().getType(), chunk.getType());
    EXPECT_EQ(deleteNode->getNumResults(), 0u);
}

// Without the detach flag the unit attribute is absent.
TEST_F(NLDialectTest, verifierAcceptsDeleteNodeNoDetach) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    mlir::nl::DeleteNode deleteNode = builder.create<mlir::nl::DeleteNode>(loc, chunk, /*detach=*/false, mlir::Value {});
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_FALSE(deleteNode.getDetach());
}

// The edge counterpart: an edge-ID chunk, no detach, no result.
TEST_F(NLDialectTest, verifierAcceptsDeleteEdge) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);

    const mlir::Type edgeChunkType = mlir::nl::ChunkType::get(&_context, mlir::storage::EdgeIDType::get(&_context));
    builder.setInsertionPointToEnd(module->getBody());
    auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {edgeChunkType}, {}));
    builder.setInsertionPointToStart(function.addEntryBlock());
    const mlir::Value edgeChunk = function.getBody().front().getArgument(0);

    mlir::nl::DeleteEdge deleteEdge = builder.create<mlir::nl::DeleteEdge>(loc, edgeChunk, mlir::Value {});
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(deleteEdge.getInputEdges().getType(), edgeChunkType);
    EXPECT_EQ(deleteEdge->getNumResults(), 0u);
}

// db.collect bounds its key count against the columns it is given before summing it with
// the aggregates, and rejects a distinct index naming no collected column. The nl sibling
// carries the split across two ops, so the update is where the same guarantees are made.
TEST_F(NLDialectTest, verifierAcceptsCollectUpdateSplitAcrossKeysAndValues) {
    EXPECT_TRUE(parses(collectsOneValueUnderOneKey));
}

TEST_F(NLDialectTest, verifierRejectsCollectUpdateWithMoreKeysThanColumns) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(collectsUnderMoreKeysThanColumns));
}

TEST_F(NLDialectTest, verifierRejectsCollectUpdateWithADistinctIndexPastTheValues) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(collectsDistinctPastTheValues));
}

// A property-value scan needs a literal one of the stored types can hold. The db op
// verifies that; a hand-written .nl.mlir skips the db dialect entirely, so the nl op
// verifies it too rather than scanning nothing for a needle no value can equal.
constexpr const char* scansAPropertyValueAgainstAnInteger = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes_by_property_value("age", 32 : i64, ["Person"])
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%node) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
)mlir";

constexpr const char* scansAPropertyValueAgainstAnI32 = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes_by_property_value("age", 32 : i32)
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%node) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
)mlir";

constexpr const char* scansAnUnnamedProperty = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes_by_property_value("", 32 : i64)
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%node) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
)mlir";

constexpr const char* scansAPropertyValueUnderNoLabel = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes_by_property_value("age", 32 : i64, [])
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%node) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
)mlir";

// The shapes db.vector_search rejects reach faiss through the nl sibling otherwise: a
// search reporting no neighbour sizes its output buffers at zero, and a query vector of
// no dimension has nothing to score against.
TEST_F(NLDialectTest, verifierAcceptsVectorSearchForSeveralNeighbours) {
    EXPECT_TRUE(parses(searchesForThreeNeighbours));
}

TEST_F(NLDialectTest, verifierRejectsVectorSearchForNoNeighbour) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(searchesForNoNeighbour));
}

TEST_F(NLDialectTest, verifierRejectsVectorSearchForAVectorOfNoDimension) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(searchesForAVectorOfNoDimension));
}

TEST_F(NLDialectTest, verifierAcceptsAPropertyValueScan) {
    EXPECT_TRUE(parses(scansAPropertyValueAgainstAnInteger));
}

TEST_F(NLDialectTest, verifierRejectsAPropertyValueScanOnAnUnstorableLiteral) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(scansAPropertyValueAgainstAnI32));
}

TEST_F(NLDialectTest, verifierRejectsAPropertyValueScanWithoutAPropertyName) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(scansAnUnnamedProperty));
}

TEST_F(NLDialectTest, verifierRejectsAPropertyValueScanUnderAnEmptyLabelList) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(scansAPropertyValueUnderNoLabel));
}

}
