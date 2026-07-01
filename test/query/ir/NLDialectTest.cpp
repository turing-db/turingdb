#include <gtest/gtest.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"

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

    mlir::MLIRContext _context;
};

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
    builder.create<mlir::nl::Output>(loc, truncate.getResults(), mlir::Value(), mlir::Value());
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
    builder.create<mlir::nl::Output>(loc, truncate.getResults(), mlir::Value(), mlir::Value());
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

    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, mlir::Value(), mlir::Value());
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
    mlir::nl::Output output = builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, handle, mlir::Value());
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
    mlir::nl::Output output = builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, mlir::Value(), handle);
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
    builder.create<mlir::nl::Output>(loc, filter.getResults(), mlir::Value(), mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
    EXPECT_EQ(filter.getState(), handle);
    EXPECT_EQ(filter.getResult(0).getType(), chunk.getType());
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
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, limitHandle, skipHandle);
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

}
