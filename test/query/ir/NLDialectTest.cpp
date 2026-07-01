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

}
