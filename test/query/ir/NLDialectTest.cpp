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

namespace {

// A context with just the dialects an nl-dialect program needs: nl for the
// nested-loop ops and func for the enclosing function.
class NLDialectTest : public ::testing::Test {
protected:
    NLDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    // A function whose entry block takes one node-ID chunk argument - the chunk
    // an nl.output emits and an nl.limit_update charges. Leaves the builder at the
    // start of that block, ready to append the ops under test.
    mlir::func::FuncOp buildOneChunkFunction(mlir::OpBuilder& builder, mlir::ModuleOp module) {
        const mlir::Location loc = builder.getUnknownLoc();
        const mlir::Type chunkType = mlir::nl::ChunkType::get(&_context, mlir::nl::NodeIDType::get(&_context));

        builder.setInsertionPointToEnd(module.getBody());
        auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {chunkType}, {}));
        builder.setInsertionPointToStart(function.addEntryBlock());

        return function;
    }

    mlir::MLIRContext _context;
};

// An nl.output that names a limit handle but has no preceding nl.limit_update in
// its block would read a stale or zero count, so the verifier rejects it.
TEST_F(NLDialectTest, verifierRejectsLimitedOutputWithoutUpdate) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Limit>(loc, /*count=*/3u).getState();
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, handle);
    builder.create<mlir::func::ReturnOp>(loc);

    const mlir::ScopedDiagnosticHandler diagnostics(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });
    EXPECT_TRUE(mlir::failed(mlir::verify(function)));
}

// The same output verifies once an nl.limit_update for the same handle runs
// before it in the block - the pairing DBLowering always emits.
TEST_F(NLDialectTest, verifierAcceptsLimitedOutputAfterUpdate) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    const mlir::Value handle = builder.create<mlir::nl::Limit>(loc, /*count=*/3u).getState();
    builder.create<mlir::nl::LimitUpdate>(loc, handle, chunk);
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, handle);
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

// An nl.output with no limit handle is unbounded and needs no update before it.
TEST_F(NLDialectTest, verifierAcceptsUnlimitedOutput) {
    mlir::OpBuilder builder(&_context);
    const mlir::Location loc = builder.getUnknownLoc();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
    mlir::func::FuncOp function = buildOneChunkFunction(builder, *module);
    mlir::Block& entryBlock = function.getBody().front();
    const mlir::Value chunk = entryBlock.getArgument(0);

    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {chunk}, mlir::Value());
    builder.create<mlir::func::ReturnOp>(loc);

    EXPECT_TRUE(mlir::succeeded(mlir::verify(function)));
}

}
