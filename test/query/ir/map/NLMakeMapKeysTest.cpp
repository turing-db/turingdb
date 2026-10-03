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

// The keys of one map name its entries, so two entries cannot share a key: a map built
// with the same key twice has two values under one name, and every reader of it - JSON,
// the binary protocol, a Python dict - answers that differently.
class NLMakeMapKeysTest : public ::testing::Test {
protected:
    NLMakeMapKeysTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    // Builds `nl.make_map` over one value chunk per key and reports whether it verified
    bool verifiesWithKeys(mlir::OpBuilder& builder, llvm::ArrayRef<llvm::StringRef> keys) {
        const mlir::Location loc = builder.getUnknownLoc();

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(loc);
        builder.setInsertionPointToEnd(module->getBody());
        auto function = builder.create<mlir::func::FuncOp>(loc, "main", mlir::FunctionType::get(&_context, {}, {}));
        builder.setInsertionPointToStart(function.addEntryBlock());

        llvm::SmallVector<mlir::Value> values;
        for (size_t index = 0; index < keys.size(); index++) {
            mlir::nl::Constant value = builder.create<mlir::nl::Constant>(loc,
                                                                          builder.getI64IntegerAttr(static_cast<int64_t>(index)));
            values.push_back(value.getResult());
        }

        builder.create<mlir::nl::MakeMap>(loc, values, builder.getStrArrayAttr(keys));
        builder.create<mlir::func::ReturnOp>(loc);

        // Swallow the verifier's diagnostic so a deliberate failure does not print.
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::succeeded(mlir::verify(function));
    }

    mlir::MLIRContext _context;
};

TEST_F(NLMakeMapKeysTest, verifierAcceptsDistinctKeys) {
    mlir::OpBuilder builder(&_context);

    EXPECT_TRUE(verifiesWithKeys(builder, {"age", "name"}));
}

TEST_F(NLMakeMapKeysTest, verifierRejectsDuplicateKeys) {
    mlir::OpBuilder builder(&_context);

    EXPECT_FALSE(verifiesWithKeys(builder, {"name", "name"}));
}

TEST_F(NLMakeMapKeysTest, verifierRejectsADuplicateAmongDistinctKeys) {
    mlir::OpBuilder builder(&_context);

    EXPECT_FALSE(verifiesWithKeys(builder, {"age", "name", "age"}));
}

}
