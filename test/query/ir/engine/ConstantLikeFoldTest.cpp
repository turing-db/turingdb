#include <gtest/gtest.h>

#include <stddef.h>

#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Matchers.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Rewrite/FrozenRewritePatternSet.h"
#include "mlir/Transforms/GreedyPatternRewriteDriver.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "NLDialect.h"
#include "NLOps.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

// MLIR reads the value of an op marked ConstantLike by folding it, so every such op folds to
// the literal it holds: m_Constant binds it, and the greedy driver uniques constants by it
class ConstantLikeFoldTest : public ::testing::Test {
protected:
    ConstantLikeFoldTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(std::string_view programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    template <typename ConstantOp>
    void expectEveryConstantBindsItsValue(std::string_view programText, size_t constantCount) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);

        llvm::SmallVector<ConstantOp> constants = collect<ConstantOp>(*module);
        ASSERT_EQ(constants.size(), constantCount);

        for (ConstantOp constant : constants) {
            mlir::Attribute bound;
            EXPECT_TRUE(mlir::matchPattern(constant.getResult(), mlir::m_Constant(&bound)));
            EXPECT_EQ(bound, constant.getValue());
        }
    }

    mlir::MLIRContext _context;
};

const char* const nlConstants = R"mlir(
func.func @main() -> (!nl.chunk<i64>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.string>) {
  %number = nl.constant(30 : i64)
  %cell = nl.constant("t" : !storage.string) : !nl.chunk<!storage.any>
  %null = nl.constant(unit) : !nl.chunk<!storage.any>
  %text = nl.constant("t" : !storage.string)
  return %number, %cell, %null, %text : !nl.chunk<i64>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.string>
}
)mlir";

const char* const dbConstants = R"mlir(
func.func @main() -> (!db.column<i64>, !db.column<!storage.any>, !db.column<!storage.any>) {
  %number = db.constant(30 : i64)
  %cell = db.constant("t" : !storage.string) : !db.column<!storage.any>
  %null = db.constant(unit) : !db.column<!storage.any>
  return %number, %cell, %null : !db.column<i64>, !db.column<!storage.any>, !db.column<!storage.any>
}
)mlir";

const char* const twoEqualNLConstants = R"mlir(
func.func @main() -> (!nl.chunk<i64>, !nl.chunk<i64>) {
  %first = nl.constant(30 : i64)
  %second = nl.constant(30 : i64)
  return %first, %second : !nl.chunk<i64>, !nl.chunk<i64>
}
)mlir";

TEST_F(ConstantLikeFoldTest, bindsTheValueOfEveryNLConstant) {
    expectEveryConstantBindsItsValue<mlir::nl::Constant>(nlConstants, 4);
}

TEST_F(ConstantLikeFoldTest, bindsTheValueOfEveryDBConstant) {
    expectEveryConstantBindsItsValue<mlir::db::ConstantOp>(dbConstants, 3);
}

TEST_F(ConstantLikeFoldTest, uniquesEqualNLConstantsUnderTheGreedyDriver) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoEqualNLConstants);
    ASSERT_TRUE(module);

    const mlir::FrozenRewritePatternSet noPatterns;
    ASSERT_TRUE(mlir::succeeded(mlir::applyPatternsGreedily(module->getOperation(), noPatterns)));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::nl::Constant> constants = collect<mlir::nl::Constant>(*module);
    ASSERT_EQ(constants.size(), 1u);

    const llvm::SmallVector<mlir::func::ReturnOp> returns = collect<mlir::func::ReturnOp>(*module);
    ASSERT_EQ(returns.size(), 1u);

    mlir::func::ReturnOp returned = returns.front();
    const mlir::Value kept = constants.front().getResult();
    EXPECT_EQ(returned.getOperand(0), kept);
    EXPECT_EQ(returned.getOperand(1), kept);
}
