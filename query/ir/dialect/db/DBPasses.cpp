#include "DBPasses.h"

#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/Value.h"
#include "mlir/Transforms/GreedyPatternRewriteDriver.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBTypes.h"

using namespace mlir::db;

namespace mlir::db {
#define GEN_PASS_DEF_GETOUTEDGESDCE
#define GEN_PASS_DEF_PREDICATEPUSHDOWN
#include "DBPasses.h.inc"
}

namespace {

constexpr unsigned fixedResultCount = 4;

struct GetOutEdgesDCE : public impl::GetOutEdgesDCEBase<GetOutEdgesDCE> {
    void runOnOperation() override {
        mlir::Operation* const root = getOperation();
        mlir::MLIRContext* const context = root->getContext();
        const NullptrType nullType = NullptrType::get(context);

        root->walk([&](GetOutEdges op) {
            for (unsigned index = 0; index < fixedResultCount; index++) {
                mlir::OpResult result = op->getResult(index);
                if (!result.use_empty()) {
                    continue;
                }

                result.setType(nullType);
            }
        });
    }
};

mlir::Operation* soleProducer(mlir::Operation::operand_range columns) {
    if (columns.empty()) {
        return nullptr;
    }

    mlir::Operation* const producer = columns.front().getDefiningOp();
    for (const mlir::Value column : columns) {
        if (column.getDefiningOp() != producer) {
            return nullptr;
        }
    }

    return producer;
}

struct SinkFilter : public mlir::OpRewritePattern<FilterOp> {
    using OpRewritePattern<FilterOp>::OpRewritePattern;

    llvm::LogicalResult matchAndRewrite(FilterOp filter, mlir::PatternRewriter& rewriter) const override {
        mlir::Operation* soleProd = soleProducer(filter.getColumnsToFilter());
        auto producer = dyn_cast_or_null<AbsorbFilterInterface>(soleProd);
        // not all inputs come from the same producer, or op doesn't implement interface
        if (!producer) {
            return llvm::failure();
        }

        return producer.absorbFilter(filter, rewriter);
    }
};

struct PredicatePushDown final : public impl::PredicatePushdownBase<PredicatePushDown> {
    void runOnOperation() final {
        mlir::MLIRContext* const context = getOperation()->getContext();

        mlir::RewritePatternSet patterns(context);
        patterns.add<SinkFilter>(context);

        mlir::GreedyRewriteConfig config;
        config.enableFolding(false).enableConstantCSE(false);

        if (failed(applyPatternsGreedily(getOperation(), std::move(patterns), config))) {
            signalPassFailure();
        }
    }
};

}
