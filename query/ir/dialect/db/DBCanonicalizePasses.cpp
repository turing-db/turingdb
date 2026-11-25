#include "DBPasses.h"

#include <stdint.h>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Transforms/GreedyPatternRewriteDriver.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "DBColumnElement.h"
#include "BoundEntityIDs.h"
#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"

namespace mlir::db {

#define GEN_PASS_DEF_CANONICALIZECOALESCE
#define GEN_PASS_DEF_CANONICALIZEIDEQUALITIES
#define GEN_PASS_DEF_HOISTUNCONDITIONALREGIONS
#include "DBPasses.h.inc"

namespace {

// Whether a row the predicate holds null on is dropped as one it holds false on: it is the
// mask of a filter, or what an AND or an OR combines into one
bool isTestedAsAFilter(Value predicate) {
    llvm::SmallVector<Value> worklist {predicate};
    llvm::DenseSet<Value> visited {predicate};

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();

        for (OpOperand& use : current.getUses()) {
            Operation* const user = use.getOwner();
            FilterOp filter = dyn_cast<FilterOp>(user);

            if (filter && &use == &filter.getMaskMutable()) {
                continue;
            } else if (!isa<AndOp, OrOp>(user)) {
                return false;
            }

            const Value combined = user->getResult(0);
            if (visited.insert(combined).second) {
                worklist.push_back(combined);
            }
        }
    }

    return true;
}

// id(e) = value over a node or edge column, which holds where e = entity_id_of(value) does.
// Where the value names no entity, the first is false and the second null, so it is rewritten
// only where a filter drops both
struct IDComparison {
    EqOp _equality;
    ElementID _id;
    Value _value;
};

bool matchIDComparison(EqOp equality, IDComparison& comparison) {
    for (unsigned operand = 0; operand < 2; operand++) {
        ElementID id = equality->getOperand(operand).getDefiningOp<ElementID>();
        if (!id || !isEntityColumn(id.getInput())) {
            continue;
        }

        const Value value = equality->getOperand(1 - operand);
        if (!storage::isEntityIDOfInputType(::db::declaredColumnElement(value))) {
            return false;
        }

        comparison = IDComparison {._equality = equality, ._id = id, ._value = value};

        return isTestedAsAFilter(equality.getResult());
    }

    return false;
}

void canonicalizeIDComparison(const IDComparison& comparison, mlir::OpBuilder& builder) {
    EqOp equality = comparison._equality;
    ElementID id = comparison._id;
    const Value entities = id.getInput();
    const Location loc = equality.getLoc();

    builder.setInsertionPoint(equality);

    ConstantOp literal = comparison._value.getDefiningOp<ConstantOp>();
    int64_t entityID = 0;

    Value entityIDs;
    if (literal && entityIDOfLiteral(literal.getValue(), entityID)) {
        entityIDs = builder.create<ConstantOp>(loc, builder.getI64IntegerAttr(entityID)).getResult();
    } else {
        const Type entityColumnType = ColumnType::get(builder.getContext(), entityElementOf(entities));
        entityIDs = builder.createOrFold<EntityIDOf>(loc, entityColumnType, comparison._value);
    }

    equality->setOperands({entities, entityIDs});

    eraseIfUnused(id);
    if (Operation* const valueOp = comparison._value.getDefiningOp()) {
        eraseIfUnused(valueOp);
    }
}

struct CanonicalizeIDEqualities : public impl::CanonicalizeIDEqualitiesBase<CanonicalizeIDEqualities> {
    void runOnOperation() override {
        llvm::SmallVector<IDComparison> comparisons;
        getOperation()->walk([&comparisons](EqOp equality) {
            IDComparison comparison;
            if (matchIDComparison(equality, comparison)) {
                comparisons.push_back(comparison);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (const IDComparison& comparison : comparisons) {
            canonicalizeIDComparison(comparison, builder);
        }
    }
};

struct CanonicalizeCoalesce : public impl::CanonicalizeCoalesceBase<CanonicalizeCoalesce> {
    void runOnOperation() override {
        MLIRContext* const context = &getContext();

        llvm::SmallVector<Operation*> coalesces;
        getOperation()->walk([&coalesces](Coalesce coalesce) {
            coalesces.push_back(coalesce.getOperation());
        });

        mlir::RewritePatternSet patterns(context);
        Coalesce::getCanonicalizationPatterns(patterns, context);
        const mlir::FrozenRewritePatternSet frozenPatterns(std::move(patterns));

        // Kept to the coalesces: the driver's folding and region simplification would
        // otherwise rewrite the rest of the program as well, and its dead-op removal would
        // erase an op an inlined argument held, a Pure CASE holding a division included
        mlir::GreedyRewriteConfig config;
        config.setStrictness(mlir::GreedyRewriteStrictness::ExistingOps);
        config.enableFolding(false);
        config.enableConstantCSE(false);
        config.setRegionSimplificationLevel(mlir::GreedySimplifyRegionLevel::Disabled);

        if (failed(mlir::applyOpPatternsGreedily(coalesces, frozenPatterns, config))) {
            signalPassFailure();
        }
    }
};

// A CASE's first condition and a coalesce's first argument see every row the op receives;
// each later region sees only the rows the regions before it passed on.
Region& unconditionalRegion(Operation* selection) {
    return selection->getRegion(0);
}

bool computesOverCarriedColumns(Region& region) {
    Block& body = region.front();
    Operation* const yielding = body.getTerminator()->getOperand(0).getDefiningOp();

    const bool computedInTheRegion = yielding && yielding->getBlock() == &body;
    const bool readsACarriedColumn = llvm::any_of(body.getArguments(), [](BlockArgument argument) {
        return !argument.use_empty();
    });

    return computedInTheRegion && readsACarriedColumn;
}

// The columns a region hoisted in front of its op forwards are computed there over every row,
// so the regions after it take them as carried columns instead
void carryForwardedColumns(Region& region, mlir::RewriterBase& rewriter) {
    Operation* const op = region.getParentOp();
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);
    if (!carrier.forwardsColumns(region)) {
        return;
    }

    MutableOperandRange forwarded = carrier.getForwardedColumnsMutable(region);
    const OperandRange current = forwarded;
    const llvm::SmallVector<Value> columns(current.begin(), current.end());
    if (columns.empty()) {
        return;
    }

    const size_t forwardOffset = forwardedArgumentCount(region);
    const size_t regionCount = op->getNumRegions();

    for (size_t forwardIndex = 0; forwardIndex < columns.size(); forwardIndex++) {
        bool appended = false;
        const size_t carriedIndex = carryIntoRegions(op, columns[forwardIndex], rewriter, appended);

        for (size_t laterIndex = region.getRegionNumber() + 1; laterIndex < regionCount; laterIndex++) {
            Region& later = op->getRegion(static_cast<unsigned>(laterIndex));
            const BlockArgument forwardedArgument = later.getArgument(static_cast<unsigned>(forwardOffset + forwardIndex));
            rewriter.replaceAllUsesWith(forwardedArgument, carriedArgument(later, carriedIndex));
        }
    }

    rewriter.modifyOpInPlace(region.front().getTerminator(), [&forwarded] {
        forwarded.clear();
    });

    for (size_t laterIndex = region.getRegionNumber() + 1; laterIndex < regionCount; laterIndex++) {
        Block& later = op->getRegion(static_cast<unsigned>(laterIndex)).front();
        later.eraseArguments(static_cast<unsigned>(forwardOffset), static_cast<unsigned>(columns.size()));
    }
}

void hoistUnconditionalRegion(Operation* selection, mlir::RewriterBase& rewriter) {
    Region& region = unconditionalRegion(selection);
    Block& body = region.front();
    Operation* const yield = body.getTerminator();

    for (Operation& op : llvm::make_early_inc_range(body.without_terminator())) {
        rewriter.moveOpBefore(&op, selection);
    }

    for (const BlockArgument argument : body.getArguments()) {
        rewriter.replaceAllUsesWith(argument, carriedColumnOf(argument));
    }

    carryForwardedColumns(region, rewriter);

    const Value yielded = yield->getOperand(0);
    if (::db::yieldsConstantColumn(yielded)) {
        return;
    }

    const BlockArgument value = carryIntoRegion(region, yielded, rewriter);
    rewriter.modifyOpInPlace(yield, [yield, value] {
        yield->setOperand(0, value);
    });
}

struct HoistUnconditionalRegions : public impl::HoistUnconditionalRegionsBase<HoistUnconditionalRegions> {
    void runOnOperation() override {
        llvm::SmallVector<Operation*> selections;
        getOperation()->walk([&selections](Operation* op) {
            if (isa<LazyCase, Coalesce>(op)) {
                selections.push_back(op);
            }
        });

        mlir::IRRewriter rewriter(&getContext());
        for (Operation* const selection : selections) {
            if (computesOverCarriedColumns(unconditionalRegion(selection))) {
                hoistUnconditionalRegion(selection, rewriter);
            }
        }
    }
};

}

}
