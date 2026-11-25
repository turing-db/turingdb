#include "DBPasses.h"

#include <deque>
#include <memory>
#include <optional>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"
#include "DBCrossProducts.h"
#include "DBFilterUtils.h"
#include "DBLineageClimbs.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBRowCone.h"
#include "DBRowEstimator.h"
#include "DBStagePlanner.h"
#include "DBWrittenOrder.h"

namespace mlir::db {

#define GEN_PASS_DEF_PUSHDOWNFILTERS
#define GEN_PASS_DEF_MERGEFILTERS
#define GEN_PASS_DEF_STAGEFILTERCONJUNCTS
#include "DBPasses.h.inc"

namespace {

// A filter whose mask reads the columns one op makes, such as an edge and its type, and where
// its mask should be rebuilt: right after that op, over the anchor of each input of the cone,
// in the order of the inputs.
struct PushablePredicate {
    llvm::SmallVector<Value, 2> _anchors;
    MaskCone _cone;
};

bool madeByOneOp(llvm::ArrayRef<Value> anchors) {
    Operation* const producer = anchors.front().getDefiningOp();

    return llvm::all_of(anchors, [producer](Value anchor) {
        return anchor.getDefiningOp() == producer;
    });
}

// The anchor a predicate can be rebuilt on instead of @param anchor: the equalities the climb
// crossed hold whole classes of columns to the same node, and where one of the class was
// scanned for, the predicate reads that node off the scan. Applying it there is what spares
// the walk between the two. The class is closed over the equalities rather than read a pair
// at a time, so a column held to the scan through an intermediate is still found.
Value scannedAnchorEqualTo(Value anchor, llvm::ArrayRef<ColumnEquality> anchorEqualities) {
    if (!anchor || isNodeSource(anchor.getDefiningOp())) {
        return {};
    }

    llvm::SmallVector<Value, 4> anchorClass {anchor};
    llvm::SmallPtrSet<void*, 4> seen {anchor.getAsOpaquePointer()};

    for (size_t index = 0; index < anchorClass.size(); index++) {
        for (const ColumnEquality& equality : anchorEqualities) {
            const Value lhs = equality.first;
            const Value rhs = equality.second;
            const Value held = lhs == anchorClass[index] ? rhs : (rhs == anchorClass[index] ? lhs : Value {});
            if (!held || !seen.insert(held.getAsOpaquePointer()).second) {
                continue;
            }

            if (isNodeSource(held.getDefiningOp())) {
                return held;
            }

            anchorClass.push_back(held);
        }
    }

    return {};
}

bool matchPushablePredicate(FilterOp filter,
                            const DBWrittenOrder& order,
                            LineageClimbs& climbs,
                            PushablePredicate& pushable) {
    Operation* const maskDef = filter.getMask().getDefiningOp();
    if (!maskDef || !computesFromTheRow(maskDef)) {
        return false;
    }

    pushable._cone = collectMaskCone(filter.getMask());
    if (pushable._cone._inputs.empty()) {
        return false;
    }

    bool crossedRowOp = false;
    llvm::SmallVector<ColumnEquality, 4> anchorEqualities;
    for (const Value input : pushable._cone._inputs) {
        const Value inputAnchor = climbs.climb(input, crossedRowOp, anchorEqualities);
        if (!inputAnchor) {
            return false;
        }

        pushable._anchors.push_back(inputAnchor);
    }

    if (!madeByOneOp(pushable._anchors)) {
        return false;
    }

    llvm::SmallVector<Value, 2> equatedAnchors;
    for (const Value anchor : pushable._anchors) {
        const Value equatedAnchor = scannedAnchorEqualTo(anchor, anchorEqualities);
        if (!equatedAnchor) {
            break;
        }

        equatedAnchors.push_back(equatedAnchor);
    }

    const bool scansEveryAnchor = equatedAnchors.size() == pushable._anchors.size() && madeByOneOp(equatedAnchors);
    if (scansEveryAnchor) {
        pushable._anchors = equatedAnchors;
        crossedRowOp = true;
    }

    // Crossing only filters leaves the predicate over the rows the anchor produced already:
    // moving it would swap two filters over the same rows, or step between a constraint
    // filter and the op that is about to absorb it.
    if (!crossedRowOp) {
        return false;
    }

    // A filter pushed to just after the anchor's producer cuts the rows of every op between the
    // two, so a write among them would no longer run on the rows the filter drops. A write the
    // producer holds has run on every row by then.
    return !order.writesBetween(pushable._anchors.front().getDefiningOp(), filter.getOperation());
}

void pushDownPredicate(FilterOp filter, const PushablePredicate& pushable, mlir::OpBuilder& builder) {
    const MaskCone& cone = pushable._cone;
    const mlir::Location loc = filter.getLoc();

    Operation* const anchorProducer = pushable._anchors.front().getDefiningOp();

    llvm::SmallVector<mlir::Value> liveColumns;
    for (const mlir::Value result : anchorProducer->getResults()) {
        if (!result.use_empty()) {
            liveColumns.push_back(result);
        }
    }

    // Rebuild the mask over the anchor version of each column, right after they are bound.
    mlir::IRMapping mapping;
    for (const auto [input, anchor] : llvm::zip_equal(cone._inputs, pushable._anchors)) {
        mapping.map(input, anchor);
    }

    builder.setInsertionPointAfter(anchorProducer);

    llvm::SmallPtrSet<Operation*, 8> boundaryReaders;
    for (Operation* const coneOp : cone._ops) {
        Operation* const cloned = builder.clone(*coneOp, mapping);
        boundaryReaders.insert(cloned);
        builder.setInsertionPointAfter(cloned);
    }

    const Value clonedMask = mapping.lookup(filter.getMask());

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (const mlir::Value column : liveColumns) {
        resultTypes.push_back(column.getType());
    }

    FilterOp pushed = builder.create<FilterOp>(loc, resultTypes, clonedMask, liveColumns);
    boundaryReaders.insert(pushed.getOperation());

    // Replace live columns with filtered versions
    for (size_t i {0}; mlir::Value liveCol : liveColumns) {
        liveCol.replaceAllUsesExcept(pushed.getResult(i++), boundaryReaders);
    }

    // Remove original filter
    bypassFilter(filter);
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
}

struct PushDownFilters : public impl::PushDownFiltersBase<PushDownFilters> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<FilterOp> filters;
        getOperation()->walk([&filters](FilterOp filter) {
            filters.push_back(filter);
        });

        DBWrittenOrder order;
        order.build(getOperation());

        LineageClimbs climbs;
        for (FilterOp filter : filters) {
            PushablePredicate pushable;
            if (!matchPushablePredicate(filter, order, climbs, pushable)) {
                continue;
            }

            climbs.forget(filter.getOperation());
            for (Operation* const coneOp : pushable._cone._ops) {
                climbs.forget(coneOp);
            }

            pushDownPredicate(filter, pushable, builder);
        }
    }
};

// A filter over the rows another filter keeps, and nothing else reads. The two masks hold over
// the rows of the op below both, so a conjunct of the upper one joins the lower mask, unless it
// may raise: there it would run on the rows the lower one removes, and it stays above.
struct StackedFilters {
    FilterOp _upper;
    FilterOp _lower;
    llvm::SmallPtrSet<Operation*, 8> _between;
    llvm::SmallVector<Value, 4> _merged;
    llvm::SmallVector<Value, 2> _kept;
};

// Whether @param conjunct can be computed ahead of the lower filter instead: every op computing
// it @param between the two filters is a row computation that raises on no row, and an `and`
// joining it to @param lowerMask raises on no row either
bool computesAheadOf(Value conjunct, Value lowerMask, const llvm::SmallPtrSetImpl<Operation*>& between) {
    if (!AndOp::isSpeculatableOver(lowerMask, conjunct)) {
        return false;
    }

    llvm::SmallVector<Value> pending {conjunct};
    llvm::SmallPtrSet<Operation*, 8> visited;
    while (!pending.empty()) {
        Operation* const def = pending.pop_back_val().getDefiningOp();
        const bool boundAhead = !def || !between.contains(def);

        if (boundAhead || !visited.insert(def).second) {
            continue;
        } else if (!computesFromTheRow(def) || raisesOnSomeRows(def)) {
            return false;
        }

        llvm::append_range(pending, def->getOperands());
    }

    return true;
}

bool matchStackedFilters(FilterOp upper, StackedFilters& stacked) {
    FilterOp lower = dyn_cast_or_null<FilterOp>(filteredSource(upper));
    if (!lower || lower->getBlock() != upper->getBlock() || !readsOnlyIntoTheFilter(lower, upper)) {
        return false;
    }

    Operation* const upperOp = upper.getOperation();
    for (Operation* op = lower->getNextNode(); op != upperOp; op = op->getNextNode()) {
        stacked._between.insert(op);
    }

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(upper.getMask(), conjuncts);

    const Value lowerMask = lower.getMask();
    for (const Value conjunct : conjuncts) {
        if (computesAheadOf(conjunct, lowerMask, stacked._between)) {
            stacked._merged.push_back(conjunct);
        } else {
            stacked._kept.push_back(conjunct);
        }
    }

    stacked._upper = upper;
    stacked._lower = lower;

    return !stacked._merged.empty();
}

void mergeStackedFilters(StackedFilters& stacked, mlir::RewriterBase& rewriter) {
    FilterOp upper = stacked._upper;
    FilterOp lower = stacked._lower;
    const llvm::SmallPtrSetImpl<Operation*>& between = stacked._between;
    const Location loc = upper.getLoc();

    const auto computedBetween = [&between](Operation* op) {
        return between.contains(op) && computesFromTheRow(op) && !raisesOnSomeRows(op);
    };

    MaskCone cone;
    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value conjunct : stacked._merged) {
        collectConePostOrder(conjunct, computedBetween, visited, cone);
    }

    mlir::IRMapping mapping;
    for (const auto [filtered, column] : llvm::zip_equal(lower.getFilteredColumns(), lower.getColumnsToFilter())) {
        mapping.map(filtered, column);
    }

    rewriter.setInsertionPoint(lower);
    cloneRowCone(cone._ops, mapping, rewriter);

    llvm::SmallVector<Value, 4> conjuncts {lower.getMask()};
    for (const Value conjunct : stacked._merged) {
        conjuncts.push_back(mapping.lookupOrDefault(conjunct));
    }

    const Value mask = conjoin(conjuncts, loc, rewriter);
    rewriter.modifyOpInPlace(lower, [&lower, mask]() {
        lower.getMaskMutable().assign(mask);
    });

    const Value upperMask = upper.getMask();
    if (stacked._kept.empty()) {
        bypassFilter(upper);
        rewriter.eraseOp(upper);
    } else {
        rewriter.setInsertionPoint(upper);
        const Value keptMask = conjoin(stacked._kept, loc, rewriter);

        rewriter.modifyOpInPlace(upper, [&upper, keptMask]() {
            upper.getMaskMutable().assign(keptMask);
        });
    }

    eraseUnusedConjunction(upperMask, rewriter);
    eraseUnusedRowCone(cone._ops, rewriter);
}

struct MergeFilters : public impl::MergeFiltersBase<MergeFilters> {
    void runOnOperation() override {
        runFilterWorklist<StackedFilters>(getOperation(), matchStackedFilters, mergeStackedFilters);
    }
};

void stageConjuncts(const DBStagePlanner::FilterStages& stages, mlir::RewriterBase& rewriter) {
    FilterOp filter = stages._filter;
    const Location location = filter.getLoc();
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const llvm::ArrayRef<Value> conjuncts = stages._conjuncts;
    const llvm::ArrayRef<Operation*> moved = stages._moved;
    const llvm::ArrayRef<Value> carried = stages._carried;

    mlir::IRMapping mapping;
    rewriter.setInsertionPoint(filter);

    llvm::SmallVector<Value, 4> stageMasks;
    llvm::SmallVector<Value> operands;
    llvm::SmallVector<Type> types;
    const auto addOperand = [&mapping, &operands, &types](Value column) {
        const Value operand = mapping.lookupOrDefault(column);
        operands.push_back(operand);
        types.push_back(operand.getType());
    };

    DBStagePlanner::StageEnd stageStart;
    FilterOp stageFilter;
    for (const DBStagePlanner::StageEnd& stageEnd : stages._stageEnds) {
        cloneRowCone(moved.slice(stageStart._ops, stageEnd._ops - stageStart._ops), mapping, rewriter);

        stageMasks.clear();
        for (const Value conjunct : conjuncts.slice(stageStart._conjuncts, stageEnd._conjuncts - stageStart._conjuncts)) {
            stageMasks.push_back(mapping.lookupOrDefault(conjunct));
        }

        const llvm::ArrayRef<Value> stageCarried = carried.slice(stageStart._carried, stageEnd._carried - stageStart._carried);
        operands.clear();
        types.clear();
        for (const Value column : columns) {
            addOperand(column);
        }

        for (const Value column : stageCarried) {
            addOperand(column);
        }

        const Value stageMask = conjoin(stageMasks, location, rewriter);
        stageFilter = rewriter.create<FilterOp>(location, types, stageMask, operands);

        const ResultRange kept = stageFilter.getFilteredColumns();
        const size_t columnCount = columns.size();
        for (size_t index = 0; index < columnCount; index++) {
            mapping.map(columns[index], kept[index]);
        }

        for (size_t index = 0; index < stageCarried.size(); index++) {
            mapping.map(stageCarried[index], kept[columnCount + index]);
        }

        stageStart = stageEnd;
    }

    rewriter.replaceOp(filter, stageFilter.getResults());
    for (Operation* const joint : stages._joints) {
        eraseIfUnused(joint, rewriter);
    }

    eraseUnusedRowCone(moved, rewriter);
}

struct StageFilterConjuncts : public impl::StageFilterConjunctsBase<StageFilterConjuncts> {
    StageFilterConjuncts() {}

    StageFilterConjuncts(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        if (!_context->_view) {
            return;
        }

        Operation* const root = getOperation();

        llvm::SmallVector<FilterOp> candidates;
        bool holdsAWrite = false;
        root->walk([&candidates, &holdsAWrite](Operation* op) {
            holdsAWrite = holdsAWrite || isGraphWrite(op);

            FilterOp filter = dyn_cast<FilterOp>(op);
            if (!filter) {
                return;
            }

            const Value mask = filter.getMask();
            const bool readOnlyByTheFilter = mask.hasOneUse();
            if (readOnlyByTheFilter && mask.getDefiningOp<AndOp>()) {
                candidates.push_back(filter);
            }
        });

        if (candidates.empty()) {
            return;
        }

        DBWrittenOrder writtenOrder;
        if (holdsAWrite) {
            writtenOrder.build(root);
        }

        const RowEstimator estimator(*_context->_view);
        WalkKnowledge knowledge;
        root->walk([&estimator, &knowledge](Operation* op) {
            const std::optional<double> seeded = estimateSourceRows(op, estimator, knowledge);
            if (!seeded) {
                estimateRowMultiplier(op, estimator, knowledge);
            }

            knowledge.handOn(op);
        });

        ConeCache cache;
        const auto nodeOf = [&cache](Value column) {
            return cache.climbFilters(column);
        };

        const auto selectivityOf = [&estimator, &knowledge, &nodeOf](Value predicate) {
            return estimator.getPredicateSelectivity(predicate, nodeOf, knowledge);
        };

        DBStagePlanner planner(&writtenOrder);
        std::deque<DBStagePlanner::FilterStages> plans;
        for (const FilterOp filter : candidates) {
            DBStagePlanner::FilterStages& stages = plans.emplace_back();
            if (!planner.plan(filter, selectivityOf, stages)) {
                plans.pop_back();
            }
        }

        mlir::IRRewriter rewriter(&getContext());
        for (const DBStagePlanner::FilterStages& stages : llvm::reverse(plans)) {
            stageConjuncts(stages, rewriter);
        }
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

}

std::unique_ptr<Pass> createStageFilterConjuncts(const DBPassContext* context) {
    return std::make_unique<StageFilterConjuncts>(context);
}

}
