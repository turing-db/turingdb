#include "DBPasses.h"

#include <stdint.h>
#include <optional>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"

#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBCrossProducts.h"
#include "DBPassUtils.h"
#include "DBRowCone.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_TRIMUNREADCOLUMNS
#include "DBPasses.h.inc"

namespace {

bool carriesIntoItsRegions(Operation* op) {
    ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(op);
    if (!carrier) {
        return false;
    }

    llvm::SmallVector<Value, 1> carried;
    for (Region& region : op->getRegions()) {
        for (const BlockArgument argument : region.getArguments()) {
            carrier.collectCarriedColumns(argument, carried);
        }
    }

    return !carried.empty();
}

// The columns a trim drops in pairs, each operand with the result it comes back as. A dedup
// keeps every column it carries, its key being all of them, and an op whose regions read its
// carry set keeps it for them
bool matchTrimmedColumns(Operation* op, CarrySet& carrySet) {
    const bool keepsItsCarrySet = isa<RemoveDuplicates>(op) || carriesIntoItsRegions(op);
    if (keepsItsCarrySet || !matchCarrySet(op, carrySet)) {
        return false;
    }

    carrySet._count = trimmedOperandsOf(op).size();

    return true;
}

bool trimsColumns(Operation* op) {
    CarrySet carrySet;

    return matchTrimmedColumns(op, carrySet) || isa<CrossProduct, HashJoin, OptionalMatch>(op);
}

void keepOneOf(llvm::SmallBitVector& keep, size_t begin, size_t end) {
    if (begin >= end) {
        return;
    }

    for (size_t index = begin; index < end; index++) {
        if (keep[index]) {
            return;
        }
    }

    keep.set(begin);
}

// A sort orders by its keys and a grouping op groups by them whether or not anything reads
// them; a group aggregate still has to reduce something and a collect to gather something.
void keepRequiredColumns(Operation* op, llvm::SmallBitVector& keep) {
    if (Sort sort = dyn_cast<Sort>(op)) {
        for (const int64_t keyColumn : sort.getKeyColumns()) {
            keep.set(static_cast<unsigned>(keyColumn));
        }
    } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
        if (const std::optional<uint64_t> endColumn = exploration.getEndColumn()) {
            keep.set(static_cast<unsigned>(*endColumn));
        }
    } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
        const size_t keyCount = groupAggregate.getKeyCount();
        keep.set(0, keyCount);
        keepOneOf(keep, keyCount, keep.size());
    } else if (Collect collect = dyn_cast<Collect>(op)) {
        const size_t keyCount = collect.getKeyCount();
        keep.set(0, keyCount);
        keepOneOf(keep, keyCount, keyCount + collectValueCount(collect));
    }
}

// An op standing for a relation is sized by a row-carrying column of it during lowering, so
// one has to survive whenever the op had one.
void keepRowCarryingColumn(ValueRange columns, size_t firstIndex, llvm::SmallBitVector& keep) {
    std::optional<size_t> firstRowCarrying;
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (::db::yieldsConstantColumn(columns[columnIndex])) {
            continue;
        }

        if (keep[firstIndex + columnIndex]) {
            return;
        }

        if (!firstRowCarrying) {
            firstRowCarrying = columnIndex;
        }
    }

    if (firstRowCarrying) {
        keep.set(firstIndex + *firstRowCarrying);
    } else if (!columns.empty()) {
        keepOneOf(keep, firstIndex, firstIndex + columns.size());
    }
}

void selectKeptCarriedColumns(Operation* op, const CarrySet& carrySet, llvm::SmallVectorImpl<size_t>& kept) {
    const size_t count = carrySet._count;

    llvm::SmallBitVector keep(count);
    for (size_t carriedIndex = 0; carriedIndex < count; carriedIndex++) {
        if (!op->getResult(carrySet._resultOffset + carriedIndex).use_empty()) {
            keep.set(carriedIndex);
        }
    }

    keepRequiredColumns(op, keep);

    const bool standsForItsRows = carrySet._resultOffset == 0;
    if (standsForItsRows) {
        keepRowCarryingColumn(op->getOperands().slice(carrySet._operandOffset, count), 0, keep);
    }

    for (size_t carriedIndex = 0; carriedIndex < count; carriedIndex++) {
        if (keep[carriedIndex]) {
            kept.push_back(carriedIndex);
        }
    }
}

// The results a two-factor op has to keep: the ones something reads, the ones the op
// itself matches on, and - since lowering sizes each side by a column of it - one
// row-carrying column per factor. False when that is every result, so the caller leaves
// the op alone.
bool selectKeptFactorColumns(Operation* op, llvm::SmallBitVector& keep) {
    Yield leftYield = factorYield(op, 0);
    Yield rightYield = factorYield(op, 1);
    const size_t leftCount = leftYield.getNumOperands();

    const Operation::result_range results = op->getResults();

    keep.resize(results.size());
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (!results[resultIndex].use_empty()) {
            keep.set(resultIndex);
        }
    }

    // A join reads its two key columns to match on whether or not anything downstream
    // reads them, the way a sort orders by its keys.
    if (HashJoin join = dyn_cast<HashJoin>(op)) {
        keep.set(join.getLeftKey());
        keep.set(leftCount + join.getRightKey());
    }

    keepRowCarryingColumn(leftYield.getColumns(), 0, keep);
    keepRowCarryingColumn(rightYield.getColumns(), leftCount, keep);

    return !keep.all();
}

// Where a factor's key column lands once that factor's unkept columns are gone: the kept
// columns of the factor ahead of it. firstIndex is where the factor's columns start among
// the op's results.
size_t trimmedKeyColumn(const llvm::SmallBitVector& keep, size_t firstIndex, size_t keyColumn) {
    size_t trimmed = 0;
    for (size_t columnIndex = 0; columnIndex < keyColumn; columnIndex++) {
        if (keep[firstIndex + columnIndex]) {
            trimmed++;
        }
    }

    return trimmed;
}

// A product's results are the columns its two factors yield; a result nobody reads leaves
// the yield, and each factor keeps a row-carrying column to be sized by.
void trimCrossProduct(CrossProduct product, mlir::OpBuilder& builder) {
    Operation* const productOp = product.getOperation();

    llvm::SmallBitVector keep;
    if (!selectKeptFactorColumns(productOp, keep)) {
        return;
    }

    const size_t leftCount = factorYield(productOp, 0).getNumOperands();

    llvm::SmallVector<Type> trimmedTypes;
    keptResultTypes(productOp, keep, trimmedTypes);

    builder.setInsertionPoint(product);
    CrossProduct trimmed = builder.create<CrossProduct>(product.getLoc(), trimmedTypes);

    replaceWithTrimmedFactors(productOp, trimmed.getOperation(), keep, leftCount);
    productOp->erase();
}

// The product's sibling, with the two key columns kept on top of what is read and each
// renumbered into the yield the trim leaves behind.
void trimHashJoin(HashJoin join, mlir::OpBuilder& builder) {
    Operation* const joinOp = join.getOperation();

    llvm::SmallBitVector keep;
    if (!selectKeptFactorColumns(joinOp, keep)) {
        return;
    }

    const size_t leftCount = factorYield(joinOp, 0).getNumOperands();
    const size_t leftKey = trimmedKeyColumn(keep, 0, join.getLeftKey());
    const size_t rightKey = trimmedKeyColumn(keep, leftCount, join.getRightKey());

    llvm::SmallVector<Type> trimmedTypes;
    keptResultTypes(joinOp, keep, trimmedTypes);

    builder.setInsertionPoint(join);
    HashJoin trimmed = builder.create<HashJoin>(join.getLoc(), trimmedTypes, leftKey, rightKey);

    replaceWithTrimmedFactors(joinOp, trimmed.getOperation(), keep, leftCount);
    joinOp->erase();
}

// A CASE or a coalesce lays its rows out over a column it carries into its regions, and so
// does an op over the elements of a constant list, so one row-carrying column stays for them
bool sizesItsRowsByItsRegionCarrySet(Operation* op) {
    if (isa<LazyCase, Coalesce>(op)) {
        return true;
    } else if (ListComprehension comprehension = dyn_cast<ListComprehension>(op)) {
        return ::db::yieldsConstantColumn(comprehension.getSource());
    } else if (ListPredicate predicate = dyn_cast<ListPredicate>(op)) {
        return ::db::yieldsConstantColumn(predicate.getSource());
    } else if (Reduce reduce = dyn_cast<Reduce>(op)) {
        return ::db::yieldsConstantColumn(reduce.getSource());
    } else if (AllReduce allReduce = dyn_cast<AllReduce>(op)) {
        return ::db::yieldsConstantColumn(allReduce.getSource());
    } else {
        return false;
    }
}

// A column a region forwards that no region after it reads leaves its yield and the arguments
// of every region after it
void trimForwardedColumns(Operation* op) {
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);
    const size_t regionCount = op->getNumRegions();

    for (size_t sourceIndex = 0; sourceIndex < regionCount; sourceIndex++) {
        Region& source = op->getRegion(static_cast<unsigned>(sourceIndex));
        if (!carrier.forwardsColumns(source)) {
            continue;
        }

        MutableOperandRange forwarded = carrier.getForwardedColumnsMutable(source);
        const OperandRange columns = forwarded;
        const size_t forwardOffset = forwardedArgumentCount(source);

        llvm::SmallBitVector read(columns.size());
        for (size_t laterIndex = sourceIndex + 1; laterIndex < regionCount; laterIndex++) {
            Block& later = op->getRegion(static_cast<unsigned>(laterIndex)).front();
            for (size_t forwardIndex = 0; forwardIndex < columns.size(); forwardIndex++) {
                if (!later.getArgument(static_cast<unsigned>(forwardOffset + forwardIndex)).use_empty()) {
                    read.set(forwardIndex);
                }
            }
        }

        if (read.all()) {
            continue;
        }

        llvm::SmallVector<Value> keptColumns;
        for (size_t forwardIndex = 0; forwardIndex < columns.size(); forwardIndex++) {
            if (read[forwardIndex]) {
                keptColumns.push_back(columns[forwardIndex]);
            }
        }

        for (size_t laterIndex = sourceIndex + 1; laterIndex < regionCount; laterIndex++) {
            Block& later = op->getRegion(static_cast<unsigned>(laterIndex)).front();

            llvm::BitVector erasedArguments(later.getNumArguments());
            for (size_t forwardIndex = 0; forwardIndex < columns.size(); forwardIndex++) {
                if (!read[forwardIndex]) {
                    erasedArguments.set(forwardOffset + forwardIndex);
                }
            }

            later.eraseArguments(erasedArguments);
        }

        forwarded.assign(keptColumns);
    }
}

// A column no region reads leaves the operands and the arguments of every region with a body
// together. A column carried twice - the subject of `CASE t WHEN ...` beside the variable t -
// is read through its first argument.
void trimRegionCarrySet(Operation* op) {
    trimForwardedColumns(op);

    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);
    MutableOperandRange carriedOperands = carrier.getRegionCarrySetMutable();
    const OperandRange carried = carriedOperands;

    llvm::SmallVector<std::pair<Block*, size_t>, 4> bodies;
    for (Region& region : op->getRegions()) {
        if (region.empty()) {
            continue;
        }

        RegionCarrySet carrySet;
        const bool carries = carrier.getRegionCarrySet(region, carrySet);
        bioassert(carries, "An op carrying columns into a region carries them into all of them");

        bodies.emplace_back(&region.front(), carrySet._argumentOffset);
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const size_t firstIndex = static_cast<size_t>(llvm::find(carried, carried[carriedIndex]) - carried.begin());
        if (firstIndex == carriedIndex) {
            continue;
        }

        for (const auto& [body, argumentOffset] : bodies) {
            BlockArgument repeated = body->getArgument(static_cast<unsigned>(argumentOffset + carriedIndex));
            repeated.replaceAllUsesWith(body->getArgument(static_cast<unsigned>(argumentOffset + firstIndex)));
        }
    }

    llvm::SmallBitVector keep(carried.size());
    for (const auto& [body, argumentOffset] : bodies) {
        for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
            if (!body->getArgument(static_cast<unsigned>(argumentOffset + carriedIndex)).use_empty()) {
                keep.set(carriedIndex);
            }
        }
    }

    if (sizesItsRowsByItsRegionCarrySet(op)) {
        keepRowCarryingColumn(carried, 0, keep);
    }

    if (keep.all()) {
        return;
    }

    llvm::SmallVector<Value> keptColumns;
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (keep[carriedIndex]) {
            keptColumns.push_back(carried[carriedIndex]);
        }
    }

    for (const auto& [body, argumentOffset] : bodies) {
        llvm::BitVector erasedArguments(body->getNumArguments());
        for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
            if (!keep[carriedIndex]) {
                erasedArguments.set(argumentOffset + carriedIndex);
            }
        }

        body->eraseArguments(erasedArguments);
    }

    carriedOperands.assign(keptColumns);
}

bool trimsItsRegionCarrySet(Operation* op) {
    return isa<LazyCase, Coalesce, ListComprehension, ListPredicate, Reduce, AllReduce, ExplorePaths>(op);
}

struct TrimVisit {
    Operation* _op {nullptr};
    bool _regionsVisited {false};
};

void pushNestedOps(Operation* op, llvm::SmallVectorImpl<TrimVisit>& worklist) {
    for (Region& region : op->getRegions()) {
        for (Block& block : region) {
            for (Operation& nested : block) {
                worklist.push_back(TrimVisit {._op = &nested});
            }
        }
    }
}

// Each block is swept backwards, so a reader is trimmed before the ops feeding it. An op
// reading its carried columns inside its regions is trimmed after what they hold; a
// two-factor op only reads what its regions yield, so it is trimmed before them.
void collectTrimOrder(Operation* root, llvm::SmallVectorImpl<Operation*>& order) {
    llvm::SmallVector<TrimVisit> worklist;
    pushNestedOps(root, worklist);

    while (!worklist.empty()) {
        const TrimVisit visit = worklist.pop_back_val();
        Operation* const op = visit._op;

        if (visit._regionsVisited) {
            order.push_back(op);
            continue;
        }

        if (trimsItsRegionCarrySet(op)) {
            worklist.push_back(TrimVisit {._op = op, ._regionsVisited = true});
        } else if (trimsColumns(op)) {
            order.push_back(op);
        }

        pushNestedOps(op, worklist);
    }
}

void trimCarriedColumns(Operation* op, const CarrySet& carrySet, mlir::OpBuilder& builder) {
    llvm::SmallVector<size_t> kept;
    selectKeptCarriedColumns(op, carrySet, kept);

    if (kept.size() == carrySet._count) {
        return;
    }

    trimCarrySet(op, carrySet, kept, builder);
    op->erase();
}

// An op that may raise stays, and so does the error a row raises on it. Its own set, not
// @param erased, tells a freed op from a live one: the caller's set can hold an address a
// later op was allocated at, and nothing is allocated while this runs.
template <typename Erasable>
bool eraseUnreadOps(llvm::SmallVectorImpl<Operation*>& pending, Erasable erasable, llvm::SmallPtrSetImpl<Operation*>& erased) {
    llvm::SmallPtrSet<Operation*, 16> freed;

    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val();

        const bool unread = op && !freed.contains(op) && erasable(op) && mlir::isOpTriviallyDead(op);
        if (!unread || raisesOnSomeRows(op)) {
            continue;
        }

        for (const Value operand : op->getOperands()) {
            pending.push_back(operand.getDefiningOp());
        }

        op->walk([&freed](Operation* nested) {
            freed.insert(nested);
        });

        op->erase();
    }

    erased.insert(freed.begin(), freed.end());

    return !freed.empty();
}

// Lowering runs every op it is handed, so the ops a dropped column leaves unread in the
// pattern go too, but for one that may raise. Every op there that shapes the rows hands the
// yield the row tag or a column it keeps, so the ones left unread only computed dropped values.
void eraseUnreadInPattern(llvm::ArrayRef<Value> dropped, Region& pattern, llvm::SmallPtrSetImpl<Operation*>& erased) {
    llvm::SmallVector<Operation*> producers;
    for (const Value column : dropped) {
        producers.push_back(column.getDefiningOp());
    }

    const auto inPattern = [&pattern](Operation* op) {
        return pattern.isAncestor(op->getParentRegion());
    };

    eraseUnreadOps(producers, inPattern, erased);
}

// An optional match pads and carries the variables its pattern binds for what reads them
// past it, so one nothing reads leaves its yield. The columns it joins onto stay: the
// pattern walks them, and a row it missed is rebuilt from them.
void trimOptionalMatch(OptionalMatch optional, mlir::OpBuilder& builder, llvm::SmallPtrSetImpl<Operation*>& erased) {
    OptionalYield yield = patternYield(optional);
    const OperandRange columns = yield.getColumns();
    const size_t inputCount = optional.getInputColumns().size();

    llvm::SmallBitVector keep(columns.size());
    keep.set(0, inputCount);
    for (size_t columnIndex = inputCount; columnIndex < columns.size(); columnIndex++) {
        if (!optional->getResult(columnIndex).use_empty()) {
            keep.set(columnIndex);
        }
    }

    keepRowCarryingColumn(columns, 0, keep);

    if (keep.all()) {
        return;
    }

    llvm::SmallVector<Value> keptColumns;
    llvm::SmallVector<Value> droppedColumns;
    llvm::SmallVector<size_t> keptResults;
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (keep[columnIndex]) {
            keptColumns.push_back(columns[columnIndex]);
            keptResults.push_back(columnIndex);
        } else {
            droppedColumns.push_back(columns[columnIndex]);
        }
    }

    yield.getColumnsMutable().assign(keptColumns);

    OptionalMatch trimmed = rebuildOptionalMatch(optional, keptResults, builder);
    eraseUnreadInPattern(droppedColumns, trimmed.getPattern(), erased);
}

void trimUnreadColumnsOf(Operation* op, mlir::OpBuilder& builder, llvm::SmallPtrSetImpl<Operation*>& erased) {
    if (trimsItsRegionCarrySet(op)) {
        trimRegionCarrySet(op);
    }

    CarrySet carrySet;
    if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
        trimCrossProduct(product, builder);
    } else if (HashJoin join = dyn_cast<HashJoin>(op)) {
        trimHashJoin(join, builder);
    } else if (OptionalMatch optional = dyn_cast<OptionalMatch>(op)) {
        trimOptionalMatch(optional, builder, erased);
    } else if (matchTrimmedColumns(op, carrySet)) {
        trimCarriedColumns(op, carrySet, builder);
    }
}

// Lowering runs every op it is handed, so a value computed per row that nothing reads goes,
// and so do the ones only it read. Unlike a filter or a hop nothing reads, it shapes no row
// a constant could be laid out over.
bool eraseUnreadRowValues(Operation* root) {
    llvm::SmallVector<Operation*> ops;
    root->walk([&ops](Operation* op) {
        ops.push_back(op);
    });

    llvm::SmallPtrSet<Operation*, 16> erased;
    return eraseUnreadOps(ops, computesAValuePerRow, erased);
}

struct TrimUnreadColumns : public impl::TrimUnreadColumnsBase<TrimUnreadColumns> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        // A value erased as unread can be all that read a column a hop or a filter still
        // carries, so the trim runs again until a round erases no value
        bool erasedRowValues = true;
        while (erasedRowValues) {
            llvm::SmallVector<Operation*> order;
            collectTrimOrder(getOperation(), order);

            llvm::SmallPtrSet<Operation*, 16> erased;
            for (Operation* const op : order) {
                if (!erased.contains(op)) {
                    trimUnreadColumnsOf(op, builder, erased);
                }
            }

            erasedRowValues = eraseUnreadRowValues(getOperation());
        }
    }
};

}

}
