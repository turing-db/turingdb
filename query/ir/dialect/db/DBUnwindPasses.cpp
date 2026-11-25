#include "DBPasses.h"

#include <stdint.h>
#include <algorithm>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "DBOps.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBUnwindEqualityCross.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSEUNWINDEQUALITY
#define GEN_PASS_DEF_FUSEUNWINDRANGE
#define GEN_PASS_DEF_UNNESTUNWINDCOMPREHENSION
#define GEN_PASS_DEF_SINKUNWINDSOURCE
#include "DBPasses.h.inc"

namespace {

// Moves the relation factor's ops out to where the product stood and rewires the results
// it contributed to the columns it yielded, leaving the product's own results unread.
void inlineRelationFactor(CrossProduct product, Region& relationFactor, bool unwindOnTheLeft) {
    Block& relationBlock = relationFactor.front();
    Yield relationYield = cast<Yield>(relationBlock.getTerminator());

    const llvm::SmallVector<Value> yielded(relationYield.getColumns().begin(),
                                           relationYield.getColumns().end());

    Block* const parentBlock = product->getBlock();
    parentBlock->getOperations().splice(Block::iterator(product),
                                        relationBlock.getOperations(),
                                        relationBlock.begin(),
                                        Block::iterator(relationYield));

    // The unwound column is the product's first result or its last, so the relation's run
    // from the other end.
    const size_t firstRelationResult = unwindOnTheLeft ? 1 : 0;
    for (size_t index = 0; index < yielded.size(); index++) {
        product.getResult(firstRelationResult + index).replaceAllUsesWith(yielded[index]);
    }
}

Value buildElementDisjunction(UnwindEqualityCross& match, mlir::OpBuilder& builder) {
    const Location loc = match._equality.getLoc();
    const Type boolColumnType = match._equality.getResult().getType();

    builder.setInsertionPoint(match._equality);

    Value mask;
    for (const Attribute element : match._unwind.getElements()) {
        int64_t entityID = 0;
        const bool namesAnEntity = match._converted && entityIDOfLiteral(element, entityID);
        if (match._converted && !namesAnEntity) {
            continue;
        }

        const Attribute value = namesAnEntity ? Attribute(builder.getI64IntegerAttr(entityID)) : element;
        ConstantOp constant = builder.create<ConstantOp>(loc, value);
        EqOp equality = builder.create<EqOp>(loc, boolColumnType, match._comparedColumn, constant.getResult());

        if (!mask) {
            mask = equality.getResult();
            continue;
        }

        mask = builder.create<OrOp>(loc, boolColumnType, mask, equality.getResult()).getResult();
    }

    return mask;
}

void fuseUnwindEquality(UnwindEqualityCross& match, mlir::OpBuilder& builder) {
    inlineRelationFactor(match._product, *match._relationFactor, match._unwindOnTheLeft);

    // Inlining rewired every use of the columns the relation contributed, the equality's
    // own operand among them, so the compared column is read back rather than remembered.
    const Value compared = match._converted ? match._converted.getResult() : match._unwoundColumn;
    const Value lhs = match._equality.getLhs();
    match._comparedColumn = lhs == compared ? match._equality.getRhs() : lhs;

    const Value mask = buildElementDisjunction(match, builder);

    // The elements the rows carried are gone. On every row the filter keeps they were the
    // compared value, so that column takes their place where something still reads them,
    // and the slot goes away where nothing does.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();

    const bool convertsTheStandIn = match._readsTheIDs || match._readsTheElement;

    llvm::SmallVector<Value> columns;
    llvm::SmallVector<Type> resultTypes;
    llvm::SmallVector<size_t> keptColumns;
    llvm::SmallVector<size_t> elementsReadConverted;
    for (size_t index = 0; index < carried.size(); index++) {
        const bool holdsTheElements = carried[index] == match._unwoundColumn;
        if (holdsTheElements && filtered[index].use_empty()) {
            continue;
        } else if (holdsTheElements && convertsTheStandIn) {
            elementsReadConverted.push_back(index);
            continue;
        }

        const Value kept = holdsTheElements ? match._comparedColumn : carried[index];

        columns.push_back(kept);
        resultTypes.push_back(kept.getType());
        keptColumns.push_back(index);
    }

    const Value standInSource = match._comparedColumn;
    const size_t standInIndex = llvm::find(columns, standInSource) - columns.begin();
    if (convertsTheStandIn && standInIndex == columns.size()) {
        columns.push_back(standInSource);
        resultTypes.push_back(standInSource.getType());
    }

    builder.setInsertionPoint(match._filter);
    FilterOp fused = builder.create<FilterOp>(match._filter.getLoc(), resultTypes, mask, columns);

    for (size_t index = 0; index < keptColumns.size(); index++) {
        filtered[keptColumns[index]].replaceAllUsesWith(fused.getResult(index));
    }

    if (convertsTheStandIn) {
        builder.setInsertionPointAfter(fused);

        const Location loc = match._filter.getLoc();
        const Type elementsType = match._unwoundColumn.getType();
        const Value standIn = fused.getResult(standInIndex);

        Value elements;
        if (match._readsTheIDs) {
            elements = builder.create<ElementID>(loc, elementsType, standIn).getResult();
        } else {
            ConstantOp element = builder.create<ConstantOp>(loc, match._unwind.getElements()[0]);
            elements = builder.create<BroadcastConstant>(loc, element.getResult(), standIn).getResult();
        }

        for (const size_t index : elementsReadConverted) {
            filtered[index].replaceAllUsesWith(elements);
        }
    }

    match._filter.erase();
    match._equality.erase();
    if (match._converted) {
        match._converted.erase();
    }
    match._product.erase();
}

struct FuseUnwindEquality : public impl::FuseUnwindEqualityBase<FuseUnwindEquality> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect matches first: fusing erases ops, which would invalidate the walk.
        llvm::SmallVector<UnwindEqualityCross> matches;
        root->walk([&matches](CrossProduct product) {
            UnwindEqualityCross match;
            if (matchUnwindEqualityCross(product, match)) {
                matches.push_back(match);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (UnwindEqualityCross& match : matches) {
            fuseUnwindEquality(match, builder);
        }
    }
};

// What an unwound list is when it is a run of a range: the range, the one slice taken of it
// in its own order - db.tail being the slice from 1 - and whether the run is reversed. The
// chain holds the ops above the range, from the unwind down.
struct RangeRun {
    Range _range;
    Value _from;
    Value _to;
    bool _sliced {false};
    bool _fromOne {false};
    bool _reversed {false};
    llvm::SmallVector<Operation*, 4> _chain;
};

bool matchRangeRun(Value list, RangeRun& run) {
    bool pendingReversal = false;

    Value current = list;
    while (Operation* const op = current.getDefiningOp()) {
        if (Range range = dyn_cast<Range>(op)) {
            // A slice is cut in the range's own order, so a reversal under it is no run
            if (run._sliced && pendingReversal) {
                return false;
            }

            run._range = range;
            run._reversed = run._reversed != pendingReversal;
            return true;
        } else if (Reverse reverse = dyn_cast<Reverse>(op)) {
            pendingReversal = !pendingReversal;
            current = reverse.getInput();
        } else if (ListSlice slice = dyn_cast<ListSlice>(op)) {
            if (run._sliced) {
                return false;
            }

            run._sliced = true;
            run._from = slice.getFrom();
            run._to = slice.getTo();
            run._reversed = pendingReversal;
            pendingReversal = false;
            current = slice.getList();
        } else if (Tail tail = dyn_cast<Tail>(op)) {
            if (run._sliced) {
                return false;
            }

            run._sliced = true;
            run._fromOne = true;
            run._reversed = pendingReversal;
            pendingReversal = false;
            current = tail.getInput();
        } else {
            return false;
        }

        run._chain.push_back(op);
    }

    return false;
}

bool fuseUnwindRange(Unwind unwind, mlir::RewriterBase& rewriter) {
    RangeRun run;
    if (!matchRangeRun(unwind.getSource(), run)) {
        return false;
    }

    Range range = run._range;
    const Location loc = unwind.getLoc();
    rewriter.setInsertionPoint(unwind);

    Value step = range.getStep();
    if (!step) {
        step = rewriter.create<ConstantOp>(loc, rewriter.getI64IntegerAttr(1)).getResult();
    }

    Value from = run._from;
    if (run._fromOne) {
        from = rewriter.create<ConstantOp>(loc, rewriter.getI64IntegerAttr(1)).getResult();
    }

    UnwindRange fused = rewriter.create<UnwindRange>(loc,
                                                     unwind->getResultTypes(),
                                                     range.getStart(),
                                                     range.getEnd(),
                                                     step,
                                                     from,
                                                     run._to,
                                                     unwind.getColumnsToFilter(),
                                                     run._reversed);

    rewriter.replaceOp(unwind, fused->getResults());

    for (Operation* const op : run._chain) {
        eraseIfUnused(op, rewriter);
    }

    eraseIfUnused(range.getOperation(), rewriter);

    return true;
}

struct FuseUnwindRange : public impl::FuseUnwindRangeBase<FuseUnwindRange> {
    void runOnOperation() override {
        runWorklist<Unwind>(getOperation(), [](Operation* op, mlir::RewriterBase& rewriter) {
            return fuseUnwindRange(cast<Unwind>(op), rewriter);
        });
    }
};

// The values a comprehension's row tag reaches: the tag and what each WHERE cuts it to. False
// where the tag reaches anything but those cuts and the yield, which an unwind has no row
// tag to give.
bool collectRowTags(Block& body, ComprehensionYield yield, llvm::SmallPtrSetImpl<Value>& rowTags) {
    llvm::SmallVector<Value> worklist {body.getArgument(1)};

    while (!worklist.empty()) {
        const Value rowTag = worklist.pop_back_val();
        rowTags.insert(rowTag);

        for (OpOperand& use : rowTag.getUses()) {
            Operation* const user = use.getOwner();
            FilterOp filter = dyn_cast<FilterOp>(user);
            const unsigned operandNumber = use.getOperandNumber();

            const bool cutByAFilter = filter && filter->getBlock() == &body && operandNumber != 0;
            if (cutByAFilter) {
                worklist.push_back(filter->getResult(operandNumber - 1));
            } else if (user != yield.getOperation()) {
                return false;
            }
        }
    }

    return rowTags.contains(yield.getRowTags());
}

// Re-emits a WHERE of the body over the unwound rows. The tag is dropped from what it cuts,
// and every column of the rows it does not cut yet is cut alongside, so @param rows, the
// columns of the rows in flight, stays row-aligned with the elements.
void cutUnwoundRows(FilterOp filter,
                    const llvm::SmallPtrSetImpl<Value>& rowTags,
                    mlir::IRMapping& mapping,
                    llvm::SmallVectorImpl<Value>& rows,
                    mlir::RewriterBase& rewriter) {
    const OperandRange filteredColumns = filter.getColumnsToFilter();

    llvm::SmallVector<Value> cutColumns;
    for (const Value column : filteredColumns) {
        if (!rowTags.contains(column)) {
            cutColumns.push_back(mapping.lookupOrDefault(column));
        }
    }

    for (const Value column : rows) {
        if (!llvm::is_contained(cutColumns, column)) {
            cutColumns.push_back(column);
        }
    }

    llvm::SmallVector<Type> cutTypes;
    for (const Value column : cutColumns) {
        cutTypes.push_back(column.getType());
    }

    FilterOp cut = rewriter.create<FilterOp>(filter.getLoc(),
                                             cutTypes,
                                             mapping.lookupOrDefault(filter.getMask()),
                                             cutColumns);

    size_t cutIndex = 0;
    for (const auto [column, result] : llvm::zip(filteredColumns, filter.getResults())) {
        if (!rowTags.contains(column)) {
            mapping.map(result, cut.getResult(cutIndex));
            cutIndex++;
        }
    }

    for (Value& column : rows) {
        const size_t position = llvm::find(cutColumns, column) - cutColumns.begin();
        column = cut.getResult(position);
    }
}

// UNWIND [x IN xs WHERE p | f] AS y is UNWIND xs AS x WITH x WHERE p, then f as y: the body
// of the comprehension runs over the unwound rows, so no list is built for it.
bool unnestUnwindComprehension(Unwind unwind, mlir::RewriterBase& rewriter) {
    ListComprehension comprehension = unwind.getSource().getDefiningOp<ListComprehension>();
    if (!comprehension || !comprehension->hasOneUse()) {
        return false;
    }

    Block& body = comprehension.getBody().front();
    ComprehensionYield yield = cast<ComprehensionYield>(body.getTerminator());

    // A value standing for every element would stand for one row once it is unwound
    if (::db::yieldsConstantColumn(yield.getValue())) {
        return false;
    }

    llvm::SmallPtrSet<Value, 4> rowTags;
    if (!collectRowTags(body, yield, rowTags)) {
        return false;
    }

    const OperandRange bodyColumns = comprehension.getColumnsToFilter();
    const OperandRange unwoundColumns = unwind.getColumnsToFilter();

    llvm::SmallVector<Value> carried(bodyColumns.begin(), bodyColumns.end());
    for (const Value column : unwoundColumns) {
        if (!llvm::is_contained(carried, column)) {
            carried.push_back(column);
        }
    }

    llvm::SmallVector<Type> resultTypes {unwind.getElement().getType()};
    for (const Value column : carried) {
        resultTypes.push_back(column.getType());
    }

    rewriter.setInsertionPoint(unwind);
    Unwind unnested = rewriter.create<Unwind>(unwind.getLoc(), resultTypes, comprehension.getSource(), carried);

    const ResultRange unnestedCarried = unnested.getCarried();
    llvm::SmallVector<Value> rows(unnestedCarried.begin(), unnestedCarried.end());

    mlir::IRMapping mapping;
    mapping.map(body.getArgument(0), unnested.getElement());

    for (size_t carriedIndex = 0; carriedIndex < bodyColumns.size(); carriedIndex++) {
        mapping.map(body.getArgument(carriedIndex + 2), rows[carriedIndex]);
    }

    for (Operation& op : body.without_terminator()) {
        if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            cutUnwoundRows(filter, rowTags, mapping, rows, rewriter);
        } else {
            rewriter.clone(op, mapping);
        }
    }

    llvm::SmallVector<Value> replacements {mapping.lookupOrDefault(yield.getValue())};
    for (const Value column : unwoundColumns) {
        const size_t position = llvm::find(carried, column) - carried.begin();
        replacements.push_back(rows[position]);
    }

    rewriter.replaceOp(unwind, replacements);
    rewriter.eraseOp(comprehension);

    return true;
}

struct UnnestUnwindComprehension : public impl::UnnestUnwindComprehensionBase<UnnestUnwindComprehension> {
    void runOnOperation() override {
        runWorklist<Unwind>(getOperation(), [](Operation* op, mlir::RewriterBase& rewriter) {
            return unnestUnwindComprehension(cast<Unwind>(op), rewriter);
        });
    }
};

// The ops computing an unwind's source that nothing else reads, in block order, and the
// columns of the rows they read from outside them.
struct UnwindSourceCone {
    llvm::SmallVector<Operation*> _ops;
    llvm::SmallVector<Value> _inputs;
};

bool collectUnwindSourceCone(Unwind unwind, UnwindSourceCone& cone) {
    const Value source = unwind.getSource();
    Operation* const root = source.getDefiningOp();
    Block* const block = unwind->getBlock();

    // A source read from storage is a view of what the graph holds, so no list is built for it
    const bool computedHere = root && root->getBlock() == block && computesAValuePerRow(root)
                           && !isa<GetNodeProperties, GetEdgeProperties>(root);
    if (!computedHere || !source.hasOneUse() || root->getNumResults() != 1 || ::db::yieldsConstantColumn(source)) {
        return false;
    }

    llvm::SmallPtrSet<Operation*, 16> inCone {root};
    cone._ops.push_back(root);

    // Every reader of an op comes after it, so walking back from the root decides each op
    // once all of its readers are decided
    for (Operation* op = root->getPrevNode(); op; op = op->getPrevNode()) {
        if (!computesAValuePerRow(op)) {
            continue;
        }

        bool readInCone = false;
        bool readOutside = false;
        for (Operation* const user : op->getUsers()) {
            Operation* const ancestor = block->findAncestorOpInBlock(*user);
            if (ancestor && inCone.contains(ancestor)) {
                readInCone = true;
            } else {
                readOutside = true;
            }
        }

        if (readInCone && !readOutside) {
            inCone.insert(op);
            cone._ops.push_back(op);
        }
    }

    std::reverse(cone._ops.begin(), cone._ops.end());

    const auto definedInCone = [block, &inCone](Value value) {
        Operation* const definer = value.getDefiningOp() ? value.getDefiningOp() : value.getParentBlock()->getParentOp();
        Operation* const ancestor = definer ? block->findAncestorOpInBlock(*definer) : nullptr;
        return ancestor && inCone.contains(ancestor);
    };

    for (Operation* const op : cone._ops) {
        op->walk([&](Operation* nested) {
            for (const Value operand : nested->getOperands()) {
                const bool readFromOutside = !definedInCone(operand) && !::db::yieldsConstantColumn(operand);
                if (readFromOutside && !llvm::is_contained(cone._inputs, operand)) {
                    cone._inputs.push_back(operand);
                }
            }
        });
    }

    return !cone._inputs.empty();
}

// Moves what an unwind's source is computed from into a db.unwind_computed, so lowering can
// compute it a few input rows at a time.
bool sinkUnwindSource(Unwind unwind, mlir::RewriterBase& rewriter) {
    UnwindSourceCone cone;
    if (!collectUnwindSourceCone(unwind, cone)) {
        return false;
    }

    const OperandRange unwoundColumns = unwind.getColumnsToFilter();

    llvm::SmallVector<Value> carried(unwoundColumns.begin(), unwoundColumns.end());
    for (const Value input : cone._inputs) {
        if (!llvm::is_contained(carried, input)) {
            carried.push_back(input);
        }
    }

    llvm::SmallVector<Type> carriedTypes;
    llvm::SmallVector<Location> carriedLocations;
    for (const Value column : carried) {
        carriedTypes.push_back(column.getType());
        carriedLocations.push_back(column.getLoc());
    }

    llvm::SmallVector<Type> resultTypes {unwind.getElement().getType()};
    resultTypes.append(carriedTypes.begin(), carriedTypes.end());

    const Location loc = unwind.getLoc();
    rewriter.setInsertionPoint(unwind);
    UnwindComputed computed = rewriter.create<UnwindComputed>(loc, resultTypes, carried);

    Block* const sourceBlock = rewriter.createBlock(&computed.getSource(), {}, carriedTypes, carriedLocations);

    mlir::IRMapping mapping;
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        mapping.map(carried[carriedIndex], sourceBlock->getArgument(static_cast<unsigned>(carriedIndex)));
    }

    rewriter.setInsertionPointToEnd(sourceBlock);
    for (Operation* const op : cone._ops) {
        rewriter.clone(*op, mapping);
    }

    rewriter.create<UnwindYield>(loc, mapping.lookup(unwind.getSource()));

    rewriter.replaceOp(unwind, computed->getResults().take_front(unwind->getNumResults()));

    for (Operation* const op : llvm::reverse(cone._ops)) {
        rewriter.eraseOp(op);
    }

    return true;
}

struct SinkUnwindSource : public impl::SinkUnwindSourceBase<SinkUnwindSource> {
    void runOnOperation() override {
        runWorklist<Unwind>(getOperation(), [](Operation* op, mlir::RewriterBase& rewriter) {
            return sinkUnwindSource(cast<Unwind>(op), rewriter);
        });
    }
};

}

}
