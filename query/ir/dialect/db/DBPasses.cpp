#include "DBPasses.h"

#include <math.h>
#include <stdint.h>
#include <algorithm>
#include <deque>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <tuple>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Dominance.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Transforms/RegionUtils.h"
#include "mlir/Transforms/GreedyPatternRewriteDriver.h"

#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/Support/MathExtras.h"

#include "CardinalityEstimation.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"

#include "IRConstantColumn.h"
#include "IRRaiseEffect.h"
#include "DBColumnElement.h"
#include "StorageAttributes.h"
#include "BoundEntityIDs.h"
#include "PathHopArguments.h"
#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBCrossProducts.h"
#include "DBFilterUtils.h"
#include "DBKnownLabels.h"
#include "DBLineageClimbs.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBPatternWalk.h"
#include "DBRowCone.h"
#include "DBStagePlanner.h"
#include "DBUnwindEqualityCross.h"
#include "DBWrittenOrder.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSESCANBYLABEL
#define GEN_PASS_DEF_PUSHDOWNFILTERS
#define GEN_PASS_DEF_MERGEFILTERS
#define GEN_PASS_DEF_STAGEFILTERCONJUNCTS
#define GEN_PASS_DEF_FUSEUNWINDEQUALITY
#define GEN_PASS_DEF_FUSEUNWINDRANGE
#define GEN_PASS_DEF_UNNESTUNWINDCOMPREHENSION
#define GEN_PASS_DEF_SINKUNWINDSOURCE
#define GEN_PASS_DEF_PLACEINFACTORS
#define GEN_PASS_DEF_FUSESCANBYNODEIDS
#define GEN_PASS_DEF_FUSESCANBYPROPERTYVALUE
#define GEN_PASS_DEF_FUSESCANEDGES
#define GEN_PASS_DEF_FUSEEDGESBYTYPE
#define GEN_PASS_DEF_FUSESCANEDGESBYTYPE
#define GEN_PASS_DEF_FUSELABELPREDICATES
#define GEN_PASS_DEF_FUSEEDGETYPEPREDICATES
#define GEN_PASS_DEF_NARROWEDGETYPEREADS
#define GEN_PASS_DEF_FUSESCANOUTEDGESBYLABEL
#define GEN_PASS_DEF_FUSESCANINEDGESBYLABEL
#define GEN_PASS_DEF_FUSESCANEDGESBYENDPOINTLABEL
#define GEN_PASS_DEF_FUSEEDGESBYENDPOINTLABEL
#define GEN_PASS_DEF_REMOVEREDUNDANTLABELCHECKS
#define GEN_PASS_DEF_REUSEPROPERTYREADS
#define GEN_PASS_DEF_REUSELABELANDTYPEREADS
#define GEN_PASS_DEF_CARRYLABELANDTYPEREADS
#define GEN_PASS_DEF_FUSEHASHJOIN
#define GEN_PASS_DEF_FUSEFETCHNODES
#define GEN_PASS_DEF_REROOTPATTERNATSEED
#define GEN_PASS_DEF_REROOTPATTERNATEDGEIDS
#define GEN_PASS_DEF_REROOTPATTERNATCHEAPESTNODE
#define GEN_PASS_DEF_FUSEEXPLOREENDCONSTRAINT
#define GEN_PASS_DEF_HOISTHOPCONJUNCTS
#define GEN_PASS_DEF_FUSEEXPLOREHOPLABELS
#define GEN_PASS_DEF_SINKMAKEPATH
#define GEN_PASS_DEF_SINKINTOOPTIONALMATCH
#define GEN_PASS_DEF_FUSEPATHELEMENTS
#define GEN_PASS_DEF_FUSEPATHLENGTH
#define GEN_PASS_DEF_FUSEEXPLOREENDNODES
#define GEN_PASS_DEF_FUSEEXPLOREENDFACTOR
#define GEN_PASS_DEF_FUSEEXPLOREENDSET
#define GEN_PASS_DEF_FUSEEXPLORELISTPREDICATE
#define GEN_PASS_DEF_FUSEEXPLOREDISTINCTENDS
#define GEN_PASS_DEF_COUNTPATHROWS
#define GEN_PASS_DEF_TRIMUNREADCOLUMNS
#define GEN_PASS_DEF_COUNTFROMMETADATA
#define GEN_PASS_DEF_CANONICALIZECOALESCE
#define GEN_PASS_DEF_CANONICALIZEIDEQUALITIES
#define GEN_PASS_DEF_HOISTUNCONDITIONALREGIONS
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

struct ScanSource {
    Operation* _op {nullptr};
    ArrayAttr _labels;
    Value _column;
};

// Any carried column beyond the scan's own (a property read before the WHERE, say) has no
// filtered counterpart in a source op to be rewired to, so such a filter has to stay.
bool matchSoleScanSource(FilterOp filter, ScanSource& source) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return false;
    }

    source._column = columns.front();
    source._op = source._column.getDefiningOp();
    if (!source._op || !isa<ScanNodes, ScanNodesByLabel>(source._op)) {
        return false;
    }

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(source._op)) {
        source._labels = scanByLabel.getLabels();
    }

    return true;
}

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

void appendCone(const MaskCone& from, llvm::SmallPtrSetImpl<Operation*>& appended, MaskCone& cone) {
    for (Operation* const op : from._ops) {
        if (appended.insert(op).second) {
            cone._ops.push_back(op);
        }
    }

    cone._inputs.insert(from._inputs.begin(), from._inputs.end());
}

// A cone moved into a factor still reads its constants from above the product, so the ops
// computing one after the product move ahead of it
void mapConeIntoFactors(const MaskCone& cone, CrossProduct product, ConeCache& cache, mlir::IRMapping& mapping, mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Value> constants;
    for (const Value input : cone._inputs) {
        const Value column = cache.climbFilters(input);
        if (column.getDefiningOp() == product.getOperation()) {
            const size_t resultIndex = cast<OpResult>(column).getResultNumber();
            mapping.map(input, productFactorColumn(product, resultIndex));
        } else {
            constants.push_back(input);
        }
    }

    hoistDefinitionsAhead(constants, product, rewriter);
}

bool feedsOnlyTheChain(Operation* reader,
                       const llvm::SmallPtrSetImpl<Operation*>& chain,
                       llvm::SmallPtrSetImpl<Operation*>& visited) {
    Block* const block = reader->getBlock();

    llvm::SmallVector<Operation*> pending {reader};
    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val();
        const bool seen = chain.contains(op) || !visited.insert(op).second;

        if (seen) {
            continue;
        } else if (op->getBlock() != block || !computesFromTheRow(op)) {
            return false;
        }

        llvm::append_range(pending, op->getUsers());
    }

    return true;
}

// Climbs from @param columns, the rows @param reader takes, through the filters stacked over a
// cross product down to it, and collects those filters top first. Moving work below the
// filters changes the rows of every level they stand on, so nothing but the filters, the
// masks they read and the reader may see those rows.
bool matchFilterChain(Operation* reader,
                      ValueRange columns,
                      CrossProduct& product,
                      llvm::SmallVectorImpl<FilterOp>& filters) {
    Block* const block = reader->getBlock();

    llvm::SmallPtrSet<Operation*, 8> chain {reader};
    llvm::SmallVector<Operation*, 4> levels;

    ValueRange levelColumns = columns;
    while (!product) {
        if (levelColumns.empty()) {
            return false;
        }

        Operation* const below = levelColumns.front().getDefiningOp();
        if (!below || below->getBlock() != block) {
            return false;
        }

        for (const Value column : levelColumns) {
            if (column.getDefiningOp() != below) {
                return false;
            }
        }

        levels.push_back(below);

        if (FilterOp filter = dyn_cast<FilterOp>(below)) {
            chain.insert(below);
            filters.push_back(filter);
            levelColumns = filter.getColumnsToFilter();
        } else if (CrossProduct belowProduct = dyn_cast<CrossProduct>(below)) {
            product = belowProduct;
        } else {
            return false;
        }
    }

    llvm::SmallPtrSet<Operation*, 16> visited;
    for (Operation* const level : levels) {
        for (Operation* const levelReader : level->getUsers()) {
            if (!feedsOnlyTheChain(levelReader, chain, visited)) {
                return false;
            }
        }
    }

    return true;
}

CrossProduct stackedProduct(FilterOp filter) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.empty()) {
        return nullptr;
    }

    CrossProduct product = columns.front().getDefiningOp<CrossProduct>();
    if (!product || product->getBlock() != filter->getBlock()) {
        return nullptr;
    }

    for (const Value column : columns) {
        if (column.getDefiningOp() != product.getOperation()) {
            return nullptr;
        }
    }

    return product;
}

struct UnwindPlacement {
    Unwind _unwind {nullptr};
    MaskCone _source;
    CrossProduct _product {nullptr};
    llvm::SmallVector<FilterOp, 2> _filters;
    bool _sinksLeft {false};
    llvm::SmallVector<Operation*> _rowOps;
};

bool collectCarriedRowOps(Unwind unwind, ConeCache& cache, llvm::SmallVectorImpl<Operation*>& rowOps) {
    Block* const block = unwind->getBlock();

    llvm::SmallPtrSet<Operation*, 16> visited;
    const Operation::operand_range carried = unwind.getColumnsToFilter();
    llvm::SmallVector<Value> pending(carried.begin(), carried.end());
    while (!pending.empty()) {
        const Value value = pending.pop_back_val();
        if (cache.yieldsConstantColumn(value)) {
            continue;
        }

        Operation* const def = value.getDefiningOp();
        const bool madeInTheBlock = def && def->getBlock() == block && ::db::hasNoEffectButRaising(def);

        if (!madeInTheBlock) {
            return false;
        } else if (!visited.insert(def).second) {
            continue;
        }

        rowOps.push_back(def);
        llvm::append_range(pending, def->getOperands());

        llvm::SetVector<Value> readFromAbove;
        mlir::getUsedValuesDefinedAbove(def->getRegions(), readFromAbove);
        llvm::append_range(pending, readFromAbove);
    }

    for (Operation* const rowOp : rowOps) {
        for (Operation* const user : rowOp->getUsers()) {
            Operation* const reader = block->findAncestorOpInBlock(*user);
            const bool readOutsideTheRows = reader != unwind.getOperation() && !visited.contains(reader);
            if (readOutsideTheRows) {
                return false;
            }
        }
    }

    llvm::sort(rowOps, [](Operation* lhs, Operation* rhs) {
        return lhs->isBeforeInBlock(rhs);
    });

    return true;
}

bool matchUnwindBesideItsRows(UnwindPlacement& placement, ConeCache& cache) {
    Unwind unwind = placement._unwind;

    for (const Value column : unwind.getColumnsToFilter()) {
        if (cache.yieldsConstantColumn(column) || !climbFilters(column).getDefiningOp<CrossProduct>()) {
            return false;
        }
    }

    return collectCarriedRowOps(unwind, cache, placement._rowOps);
}

bool matchUnwindIntoAFactor(UnwindPlacement& placement, ConeCache& cache) {
    Unwind unwind = placement._unwind;

    CrossProduct product;
    if (!matchFilterChain(unwind, unwind.getColumnsToFilter(), product, placement._filters)) {
        return false;
    }

    FactorReads sourceReads;
    collectFactorReads(placement._source, product, cache, sourceReads);

    const bool readsTheLeft = !sourceReads._leftResults.empty();
    const bool readsTheRight = !sourceReads._rightResults.empty();
    if (readsTheLeft == readsTheRight || sourceReads._readsElsewhere) {
        return false;
    }

    // Sunk below a filter reading the factor, the unwind would repeat the rows that filter
    // runs on once per element
    for (FilterOp filter : placement._filters) {
        const FactorReads& reads = cache.getConjunctCone(filter.getMask(), product)._reads;
        const bool readsTheFactor = readsTheLeft ? !reads._leftResults.empty() : !reads._rightResults.empty();
        if (readsTheFactor || reads._readsElsewhere) {
            return false;
        }
    }

    placement._product = product;
    placement._sinksLeft = readsTheLeft;

    return true;
}

bool matchUnwindPlacement(Unwind unwind, ConeCache& cache, UnwindPlacement& placement) {
    if (unwind.getColumnsToFilter().empty()) {
        return false;
    }

    placement._unwind = unwind;
    collectRowCone({unwind.getSource()}, unwind->getBlock(), placement._source);

    const bool readsARow = llvm::any_of(placement._source._inputs, [&cache](Value input) {
        return !cache.yieldsConstantColumn(input);
    });

    if (!readsARow) {
        return matchUnwindBesideItsRows(placement, cache);
    }

    return matchUnwindIntoAFactor(placement, cache);
}

void sinkUnwindIntoFactor(UnwindPlacement& placement, ConeCache& cache, mlir::RewriterBase& rewriter) {
    Unwind unwind = placement._unwind;
    CrossProduct product = placement._product;

    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();
    Region& factor = placement._sinksLeft ? leftFactor : rightFactor;

    const size_t leftWidth = leftFactorWidth(product);
    const size_t firstResult = placement._sinksLeft ? 0 : leftWidth;

    Yield yield = cast<Yield>(factor.front().getTerminator());
    const llvm::SmallVector<Value> yielded(yield.getColumns().begin(), yield.getColumns().end());

    mlir::IRMapping mapping;
    mapConeIntoFactors(placement._source, product, cache, mapping, rewriter);

    rewriter.setInsertionPoint(yield);
    cloneRowCone(placement._source._ops, mapping, rewriter);

    const Value unwoundElement = unwind.getElement();
    llvm::SmallVector<Type> sunkTypes {unwoundElement.getType()};
    for (const Value column : yielded) {
        sunkTypes.push_back(column.getType());
    }

    const Value source = mapping.lookupOrDefault(unwind.getSource());
    Unwind sunk = rewriter.create<Unwind>(unwind.getLoc(), sunkTypes, source, yielded);

    llvm::SmallVector<Value> widenedYield(sunk.getCarried().begin(), sunk.getCarried().end());
    widenedYield.push_back(sunk.getElement());

    rewriter.modifyOpInPlace(yield, [&yield, &widenedYield]() {
        yield->setOperands(widenedYield);
    });

    llvm::SmallVector<Type> productTypes;
    collectFactorTypes(leftFactor, rightFactor, productTypes);

    rewriter.setInsertionPoint(product);
    CrossProduct widened = rewriter.create<CrossProduct>(product.getLoc(), productTypes);
    widened.getLeftFactor().takeBody(leftFactor);
    widened.getRightFactor().takeBody(rightFactor);

    // The element follows the factor's columns, so a right factor sitting after a widened
    // left one starts a column further along
    for (const Value result : product.getResults()) {
        const size_t resultIndex = cast<OpResult>(result).getResultNumber();
        const bool shifts = placement._sinksLeft && resultIndex >= leftWidth;

        rewriter.replaceAllUsesWith(result, widened.getResult(shifts ? resultIndex + 1 : resultIndex));
    }

    Value element = widened.getResult(firstResult + yielded.size());
    for (FilterOp filter : llvm::reverse(placement._filters)) {
        element = appendCarriedColumn(filter, element, rewriter);
    }

    rewriter.replaceAllUsesWith(unwoundElement, element);

    const Operation::operand_range carried = unwind.getColumnsToFilter();
    const ResultRange carriedRows = unwind.getCarried();
    for (size_t index = 0; index < carried.size(); index++) {
        rewriter.replaceAllUsesWith(carriedRows[index], carried[index]);
    }

    rewriter.eraseOp(unwind);
    eraseUnusedRowCone(placement._source._ops, rewriter);
    rewriter.eraseOp(product);
}

void unwindBesideItsRows(UnwindPlacement& placement, mlir::RewriterBase& rewriter) {
    Unwind unwind = placement._unwind;
    const Location loc = unwind.getLoc();

    const Operation::operand_range carried = unwind.getColumnsToFilter();
    const Value unwoundElement = unwind.getElement();
    const Type elementType = unwoundElement.getType();

    const Operation::operand_range::type_range carriedTypes = carried.getTypes();
    llvm::SmallVector<Type> productTypes(carriedTypes.begin(), carriedTypes.end());
    productTypes.push_back(elementType);

    rewriter.setInsertionPoint(unwind);
    CrossProduct product = rewriter.create<CrossProduct>(loc, productTypes);

    Block* const rowsBlock = &product.getLeftFactor().front();
    rewriter.setInsertionPointToEnd(rowsBlock);
    Yield rowsYield = rewriter.create<Yield>(loc, carried);

    for (Operation* const rowOp : placement._rowOps) {
        rewriter.moveOpBefore(rowOp, rowsYield);
    }

    Block* const listBlock = &product.getRightFactor().front();
    rewriter.setInsertionPointToEnd(listBlock);

    mlir::IRMapping mapping;
    cloneRowCone(placement._source._ops, mapping, rewriter);

    const Value source = mapping.lookupOrDefault(unwind.getSource());
    Unwind listUnwind = rewriter.create<Unwind>(loc, TypeRange {elementType}, source, ValueRange {});
    rewriter.create<Yield>(loc, ValueRange {listUnwind.getElement()});

    const ResultRange carriedRows = unwind.getCarried();
    for (size_t index = 0; index < carriedRows.size(); index++) {
        rewriter.replaceAllUsesWith(carriedRows[index], product.getResult(index));
    }

    rewriter.replaceAllUsesWith(unwoundElement, product.getResult(carriedRows.size()));

    rewriter.eraseOp(unwind);
    eraseUnusedRowCone(placement._source._ops, rewriter);
}

void placeUnwind(UnwindPlacement& placement, ConeCache& cache, mlir::RewriterBase& rewriter) {
    if (placement._product) {
        sinkUnwindIntoFactor(placement, cache, rewriter);
    } else {
        unwindBesideItsRows(placement, rewriter);
    }
}

CrossProduct matchProductFactor(Region& factor) {
    Block& block = factor.front();
    if (!llvm::hasNItems(block.getOperations(), 2)) {
        return nullptr;
    }

    CrossProduct nested = dyn_cast<CrossProduct>(block.front());
    if (!nested) {
        return nullptr;
    }

    for (const Value column : factorYieldColumns(factor)) {
        if (column.getDefiningOp() != nested.getOperation()) {
            return nullptr;
        }
    }

    return nested;
}

// A product nested as one factor of another, regrouped so a conjunct reading the other
// factor and one side of the nested product reads one factor: the nested side it reads and
// the other factor are crossed first, and the nested side it does not read is crossed after
struct ProductRotation {
    bool _rotates {false};
    bool _nestedOnTheLeft {false};
    bool _keepsTheNestedLeft {false};
};

struct ProductCut {
    FilterOp _filter {nullptr};
    CrossProduct _product {nullptr};
    llvm::SmallVector<Value, 2> _leftConjuncts;
    llvm::SmallVector<Value, 2> _rightConjuncts;
    llvm::SmallVector<Value, 2> _keptConjuncts;
    MaskCone _leftCone;
    MaskCone _rightCone;
    ProductRotation _rotation;
};

bool readOneNestedSide(CrossProduct product,
                       bool nestedOnTheLeft,
                       llvm::ArrayRef<size_t> results,
                       bool& readsTheNestedLeft) {
    Region& factor = nestedOnTheLeft ? product.getLeftFactor() : product.getRightFactor();
    const CrossProduct nested = matchProductFactor(factor);
    if (!nested || results.empty()) {
        return false;
    }

    const size_t nestedLeftWidth = leftFactorWidth(nested);

    bool readsTheLeft = false;
    bool readsTheRight = false;
    for (const size_t resultIndex : results) {
        const Value column = productFactorColumn(product, resultIndex);
        const size_t nestedIndex = cast<OpResult>(column).getResultNumber();
        if (nestedIndex < nestedLeftWidth) {
            readsTheLeft = true;
        } else {
            readsTheRight = true;
        }
    }

    readsTheNestedLeft = readsTheLeft;

    return readsTheLeft != readsTheRight;
}

bool matchRotation(const FactorReads& reads, CrossProduct product, ProductRotation& rotation) {
    const bool relatesBothFactors = !reads._leftResults.empty() && !reads._rightResults.empty();
    if (!relatesBothFactors || reads._readsElsewhere) {
        return false;
    }

    bool readsTheNestedLeft = false;
    if (readOneNestedSide(product, true, reads._leftResults, readsTheNestedLeft)) {
        rotation = ProductRotation {true, true, readsTheNestedLeft};
        return true;
    } else if (readOneNestedSide(product, false, reads._rightResults, readsTheNestedLeft)) {
        rotation = ProductRotation {true, false, readsTheNestedLeft};
        return true;
    }

    return false;
}

bool matchProductCut(FilterOp filter, CrossProduct product, ConeCache& cache, ProductCut& cut) {
    cut._filter = filter;
    cut._product = product;

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    llvm::SmallPtrSet<Operation*, 16> appendedLeft;
    llvm::SmallPtrSet<Operation*, 16> appendedRight;
    for (const Value conjunct : conjuncts) {
        const ConjunctCone& conjunctCone = cache.getConjunctCone(conjunct, cut._product);
        const FactorReads& reads = conjunctCone._reads;

        const bool readsTheLeft = !reads._leftResults.empty();
        const bool readsTheRight = !reads._rightResults.empty();
        const bool readsOneFactor = readsTheLeft != readsTheRight && !reads._readsElsewhere;

        if (readsOneFactor && readsTheLeft) {
            cut._leftConjuncts.push_back(conjunct);
            appendCone(conjunctCone._cone, appendedLeft, cut._leftCone);
        } else if (readsOneFactor) {
            cut._rightConjuncts.push_back(conjunct);
            appendCone(conjunctCone._cone, appendedRight, cut._rightCone);
        } else {
            cut._keptConjuncts.push_back(conjunct);

            if (!cut._rotation._rotates) {
                matchRotation(reads, cut._product, cut._rotation);
            }
        }
    }

    const bool pushes = !cut._leftConjuncts.empty() || !cut._rightConjuncts.empty();
    if (pushes) {
        cut._rotation._rotates = false;
    }

    return pushes || cut._rotation._rotates;
}

void cutFactor(CrossProduct product,
               bool leftFactor,
               llvm::ArrayRef<Value> conjuncts,
               const MaskCone& cone,
               Location loc,
               ConeCache& cache,
               mlir::RewriterBase& rewriter) {
    if (conjuncts.empty()) {
        return;
    }

    Region& factor = leftFactor ? product.getLeftFactor() : product.getRightFactor();
    Yield yield = cast<Yield>(factor.front().getTerminator());
    const Operation::operand_range yieldColumns = yield.getColumns();
    const llvm::SmallVector<Value> yielded(yieldColumns.begin(), yieldColumns.end());
    const Operation::operand_range::type_range yieldTypes = yieldColumns.getTypes();
    const llvm::SmallVector<Type> types(yieldTypes.begin(), yieldTypes.end());

    mlir::IRMapping mapping;
    mapConeIntoFactors(cone, product, cache, mapping, rewriter);

    rewriter.setInsertionPoint(yield);
    cloneRowCone(cone._ops, mapping, rewriter);

    llvm::SmallVector<Value, 2> mappedConjuncts;
    for (const Value conjunct : conjuncts) {
        mappedConjuncts.push_back(mapping.lookup(conjunct));
    }

    const Value mask = conjoin(mappedConjuncts, loc, rewriter);
    FilterOp cut = rewriter.create<FilterOp>(loc, types, mask, yielded);

    rewriter.modifyOpInPlace(yield, [&yield, &cut]() {
        yield->setOperands(cut.getResults());
    });
}

void cutFactors(ProductCut& cut, ConeCache& cache, mlir::RewriterBase& rewriter) {
    const Location loc = cut._filter.getLoc();

    cutFactor(cut._product, true, cut._leftConjuncts, cut._leftCone, loc, cache, rewriter);
    cutFactor(cut._product, false, cut._rightConjuncts, cut._rightCone, loc, cache, rewriter);
}

void keepUncutConjuncts(ProductCut& cut, mlir::RewriterBase& rewriter) {
    FilterOp filter = cut._filter;
    const Location loc = filter.getLoc();
    const Value mask = filter.getMask();

    if (cut._keptConjuncts.empty()) {
        bypassFilter(filter);
        rewriter.eraseOp(filter);
    } else {
        rewriter.setInsertionPoint(filter);
        const Value keptMask = conjoin(cut._keptConjuncts, loc, rewriter);

        rewriter.modifyOpInPlace(filter, [&filter, keptMask]() {
            filter.getMaskMutable().assign(keptMask);
        });
    }

    eraseUnusedConjunction(mask, rewriter);
    eraseUnusedRowCone(cut._leftCone._ops, rewriter);
    eraseUnusedRowCone(cut._rightCone._ops, rewriter);
}

CrossProduct rotateProduct(CrossProduct product, const ProductRotation& rotation, mlir::RewriterBase& rewriter) {
    const Location loc = product.getLoc();

    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();
    Region& nestedFactor = rotation._nestedOnTheLeft ? leftFactor : rightFactor;
    Region& otherFactor = rotation._nestedOnTheLeft ? rightFactor : leftFactor;

    CrossProduct nested = matchProductFactor(nestedFactor);
    Region& keptFactor = rotation._keepsTheNestedLeft ? nested.getLeftFactor() : nested.getRightFactor();
    Region& restFactor = rotation._keepsTheNestedLeft ? nested.getRightFactor() : nested.getLeftFactor();

    const Operation::operand_range nestedYield = factorYieldColumns(nestedFactor);

    const size_t productLeftWidth = leftFactorWidth(product);
    const size_t nestedLeftWidth = leftFactorWidth(nested);
    const size_t keptWidth = factorYieldColumns(keptFactor).size();
    const size_t otherWidth = factorYieldColumns(otherFactor).size();

    // The other factor keeps its side, so an outer factor is not re-run once per chunk of the
    // nested side it is crossed with
    Region& innerLeft = rotation._nestedOnTheLeft ? keptFactor : otherFactor;
    Region& innerRight = rotation._nestedOnTheLeft ? otherFactor : keptFactor;
    const size_t keptFirst = rotation._nestedOnTheLeft ? 0 : otherWidth;
    const size_t otherFirstInInner = rotation._nestedOnTheLeft ? keptWidth : 0;

    llvm::SmallVector<Type> innerTypes;
    collectFactorTypes(innerLeft, innerRight, innerTypes);

    llvm::SmallVector<Type> rotatedTypes(innerTypes);
    llvm::append_range(rotatedTypes, factorYieldColumns(restFactor).getTypes());

    rewriter.setInsertionPoint(product);
    CrossProduct rotated = rewriter.create<CrossProduct>(loc, rotatedTypes);
    rotated.getRightFactor().takeBody(restFactor);

    rewriter.setInsertionPointToEnd(&rotated.getLeftFactor().front());
    CrossProduct inner = rewriter.create<CrossProduct>(loc, innerTypes);
    rewriter.create<Yield>(loc, inner.getResults());

    inner.getLeftFactor().takeBody(innerLeft);
    inner.getRightFactor().takeBody(innerRight);

    // The rotated product holds the inner product's columns, then the rest of the nested
    // product's
    const auto rotatedIndex = [&](size_t resultIndex) {
        const bool fromTheNested = (resultIndex < productLeftWidth) == rotation._nestedOnTheLeft;
        if (!fromTheNested) {
            const size_t otherFirst = rotation._nestedOnTheLeft ? productLeftWidth : 0;
            return otherFirstInInner + (resultIndex - otherFirst);
        }

        const size_t nestedFirst = rotation._nestedOnTheLeft ? 0 : productLeftWidth;
        const Value column = nestedYield[resultIndex - nestedFirst];
        const size_t nestedIndex = cast<OpResult>(column).getResultNumber();

        const bool fromTheLeft = nestedIndex < nestedLeftWidth;
        const size_t sidePosition = fromTheLeft ? nestedIndex : nestedIndex - nestedLeftWidth;
        if (fromTheLeft == rotation._keepsTheNestedLeft) {
            return keptFirst + sidePosition;
        }

        return keptWidth + otherWidth + sidePosition;
    };

    for (const Value result : product.getResults()) {
        const size_t resultIndex = cast<OpResult>(result).getResultNumber();
        rewriter.replaceAllUsesWith(result, rotated.getResult(rotatedIndex(resultIndex)));
    }

    rewriter.eraseOp(product);

    return rotated;
}

// A rotation is pushed through in the same rewrite: the filter then stands in the factor the
// rotation made, which is no bare product any more, so no other filter can rotate it back
void regroupProductCut(ProductCut& cut, ConeCache& cache, mlir::RewriterBase& rewriter) {
    const CrossProduct rotated = rotateProduct(cut._product, cut._rotation, rewriter);
    cache.clear();

    ProductCut rotatedCut;
    const bool pushes = matchProductCut(cut._filter, rotated, cache, rotatedCut) && !rotatedCut._rotation._rotates;
    bioassert(pushes, "A rotated product leaves the conjunct that rotated it on one factor");

    cutFactors(rotatedCut, cache, rewriter);
    keepUncutConjuncts(rotatedCut, rewriter);
}

// Moves into the factors of the product under @param bottom what each filter stacked over it
// reads of one factor, bottom first, in one walk up the stack. The filters are only erased once
// the walk ends, so the stack and the climbs the cache holds stay valid throughout
bool placeFilterStack(FilterOp bottom, ConeCache& cache, mlir::RewriterBase& rewriter) {
    const CrossProduct product = stackedProduct(bottom);
    if (!product) {
        return false;
    }

    std::vector<std::unique_ptr<ProductCut>> cuts;
    FilterStack stack(product);
    for (FilterOp filter = bottom; filter; filter = stack.getReader()) {
        const bool exclusive = stack.isExclusiveTo(filter);
        stack.climbTo(filter);

        if (!exclusive) {
            continue;
        }

        ProductCut& cut = *cuts.emplace_back(std::make_unique<ProductCut>());
        if (!matchProductCut(filter, product, cache, cut)) {
            cuts.pop_back();
        } else if (cut._rotation._rotates) {
            break;
        } else {
            cutFactors(cut, cache, rewriter);
        }
    }

    for (const std::unique_ptr<ProductCut>& cut : cuts) {
        if (!cut->_rotation._rotates) {
            keepUncutConjuncts(*cut, rewriter);
        }
    }

    if (!cuts.empty() && cuts.back()->_rotation._rotates) {
        regroupProductCut(*cuts.back(), cache, rewriter);
    }

    return !cuts.empty();
}

bool placeInAFactor(Operation* op, ConeCache& cache, mlir::RewriterBase& rewriter) {
    FilterOp filter = dyn_cast<FilterOp>(op);

    bool placed = false;
    UnwindPlacement placement;
    if (filter) {
        placed = placeFilterStack(filter, cache, rewriter);
    } else if (matchUnwindPlacement(cast<Unwind>(op), cache, placement)) {
        placeUnwind(placement, cache, rewriter);
        placed = true;
    }

    if (placed) {
        cache.clear();
    }

    return placed;
}

// A filter moved into a factor can let an unwind sink below the filters left over the
// product, and a sunk unwind can leave a filter over a product it then moves into: one
// worklist over both drives the two to a fixed point
struct PlaceInFactors : public impl::PlaceInFactorsBase<PlaceInFactors> {
    void runOnOperation() override {
        ConeCache cache;
        runWorklist<FilterOp, Unwind>(getOperation(), [&cache](Operation* op, mlir::RewriterBase& rewriter) {
            return placeInAFactor(op, cache, rewriter);
        });
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

// A test of labels every node a scan makes has to carry, in the mask of a filter nothing but
// which reads the scan: the scan reads the nodes carrying them alone instead
struct LabelScanChain {
    FilterOp _filter;
    Value _conjunct;
    Operation* _scan {nullptr};
    ArrayAttr _labels;
};

bool matchLabelScanConjunct(Value conjunct, Value scanned, LabelScanChain& chain) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    const ArrayAttr conjunction = check ? check.getConjunction() : ArrayAttr();
    if (!conjunction) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet || labelSet.getInputNodes() != scanned) {
        return false;
    }

    chain._conjunct = conjunct;
    chain._labels = conjunction;

    return true;
}

bool matchLabelScanChain(FilterOp filter, LabelScanChain& chain) {
    Operation* const scan = filteredSource(filter);
    if (!isa_and_nonnull<ScanNodes, ScanNodesByLabel>(scan) || !readsOnlyIntoTheFilter(scan, filter)) {
        return false;
    }

    chain._filter = filter;
    chain._scan = scan;

    const Value scanned = scan->getResult(0);

    return matchAConjunct(filter, [scanned, &chain](Value conjunct) {
        return matchLabelScanConjunct(conjunct, scanned, chain);
    });
}

void fuseScanByLabel(LabelScanChain& chain, mlir::RewriterBase& rewriter) {
    Operation* const scan = chain._scan;

    llvm::SmallVector<Attribute, 4> labels;
    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        const ArrayAttr scanLabels = scanByLabel.getLabels();
        labels.append(scanLabels.begin(), scanLabels.end());
    }

    for (const Attribute label : chain._labels) {
        if (!llvm::is_contained(labels, label)) {
            labels.push_back(label);
        }
    }

    rewriter.setInsertionPoint(scan);
    ScanNodesByLabel fused = rewriter.create<ScanNodesByLabel>(chain._filter.getLoc(),
                                                               scan->getResult(0).getType(),
                                                               rewriter.getArrayAttr(labels));

    rewriter.replaceOp(scan, fused.getResult());
    dropConjunct(chain._filter, chain._conjunct, rewriter);
}

struct FuseScanByLabel : public impl::FuseScanByLabelBase<FuseScanByLabel> {
    void runOnOperation() override {
        runFilterWorklist<LabelScanChain>(getOperation(), matchLabelScanChain, fuseScanByLabel);
    }
};

struct NodeIDScanChain {
    FilterOp _filter;
    Value _conjunct;
    ScanSource _source;
    EntityIDList _nodeIDs;
};

// A disjunction over the scanned node read after the hops and filters of the pattern
// rooted at the scan: it restricts the scan itself
bool matchPatternScanSource(FilterOp filter, Value column, LineageClimbs& climbs, OpAnswers& reaching, ScanSource& source) {
    const Value anchor = climbs.anchorOf(column);
    Operation* const scan = anchor ? anchor.getDefiningOp() : nullptr;
    const bool isNodeScan = isa_and_nonnull<ScanNodes, ScanNodesByLabel>(scan);
    if (!isNodeScan || scan->getBlock() != filter->getBlock()) {
        return false;
    }

    const bool reachesTheFilter = answerOnce(reaching, scan, [scan, &filter]() {
        return rowsReachTheFilter(scan, filter, keepsTheSourceRows);
    });

    if (!reachesTheFilter) {
        return false;
    }

    source._op = scan;
    source._column = column;
    source._labels = ArrayAttr();

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        source._labels = scanByLabel.getLabels();
    }

    return true;
}

bool matchNodeIDConjunct(FilterOp filter, Value conjunct, LineageClimbs& climbs, OpAnswers& reaching, NodeIDScanChain& chain) {
    chain._nodeIDs._literals.clear();
    chain._nodeIDs._bound.clear();
    chain._filter = filter;
    chain._conjunct = conjunct;

    const Value column = nodeDisjunctionColumn(conjunct);
    if (column) {
        return collectIDDisjunction(conjunct, column, chain._nodeIDs)
            && matchPatternScanSource(filter, column, climbs, reaching, chain._source);
    }

    // A disjunction comparing no node, every disjunct of it never true, keeps no row of the
    // scan the filter reads
    const bool listsNoNode = matchSoleScanSource(filter, chain._source)
                          && collectIDDisjunction(conjunct, chain._source._column, chain._nodeIDs);

    return listsNoNode && readsOnlyIntoTheFilter(chain._source._op, filter);
}

bool matchNodeIDScanChain(FilterOp filter, LineageClimbs& climbs, NodeIDScanChain& chain) {
    OpAnswers reaching;

    return matchAConjunct(filter, [&filter, &climbs, &reaching, &chain](Value conjunct) {
        return matchNodeIDConjunct(filter, conjunct, climbs, reaching, chain);
    });
}

// The listed nodes start the pattern in the scan's place, and every row it builds from them
// holds the disjunction
void fuseScanByNodeIDs(NodeIDScanChain& chain, mlir::RewriterBase& rewriter) {
    Operation* const scan = chain._source._op;
    const mlir::Location loc = chain._filter.getLoc();

    rewriter.setInsertionPoint(scan);
    ConstScanNodes constScan = rewriter.create<ConstScanNodes>(loc,
                                                               scan->getResult(0).getType(),
                                                               chain._nodeIDs._literals,
                                                               chain._nodeIDs._bound);
    computeBoundValuesAhead(constScan);

    llvm::SmallVector<Value> fused {constScan.getResult()};
    if (chain._source._labels) {
        keepLabelledRows(fused, chain._source._labels, loc, rewriter);
    }

    rewriter.replaceOp(scan, fused.front());
    dropConjunct(chain._filter, chain._conjunct, rewriter);
}

struct FuseScanByNodeIDs : public impl::FuseScanByNodeIDsBase<FuseScanByNodeIDs> {
    void runOnOperation() override {
        LineageClimbs climbs;

        const auto matchChain = [&climbs](FilterOp filter, NodeIDScanChain& chain) {
            return matchNodeIDScanChain(filter, climbs, chain);
        };

        const auto fuseChain = [&climbs](NodeIDScanChain& chain, mlir::RewriterBase& rewriter) {
            climbs.clear();
            fuseScanByNodeIDs(chain, rewriter);
        };

        runFilterWorklist<NodeIDScanChain>(getOperation(), matchChain, fuseChain);
    }
};

// A hop over a whole-graph node scan walks every node's edges, which is the whole edge
// set - what a scan_edges produces on its own.
bool matchWholeEdgeSetHop(Operation* hop, ScanNodes& scan) {
    // get_edges walks both directions, so over every node it reports each edge twice, once
    // from each endpoint, and the by-type hops keep one type. Only the directed untyped
    // hops read the edge set a scan_edges produces.
    if (!isa<GetOutEdges, GetInEdges>(hop)) {
        return false;
    }

    // Operand 0 is the input node column and anything after it is a carry set, row-aligned
    // with the scan and with no counterpart in a scan_edges to be rewired to.
    if (hop->getNumOperands() != 1) {
        return false;
    }

    scan = hop->getOperand(0).getDefiningOp<ScanNodes>();
    if (!scan) {
        return false;
    }

    // The fused form drops the node column, so a second reader of it keeps the scan alive.
    return scan.getResult().hasOneUse();
}

void fuseScanEdges(Operation* hop, ScanNodes scan, mlir::OpBuilder& builder) {
    builder.setInsertionPoint(hop);

    // Both directed hops fill srcids and tgtids with the edge's own source and target -
    // the direction only decided which side was walked from - so the hop's four results
    // map one-for-one onto the edge scan's, which are declared in the same order.
    ScanEdges edgeScan = builder.create<ScanEdges>(hop->getLoc(),
                                                   hop->getResult(0).getType(),
                                                   hop->getResult(1).getType(),
                                                   hop->getResult(2).getType(),
                                                   hop->getResult(3).getType());

    hop->replaceAllUsesWith(edgeScan.getOperation());
    hop->erase();
    scan.erase();
}

struct FuseScanEdges : public impl::FuseScanEdgesBase<FuseScanEdges> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first: fusing erases the hop and its scan, which would invalidate the walk.
        llvm::SmallVector<Operation*> hops;
        root->walk([&](Operation* op) {
            ScanNodes scan;
            if (matchWholeEdgeSetHop(op, scan)) {
                hops.push_back(op);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (Operation* const hop : hops) {
            ScanNodes scan;
            if (!matchWholeEdgeSetHop(hop, scan)) {
                continue;
            }

            fuseScanEdges(hop, scan, builder);
        }
    }
};

// A directed hop whose rows are then cut down to the types the pattern named: the by-type
// hop spelled the long way, since the walk itself can keep the edges of those types and
// never build the rows the filter goes on to drop.
struct TypedHop {
    FilterOp _filter;
    Value _conjunct;
    Operation* _hop {nullptr};
    ArrayAttr _edgeTypes;
};

bool matchTypedHopConjunct(Value conjunct, Operation* hop, TypedHop& typedHop) {
    constexpr size_t etypesResultIndex = 2;
    CheckEdgeTypeConstraint check = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check || check.getEdgeTypeIds() != hop->getResult(etypesResultIndex)) {
        return false;
    }

    // The hop walks the disjunction itself, so any number of required types fuses.
    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.empty()) {
        return false;
    }

    typedHop._conjunct = conjunct;
    typedHop._edgeTypes = edgeTypes;

    return true;
}

bool matchTypedHop(FilterOp filter, TypedHop& typedHop) {
    // Every column the filter cuts has to be one the hop bound, or the fused hop has nothing
    // of its own to hand back in its place. And nothing outside the filter may read the hop,
    // or that reader would go on seeing the rows the type turns away.
    Operation* const hop = filteredSource(filter);
    if (!isa_and_nonnull<GetOutEdges, GetInEdges>(hop) || !readsOnlyIntoTheFilter(hop, filter)) {
        return false;
    }

    typedHop._filter = filter;
    typedHop._hop = hop;

    return matchAConjunct(filter, [hop, &typedHop](Value conjunct) {
        return matchTypedHopConjunct(conjunct, hop, typedHop);
    });
}

template <typename ByTypeOp>
Operation* createByTypeHop(Operation* hop, ArrayAttr edgeTypes, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();

    ByTypeOp byTypeHop = builder.create<ByTypeOp>(hop->getLoc(),
                                                  results[0].getType(),
                                                  results[1].getType(),
                                                  results[2].getType(),
                                                  results[3].getType(),
                                                  results.drop_front(hopFixedResultCount).getTypes(),
                                                  hop->getOperand(0),
                                                  edgeTypes,
                                                  hop->getOperands().drop_front());

    return byTypeHop.getOperation();
}

void fuseEdgesByType(TypedHop& typedHop, mlir::RewriterBase& rewriter) {
    Operation* const hop = typedHop._hop;

    rewriter.setInsertionPoint(hop);

    // A by-type hop declares the same four fixed results and the same carry set behind them,
    // so the plain hop's results map onto it one for one.
    Operation* const byTypeHop = isa<GetOutEdges>(hop)
                                     ? createByTypeHop<GetOutEdgesByType>(hop, typedHop._edgeTypes, rewriter)
                                     : createByTypeHop<GetInEdgesByType>(hop, typedHop._edgeTypes, rewriter);

    rewriter.replaceOp(hop, byTypeHop->getResults());
    dropConjunct(typedHop._filter, typedHop._conjunct, rewriter);
}

struct FuseEdgesByType : public impl::FuseEdgesByTypeBase<FuseEdgesByType> {
    void runOnOperation() override {
        runFilterWorklist<TypedHop>(getOperation(), matchTypedHop, fuseEdgesByType);
    }
};

// An edge scan whose rows are then cut down to the types the pattern named: the by-type
// scan spelled the long way, since the scan itself can keep the edges of those types and
// never build the rows the filter goes on to drop.
struct TypedEdgeScan {
    FilterOp _filter;
    Value _conjunct;
    ScanEdges _scan;
    ArrayAttr _edgeTypes;
};

bool matchTypedEdgeScanConjunct(Value conjunct, ScanEdges scan, TypedEdgeScan& typedScan) {
    CheckEdgeTypeConstraint check = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check || check.getEdgeTypeIds() != scan.getEtypes()) {
        return false;
    }

    // As in matchTypedHop: the scan walks the disjunction itself, so any number of
    // required types fuses.
    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.empty()) {
        return false;
    }

    typedScan._conjunct = conjunct;
    typedScan._edgeTypes = edgeTypes;

    return true;
}

bool matchTypedEdgeScan(FilterOp filter, TypedEdgeScan& typedScan) {
    ScanEdges scan = dyn_cast_or_null<ScanEdges>(filteredSource(filter));
    if (!scan || !readsOnlyIntoTheFilter(scan, filter)) {
        return false;
    }

    typedScan._filter = filter;
    typedScan._scan = scan;

    return matchAConjunct(filter, [scan, &typedScan](Value conjunct) {
        return matchTypedEdgeScanConjunct(conjunct, scan, typedScan);
    });
}

void fuseScanEdgesByType(TypedEdgeScan& typedScan, mlir::RewriterBase& rewriter) {
    ScanEdges scan = typedScan._scan;

    rewriter.setInsertionPoint(scan);

    // A by-type scan declares the same four results in the same order, so the plain scan's
    // map onto it one for one.
    ScanEdgesByType byTypeScan = rewriter.create<ScanEdgesByType>(scan.getLoc(),
                                                                  scan.getSrcids().getType(),
                                                                  scan.getEids().getType(),
                                                                  scan.getEtypes().getType(),
                                                                  scan.getTgtids().getType(),
                                                                  typedScan._edgeTypes);

    rewriter.replaceOp(scan, byTypeScan->getResults());
    dropConjunct(typedScan._filter, typedScan._conjunct, rewriter);
}

struct FuseScanEdgesByType : public impl::FuseScanEdgesByTypeBase<FuseScanEdgesByType> {
    void runOnOperation() override {
        runFilterWorklist<TypedEdgeScan>(getOperation(), matchTypedEdgeScan, fuseScanEdgesByType);
    }
};

// A WHERE spelling a type disjunction reaches here as one check per type OR-ed together,
// which no by-type fusion can read. Two checks over the same type column are one check over
// the combined set - the union for an OR, the intersection for an AND - and that single
// check is what the by-type reads below can take.
struct CombinedTypeChecks {
    CheckEdgeTypeConstraint _left;
    CheckEdgeTypeConstraint _right;
};

bool matchCombinedTypeChecks(Operation* op, CombinedTypeChecks& combined) {
    if (!isa<AndOp, OrOp>(op)) {
        return false;
    }

    CheckEdgeTypeConstraint left = op->getOperand(0).getDefiningOp<CheckEdgeTypeConstraint>();
    CheckEdgeTypeConstraint right = op->getOperand(1).getDefiningOp<CheckEdgeTypeConstraint>();
    if (!left || !right) {
        return false;
    }

    // Both checks must read the same column, or they are asking about different edges
    if (left.getEdgeTypeIds() != right.getEdgeTypeIds()) {
        return false;
    }

    if (left.getNullable() != right.getNullable()) {
        return false;
    }

    combined = CombinedTypeChecks {._left = left, ._right = right};

    return true;
}

// A parameter names no edge type before the program runs: a union lists it beside the
// names, where an intersection over it has no answer
bool holdsParameter(ArrayAttr names) {
    return llvm::any_of(names, [](Attribute name) { return isa<storage::ParameterAttr>(name); });
}

void combineEdgeTypes(bool isUnion,
                      ArrayAttr left,
                      ArrayAttr right,
                      llvm::SmallVectorImpl<Attribute>& edgeTypes) {
    if (isUnion) {
        edgeTypes.assign(left.begin(), left.end());

        for (const Attribute edgeType : right) {
            if (!llvm::is_contained(edgeTypes, edgeType)) {
                edgeTypes.push_back(edgeType);
            }
        }

        return;
    }

    for (const Attribute edgeType : left) {
        if (llvm::is_contained(right, edgeType)) {
            edgeTypes.push_back(edgeType);
        }
    }
}

void fuseTypeChecks(Operation* op, const CombinedTypeChecks& combined, mlir::OpBuilder& builder) {
    const bool isUnion = isa<OrOp>(op);

    // The op wrappers are handles, so copying them out of the const match is what lets their
    // accessors be called
    CheckEdgeTypeConstraint left = combined._left;
    CheckEdgeTypeConstraint right = combined._right;

    const bool intersectsAParameter = !isUnion && (holdsParameter(left.getEdgeTypes()) || holdsParameter(right.getEdgeTypes()));
    if (intersectsAParameter) {
        return;
    }

    llvm::SmallVector<Attribute, 4> edgeTypes;
    combineEdgeTypes(isUnion, left.getEdgeTypes(), right.getEdgeTypes(), edgeTypes);

    // An edge carries one type, so an AND over two disjoint sets is a predicate no edge
    // passes. The check op has no way to spell that, so the pair stays as it is.
    if (edgeTypes.empty()) {
        return;
    }

    builder.setInsertionPoint(op);

    CheckEdgeTypeConstraint fused = builder.create<CheckEdgeTypeConstraint>(op->getLoc(),
                                                                            op->getResult(0).getType(),
                                                                            left.getEdgeTypeIds(),
                                                                            builder.getArrayAttr(edgeTypes),
                                                                            left.getNullable());

    op->getResult(0).replaceAllUsesWith(fused.getResult());
    op->erase();

    Operation* const leftOp = left.getOperation();
    Operation* const rightOp = right.getOperation();

    eraseIfUnused(leftOp);
    if (rightOp != leftOp) {
        eraseIfUnused(rightOp);
    }
}

struct FuseEdgeTypePredicates : public impl::FuseEdgeTypePredicatesBase<FuseEdgeTypePredicates> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first, as every rewrite here erases ops the walk would still visit. Program
        // order then folds a chain of ORs in one run: an operand is already one check by the
        // time the op combining it is reached.
        llvm::SmallVector<Operation*> predicates;
        root->walk([&](Operation* op) {
            if (isa<AndOp, OrOp>(op)) {
                predicates.push_back(op);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (Operation* const op : predicates) {
            CombinedTypeChecks combined;
            if (!matchCombinedTypeChecks(op, combined)) {
                continue;
            }

            fuseTypeChecks(op, combined, builder);
        }
    }
};

bool containsLabels(ArrayAttr alternative, ArrayAttr labels) {
    return llvm::all_of(labels, [alternative](Attribute label) {
        return llvm::is_contained(alternative, label);
    });
}

// An alternative asking for every label of another one keeps no node that one does not, so
// it is dropped, and of two asking for the same labels in a different order only the first stays
void keepWeakestAlternatives(llvm::ArrayRef<Attribute> alternatives, llvm::SmallVectorImpl<Attribute>& kept) {
    for (size_t index = 0; index < alternatives.size(); index++) {
        const ArrayAttr alternative = cast<ArrayAttr>(alternatives[index]);

        bool subsumed = false;
        for (size_t otherIndex = 0; otherIndex < alternatives.size() && !subsumed; otherIndex++) {
            const ArrayAttr other = cast<ArrayAttr>(alternatives[otherIndex]);
            const bool otherIsWeaker = otherIndex != index && containsLabels(alternative, other);
            const bool sameLabels = containsLabels(other, alternative);
            subsumed = otherIsWeaker && (!sameLabels || otherIndex < index);
        }

        if (!subsumed) {
            kept.push_back(alternative);
        }
    }
}

// A WHERE spelling a boolean of labels reaches here as one check per label, joined by AND and
// OR. Two checks over the same nodes are one check, whichever of the two joins them.
struct LabelCheckPair {
    CheckLabelConstraint _left;
    CheckLabelConstraint _right;
    GetNodeLabelSet _rightLabelSet;
};

bool matchLabelCheckPair(Value lhs, Value rhs, LabelCheckPair& pair) {
    CheckLabelConstraint left = lhs.getDefiningOp<CheckLabelConstraint>();
    CheckLabelConstraint right = rhs.getDefiningOp<CheckLabelConstraint>();
    if (!left || !right) {
        return false;
    }

    GetNodeLabelSet leftLabelSet = left.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    GetNodeLabelSet rightLabelSet = right.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    const bool overSameNodes = leftLabelSet && rightLabelSet && leftLabelSet.getInputNodes() == rightLabelSet.getInputNodes();
    if (!overSameNodes || left.getNullable() != right.getNullable()) {
        return false;
    }

    pair = LabelCheckPair {._left = left, ._right = right, ._rightLabelSet = rightLabelSet};

    return true;
}

void replaceWithLabelCheck(Operation* op,
                           const LabelCheckPair& pair,
                           llvm::ArrayRef<Attribute> alternatives,
                           mlir::PatternRewriter& rewriter) {
    CheckLabelConstraint left = pair._left;
    CheckLabelConstraint right = pair._right;
    GetNodeLabelSet rightLabelSet = pair._rightLabelSet;

    llvm::SmallVector<Attribute, 4> kept;
    keepWeakestAlternatives(alternatives, kept);

    Value result = op->getResult(0);
    CheckLabelConstraint fused = rewriter.create<CheckLabelConstraint>(op->getLoc(),
                                                                       result.getType(),
                                                                       left.getLabelsetIds(),
                                                                       rewriter.getArrayAttr(kept),
                                                                       left.getNullable());
    rewriter.replaceOp(op, fused.getResult());

    eraseIfUnused(left.getOperation(), rewriter);
    if (right != left) {
        eraseIfUnused(right.getOperation(), rewriter);
        eraseIfUnused(rightLabelSet.getOperation(), rewriter);
    }
}

struct FuseLabelDisjunctionPattern : public mlir::OpRewritePattern<OrOp> {
    using OpRewritePattern::OpRewritePattern;

    LogicalResult matchAndRewrite(OrOp disjunction, mlir::PatternRewriter& rewriter) const override {
        LabelCheckPair pair;
        if (!matchLabelCheckPair(disjunction.getLhs(), disjunction.getRhs(), pair)) {
            return failure();
        }

        const ArrayAttr leftAlternatives = pair._left.getAlternatives();
        llvm::SmallVector<Attribute, 4> alternatives(leftAlternatives.begin(), leftAlternatives.end());
        const ArrayAttr rightAlternatives = pair._right.getAlternatives();
        alternatives.append(rightAlternatives.begin(), rightAlternatives.end());

        replaceWithLabelCheck(disjunction, pair, alternatives, rewriter);

        return success();
    }
};

// The conjunction of two disjunctions has an alternative per pair of theirs, so a chain of
// them would grow exponentially: one side has to be a single conjunction.
bool conjoinAlternatives(ArrayAttr leftAlternatives,
                         ArrayAttr rightAlternatives,
                         mlir::Builder& builder,
                         llvm::SmallVectorImpl<Attribute>& alternatives) {
    if (leftAlternatives.size() > 1 && rightAlternatives.size() > 1) {
        return false;
    }

    for (const Attribute leftAlternative : leftAlternatives) {
        const ArrayAttr leftLabels = cast<ArrayAttr>(leftAlternative);

        for (const Attribute rightAlternative : rightAlternatives) {
            llvm::SmallVector<Attribute, 4> labels(leftLabels.begin(), leftLabels.end());
            for (const Attribute label : cast<ArrayAttr>(rightAlternative)) {
                if (!llvm::is_contained(labels, label)) {
                    labels.push_back(label);
                }
            }

            alternatives.push_back(builder.getArrayAttr(labels));
        }
    }

    return true;
}

struct FuseLabelConjunctionPattern : public mlir::OpRewritePattern<AndOp> {
    using OpRewritePattern::OpRewritePattern;

    LogicalResult matchAndRewrite(AndOp conjunction, mlir::PatternRewriter& rewriter) const override {
        LabelCheckPair pair;
        if (!matchLabelCheckPair(conjunction.getLhs(), conjunction.getRhs(), pair)) {
            return failure();
        }

        llvm::SmallVector<Attribute, 4> alternatives;
        if (!conjoinAlternatives(pair._left.getAlternatives(), pair._right.getAlternatives(), rewriter, alternatives)) {
            return failure();
        }

        replaceWithLabelCheck(conjunction, pair, alternatives, rewriter);

        return success();
    }
};

// Codegen filters once per conjunct, so a WHERE ANDing labels reaches here as a label filter
// over the rows of another one, testing the same nodes.
struct StackedLabelFilters {
    FilterOp _outer;
    FilterOp _inner;
    CheckLabelConstraint _innerCheck;
    CheckLabelConstraint _outerCheck;
    GetNodeLabelSet _outerLabelSet;
    llvm::SmallVector<Attribute, 4> _alternatives;
};

bool matchStackedLabelFilters(FilterOp outer, StackedLabelFilters& stacked) {
    CheckLabelConstraint outerCheck = outer.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!outerCheck) {
        return false;
    }

    GetNodeLabelSet outerLabelSet = outerCheck.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!outerLabelSet) {
        return false;
    }

    const Value checkedColumn = outerLabelSet.getInputNodes();
    FilterOp inner = checkedColumn.getDefiningOp<FilterOp>();
    if (!inner) {
        return false;
    }

    CheckLabelConstraint innerCheck = inner.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!innerCheck) {
        return false;
    }

    GetNodeLabelSet innerLabelSet = innerCheck.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    const size_t checkedIndex = cast<OpResult>(checkedColumn).getResultNumber();
    const bool overSameNodes = innerLabelSet && innerLabelSet.getInputNodes() == inner.getColumnsToFilter()[checkedIndex];
    const bool inSameBlock = outer->getBlock() == inner->getBlock();
    if (!overSameNodes || !inSameBlock) {
        return false;
    }

    for (const Value column : outer.getColumnsToFilter()) {
        if (column.getDefiningOp() != inner.getOperation()) {
            return false;
        }
    }

    // Moving the outer check onto the inner filter cuts the inner filter's rows, so nothing
    // but the outer filter and its check may read them.
    Operation* const outerOp = outer.getOperation();
    Operation* const outerLabelSetOp = outerLabelSet.getOperation();
    for (const Value result : inner->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsTheChain = user == outerOp || user == outerLabelSetOp;
            if (!readsTheChain) {
                return false;
            }
        }
    }

    const bool chainIsPrivate = outerLabelSet.getResult().hasOneUse() && outerCheck.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    mlir::Builder builder(outer.getContext());
    llvm::SmallVector<Attribute, 4> alternatives;
    if (!conjoinAlternatives(innerCheck.getAlternatives(), outerCheck.getAlternatives(), builder, alternatives)) {
        return false;
    }

    stacked._outer = outer;
    stacked._inner = inner;
    stacked._innerCheck = innerCheck;
    stacked._outerCheck = outerCheck;
    stacked._outerLabelSet = outerLabelSet;
    keepWeakestAlternatives(alternatives, stacked._alternatives);

    return true;
}

void fuseStackedLabelFilters(StackedLabelFilters& stacked, mlir::RewriterBase& rewriter) {
    FilterOp inner = stacked._inner;
    CheckLabelConstraint innerCheck = stacked._innerCheck;
    const bool nullable = innerCheck.getNullable() || stacked._outerCheck.getNullable();

    rewriter.setInsertionPoint(inner);
    CheckLabelConstraint fused = rewriter.create<CheckLabelConstraint>(innerCheck.getLoc(),
                                                                       innerCheck.getResult().getType(),
                                                                       innerCheck.getLabelsetIds(),
                                                                       rewriter.getArrayAttr(stacked._alternatives),
                                                                       nullable);

    rewriter.modifyOpInPlace(inner, [&inner, &fused]() {
        inner.getMaskMutable().assign(fused.getResult());
    });

    FilterOp outer = stacked._outer;
    rewriter.replaceOp(outer, outer.getColumnsToFilter());

    eraseIfUnused(stacked._outerCheck.getOperation(), rewriter);
    eraseIfUnused(stacked._outerLabelSet.getOperation(), rewriter);
    eraseIfUnused(innerCheck.getOperation(), rewriter);
}

// A hop repeats each node it walks from, and each column it carries, once per edge, so a label
// filter over one of those columns keeps the rows of the nodes it would keep above the hop.
struct LabelFilterBelowHop {
    FilterOp _filter;
    Operation* _hop {nullptr};
    CheckLabelConstraint _check;
    GetNodeLabelSet _labelSet;
    Value _hopOperand;
};

bool matchLabelFilterBelowHop(FilterOp filter, LabelFilterBelowHop& below) {
    CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    const Value checkedColumn = labelSet.getInputNodes();
    Operation* const hop = checkedColumn.getDefiningOp();
    if (!hop || !isEdgeHop(hop) || hop->getBlock() != filter->getBlock()) {
        return false;
    }

    const size_t resultIndex = cast<OpResult>(checkedColumn).getResultNumber();
    Value hopOperand;
    if (resultIndex == hopInputResult(hop)) {
        hopOperand = hop->getOperand(0);
    } else if (resultIndex >= hopFixedResultCount) {
        hopOperand = hop->getOperand(1 + (resultIndex - hopFixedResultCount));
    } else {
        return false;
    }

    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != hop) {
            return false;
        }
    }

    Operation* const filterOp = filter.getOperation();
    Operation* const labelSetOp = labelSet.getOperation();
    for (const Value result : hop->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsTheChain = user == filterOp || user == labelSetOp;
            if (!readsTheChain) {
                return false;
            }
        }
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    below = LabelFilterBelowHop {._filter = filter,
                                 ._hop = hop,
                                 ._check = check,
                                 ._labelSet = labelSet,
                                 ._hopOperand = hopOperand};

    return true;
}

void hoistLabelFilterAboveHop(LabelFilterBelowHop& below, mlir::RewriterBase& rewriter) {
    Operation* const hop = below._hop;
    CheckLabelConstraint check = below._check;
    GetNodeLabelSet labelSet = below._labelSet;
    const Location loc = check.getLoc();

    rewriter.setInsertionPoint(hop);
    GetNodeLabelSet hoistedLabelSet = rewriter.create<GetNodeLabelSet>(loc,
                                                                       labelSet.getResult().getType(),
                                                                       below._hopOperand);
    CheckLabelConstraint hoistedCheck = rewriter.create<CheckLabelConstraint>(loc,
                                                                              check.getResult().getType(),
                                                                              hoistedLabelSet.getResult(),
                                                                              check.getAlternatives(),
                                                                              check.getNullable());

    const Operation::operand_range hopOperands = hop->getOperands();
    FilterOp hoistedFilter = rewriter.create<FilterOp>(loc,
                                                       hopOperands.getTypes(),
                                                       hoistedCheck.getResult(),
                                                       hopOperands);

    const mlir::ResultRange filteredOperands = hoistedFilter.getFilteredColumns();
    rewriter.modifyOpInPlace(hop, [hop, &filteredOperands]() {
        hop->setOperands(filteredOperands);
    });

    FilterOp filter = below._filter;
    rewriter.replaceOp(filter, filter.getColumnsToFilter());

    eraseIfUnused(check.getOperation(), rewriter);
    eraseIfUnused(labelSet.getOperation(), rewriter);
}

struct FuseLabelPredicates : public impl::FuseLabelPredicatesBase<FuseLabelPredicates> {
    void runOnOperation() override {
        MLIRContext* const context = &getContext();
        Operation* const root = getOperation();

        llvm::SmallVector<Operation*> predicates;
        root->walk([&predicates](Operation* op) {
            if (isa<AndOp, OrOp>(op)) {
                predicates.push_back(op);
            }
        });

        mlir::RewritePatternSet patterns(context);
        patterns.add<FuseLabelDisjunctionPattern, FuseLabelConjunctionPattern>(context);
        const mlir::FrozenRewritePatternSet frozenPatterns(std::move(patterns));

        // Kept to the ANDs, the ORs and the checks fused from them: the driver's folding and
        // region simplification would otherwise rewrite the rest of the program as well.
        mlir::GreedyRewriteConfig config;
        config.setStrictness(mlir::GreedyRewriteStrictness::ExistingAndNewOps);
        config.enableFolding(false);
        config.enableConstantCSE(false);
        config.setRegionSimplificationLevel(mlir::GreedySimplifyRegionLevel::Disabled);

        if (failed(mlir::applyOpPatternsGreedily(predicates, frozenPatterns, config))) {
            signalPassFailure();
            return;
        }

        runFilterWorklist<LabelFilterBelowHop>(root, matchLabelFilterBelowHop, hoistLabelFilterAboveHop);
        runFilterWorklist<StackedLabelFilters>(root, matchStackedLabelFilters, fuseStackedLabelFilters);
    }
};

// A type check over a read that already narrows by type is that read's types ANDed with the
// check's: the rows that survive are the ones both keep. Folding the check into the read
// leaves the walk to skip everything else, and the check goes. A check the read already
// guarantees is the case where the intersection changes nothing, so only the check goes.
struct TypeChecksOverReads {
    FilterOp _filter;
    llvm::SmallVector<Value, 2> _conjuncts;
    Operation* _narrowedRead {nullptr};
    ArrayAttr _narrowedTypes;
};

// The by-type read a column comes off, or null for a column no read constrains. The by-type
// reads declare four columns and only the edge type one is constrained, so which result the
// value is matters as much as which op produced it.
Operation* byTypeReadOf(Value column) {
    Operation* const producer = column.getDefiningOp();
    if (!producer || !isa<ScanEdgesByType, GetOutEdgesByType, GetInEdgesByType, GetOutEdgesByTypeAndLabel, GetInEdgesByTypeAndLabel>(producer)) {
        return nullptr;
    }

    constexpr size_t etypesResultIndex = 2;
    if (column != producer->getResult(etypesResultIndex)) {
        return nullptr;
    }

    return producer;
}

// Narrowing a read changes what every reader of it sees, so the one read a filter narrows is
// the one making every column it cuts, which nothing but the filter reads
Operation* narrowableReadOf(FilterOp filter) {
    constexpr size_t etypesResultIndex = 2;
    Operation* const source = filteredSource(filter);
    const bool readsByType = source && source->getNumResults() > etypesResultIndex
                             && byTypeReadOf(source->getResult(etypesResultIndex)) == source;

    return readsByType && readsOnlyIntoTheFilter(source, filter) ? source : nullptr;
}

bool matchTypeChecksOverReads(FilterOp filter, TypeChecksOverReads& matched) {
    Operation* const narrowable = narrowableReadOf(filter);
    ArrayAttr narrowedTypes = narrowable ? narrowable->getAttrOfType<ArrayAttr>("edge_types") : ArrayAttr();

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        CheckEdgeTypeConstraint check = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();
        Operation* const read = check ? byTypeReadOf(check.getEdgeTypeIds()) : nullptr;
        if (!read) {
            continue;
        }

        const ArrayAttr readTypes = read == narrowable ? narrowedTypes : read->getAttrOfType<ArrayAttr>("edge_types");
        const ArrayAttr checkTypes = check.getEdgeTypes();

        const bool readIsWithinCheck = llvm::all_of(readTypes, [checkTypes](Attribute edgeType) {
            return llvm::is_contained(checkTypes, edgeType);
        });

        const bool holdsAParameter = holdsParameter(readTypes) || holdsParameter(checkTypes);
        if (holdsAParameter && !readIsWithinCheck) {
            continue;
        }

        // An empty intersection is a read that matches no edge, which an empty type set on the
        // read says outright: the loop is then unmatchable and walks nothing.
        // The intersection is a subset of the read's types, so a smaller one is a narrower read
        llvm::SmallVector<Attribute, 4> intersection;
        combineEdgeTypes(false, readTypes, checkTypes, intersection);

        if (intersection.size() < readTypes.size()) {
            const bool narrowsTheRead = read == narrowable && check.getResult().hasOneUse();
            if (!narrowsTheRead) {
                continue;
            }

            narrowedTypes = ArrayAttr::get(filter.getContext(), intersection);
            matched._narrowedRead = read;
            matched._narrowedTypes = narrowedTypes;
        }

        matched._conjuncts.push_back(conjunct);
    }

    matched._filter = filter;

    return !matched._conjuncts.empty();
}

void foldTypeChecksIntoReads(TypeChecksOverReads& matched, mlir::RewriterBase& rewriter) {
    Operation* const read = matched._narrowedRead;

    if (read) {
        const ArrayAttr narrowedTypes = matched._narrowedTypes;
        rewriter.modifyOpInPlace(read, [read, narrowedTypes]() {
            read->setAttr("edge_types", narrowedTypes);
        });
    }

    dropConjuncts(matched._filter, matched._conjuncts, rewriter);
}

struct NarrowEdgeTypeReads : public impl::NarrowEdgeTypeReadsBase<NarrowEdgeTypeReads> {
    void runOnOperation() override {
        runFilterWorklist<TypeChecksOverReads>(getOperation(), matchTypeChecksOverReads, foldTypeChecksIntoReads);
    }
};

// A directed hop over a by-label node scan walks the edges hanging off every node carrying
// those labels, which is what the matching by-label edge scan reads off the edge index -
// without building the node column the hop walks from.
template <typename HopOp>
bool matchLabelledEdgeScan(HopOp hop, ScanNodesByLabel& scan) {
    // The carry set is row-aligned with the scan and the fused form has nothing of its own
    // to hand back in its place.
    if (!hop.getColumnsToFilter().empty()) {
        return false;
    }

    scan = hop.getInputNodes().template getDefiningOp<ScanNodesByLabel>();
    if (!scan) {
        return false;
    }

    // The fused form drops the node column, so a second reader of it keeps the scan alive.
    return scan.getResult().hasOneUse();
}

template <typename ScanOp, typename HopOp>
void fuseScanEdgesByLabel(HopOp hop, ScanNodesByLabel scan, mlir::OpBuilder& builder) {
    builder.setInsertionPoint(hop);

    // The fused scan declares the same four results in the same order the hop declares its
    // fixed ones, so they map one for one. Both hold the edge as the graph stores it, so
    // the direction changes which edges are produced, not which column holds what.
    ScanOp edgeScan = builder.create<ScanOp>(hop.getLoc(),
                                             hop.getSrcids().getType(),
                                             hop.getEids().getType(),
                                             hop.getEtypes().getType(),
                                             hop.getTgtids().getType(),
                                             scan.getLabelsAttr());

    Operation* const hopOp = hop.getOperation();
    hopOp->replaceAllUsesWith(edgeScan.getOperation());
    hopOp->erase();
    scan.erase();
}

template <typename ScanOp, typename HopOp>
void runScanEdgesByLabelPass(Operation* root, mlir::OpBuilder& builder) {
    // Collect first: fusing erases the hop and its scan, which would invalidate the walk.
    llvm::SmallVector<HopOp> hops;
    root->walk([&](HopOp hop) {
        ScanNodesByLabel scan;
        if (matchLabelledEdgeScan(hop, scan)) {
            hops.push_back(hop);
        }
    });

    for (HopOp hop : hops) {
        ScanNodesByLabel scan;
        if (!matchLabelledEdgeScan(hop, scan)) {
            continue;
        }

        fuseScanEdgesByLabel<ScanOp>(hop, scan, builder);
    }
}

struct FuseScanOutEdgesByLabel : public impl::FuseScanOutEdgesByLabelBase<FuseScanOutEdgesByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runScanEdgesByLabelPass<ScanOutEdgesByLabelSrc, GetOutEdges>(getOperation(), builder);
    }
};

struct FuseScanInEdgesByLabel : public impl::FuseScanInEdgesByLabelBase<FuseScanInEdgesByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runScanEdgesByLabelPass<ScanInEdgesByLabelTgt, GetInEdges>(getOperation(), builder);
    }
};

// The label set a scan's label names stand for, resolved against the schema, and whether
// the schema holds every one of them.
bool collectScanLabels(ArrayAttr labelNames, const ::db::GraphMetadata& metadata, ::db::LabelSet& labels) {
    const ::db::LabelMap& labelMap = metadata.labels();
    bool resolvesEveryLabel = true;
    for (const Attribute labelAttr : labelNames) {
        const StringAttr nameAttr = dyn_cast<StringAttr>(labelAttr);
        if (!nameAttr) {
            continue;
        }

        const llvm::StringRef name = nameAttr.getValue();
        const std::optional<::db::LabelID> label = labelMap.get(std::string_view(name.data(), name.size()));

        if (label) {
            labels.set(*label);
        } else {
            resolvesEveryLabel = false;
        }
    }

    return resolvesEveryLabel;
}

// The edges the index keyed by one end's labels holds under these labels, which is what a
// scan of it walks, and none when the schema lacks one of them.
size_t estimateIndexedEdges(ArrayAttr labelNames,
                            bool indexedByTarget,
                            const ::db::GraphMetadata& metadata,
                            const ::db::CardinalityEstimation& estimation) {
    ::db::LabelSet labels;
    if (!collectScanLabels(labelNames, metadata, labels)) {
        return 0;
    } else if (indexedByTarget) {
        return estimation.estimateInEdgeCount(labels);
    }

    return estimation.estimateOutEdgeCount(labels);
}

// The index a scan labelled on both ends walks: the one holding fewer edges under its end's
// labels, the source's on a tie. With no graph to count, the one the scan already walks.
bool indexesByTarget(const EdgeScanLabels& scanLabels, const DBPassContext& context) {
    if (!context._view) {
        return scanLabels._indexedByTarget;
    }

    const ::db::GraphView& view = *context._view;
    const ::db::GraphMetadata& metadata = view.metadata();
    const ::db::CardinalityEstimation estimation(view);

    const size_t sourceEdges = estimateIndexedEdges(scanLabels._source, false, metadata, estimation);
    const size_t targetEdges = estimateIndexedEdges(scanLabels._target, true, metadata, estimation);

    return targetEdges < sourceEdges;
}

// An edge scan whose rows are then cut down to the edges one endpoint of which carries a
// set of labels: the scan reading that endpoint by those labels as well, which never builds
// the rows the filter goes on to drop.
struct EndpointLabelledEdgeScan {
    FilterOp _filter;
    Value _conjunct;
    Operation* _scan {nullptr};
    EdgeScanLabels _scanLabels;
    llvm::SmallVector<Attribute, 4> _endLabels;
    bool _labelledTarget {false};
    bool _endWasLabelled {false};
};

bool matchEndpointLabelledEdgeScanConjunct(Value conjunct, EndpointLabelledEdgeScan& labelledScan) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr checkLabels = check.getConjunction();
    if (!checkLabels) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    constexpr size_t SRC_RESULT_INDEX = 0;
    constexpr size_t TGT_RESULT_INDEX = 3;
    Operation* const scan = labelledScan._scan;
    const Value labelledColumn = labelSet.getInputNodes();
    const bool labelledTarget = labelledColumn == scan->getResult(TGT_RESULT_INDEX);
    const bool labelledSource = labelledColumn == scan->getResult(SRC_RESULT_INDEX);
    if (!labelledTarget && !labelledSource) {
        return false;
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    const EdgeScanLabels& scanLabels = labelledScan._scanLabels;
    const ArrayAttr endLabels = labelledTarget ? scanLabels._target : scanLabels._source;

    labelledScan._endLabels.clear();
    if (endLabels) {
        labelledScan._endLabels.assign(endLabels.begin(), endLabels.end());
    }

    for (const Attribute label : checkLabels) {
        if (!llvm::is_contained(labelledScan._endLabels, label)) {
            labelledScan._endLabels.push_back(label);
        }
    }

    labelledScan._conjunct = conjunct;
    labelledScan._labelledTarget = labelledTarget;
    labelledScan._endWasLabelled = endLabels != nullptr;

    return true;
}

bool matchEndpointLabelledEdgeScan(FilterOp filter, EndpointLabelledEdgeScan& labelledScan) {
    Operation* const scan = filteredSource(filter);
    if (!scan || !readEdgeScanLabels(scan, labelledScan._scanLabels) || !readsOnlyIntoTheFilter(scan, filter)) {
        return false;
    }

    labelledScan._filter = filter;
    labelledScan._scan = scan;

    return matchAConjunct(filter, [&labelledScan](Value conjunct) {
        return matchEndpointLabelledEdgeScanConjunct(conjunct, labelledScan);
    });
}

// The by-label edge scan reading the endpoints by these labels. A scan labelled on one end
// walks that end's index; one labelled on both walks the index _indexedByTarget names.
Operation* createLabelledEdgeScan(Operation* scan, const EdgeScanLabels& scanLabels, mlir::RewriterBase& rewriter) {
    const Location loc = scan->getLoc();
    const mlir::TypeRange types = scan->getResultTypes();

    if (!scanLabels._source) {
        return rewriter.create<ScanOutEdgesByLabelTgt>(loc, types, scanLabels._target);
    } else if (!scanLabels._target) {
        return rewriter.create<ScanInEdgesByLabelSrc>(loc, types, scanLabels._source);
    } else if (scanLabels._indexedByTarget) {
        return rewriter.create<ScanInEdgesByLabelSrcTgt>(loc, types, scanLabels._source, scanLabels._target);
    }

    return rewriter.create<ScanOutEdgesByLabelSrcTgt>(loc, types, scanLabels._source, scanLabels._target);
}

void fuseScanEdgesByEndpointLabel(EndpointLabelledEdgeScan& labelledScan,
                                  const DBPassContext& context,
                                  mlir::RewriterBase& rewriter) {
    Operation* const scan = labelledScan._scan;
    const ArrayAttr endLabels = rewriter.getArrayAttr(labelledScan._endLabels);

    EdgeScanLabels& scanLabels = labelledScan._scanLabels;
    if (labelledScan._labelledTarget) {
        scanLabels._target = endLabels;
    } else {
        scanLabels._source = endLabels;
    }

    const bool labelsBothEnds = scanLabels._source && scanLabels._target;
    if (labelsBothEnds) {
        scanLabels._indexedByTarget = indexesByTarget(scanLabels, context);
    }

    if (labelledScan._endWasLabelled && !labelsBothEnds) {
        rewriter.modifyOpInPlace(scan, [scan, endLabels]() {
            scan->setAttr("labels", endLabels);
        });
    } else {
        rewriter.setInsertionPoint(scan);
        Operation* const labelledEdgeScan = createLabelledEdgeScan(scan, scanLabels, rewriter);
        rewriter.replaceOp(scan, labelledEdgeScan->getResults());
    }

    dropConjunct(labelledScan._filter, labelledScan._conjunct, rewriter);
}

struct FuseScanEdgesByEndpointLabel : public impl::FuseScanEdgesByEndpointLabelBase<FuseScanEdgesByEndpointLabel> {
    FuseScanEdgesByEndpointLabel() {}

    FuseScanEdgesByEndpointLabel(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        const DBPassContext& context = *_context;
        const auto fuse = [&context](EndpointLabelledEdgeScan& labelledScan, mlir::RewriterBase& rewriter) {
            fuseScanEdgesByEndpointLabel(labelledScan, context, rewriter);
        };

        runFilterWorklist<EndpointLabelledEdgeScan>(getOperation(), matchEndpointLabelledEdgeScan, fuse);
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

// A hop whose rows are then cut down to the edges the endpoint it reaches carries a set of
// labels on: the by-label hop spelled the long way, since the walk itself can keep those
// edges and never build the rows the filter goes on to drop.
struct EndpointLabelledHop {
    FilterOp _filter;
    Value _conjunct;
    Operation* _hop {nullptr};
    ArrayAttr _labels;
};

bool matchEndpointLabelledHopConjunct(Value conjunct, Operation* hop, EndpointLabelledHop& labelledHop) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr labels = check.getConjunction();
    if (!labels) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    // Only the end the hop reaches can ride onto it. The end it leaves is the input column,
    // which a by-label node scan constrains instead - and there the labels are no longer
    // this hop's to carry.
    constexpr size_t srcResultIndex = 0;
    constexpr size_t tgtResultIndex = 3;
    const size_t reachedResultIndex = isReverseHop(hop) ? srcResultIndex : tgtResultIndex;
    if (labelSet.getInputNodes() != hop->getResult(reachedResultIndex)) {
        return false;
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    labelledHop._conjunct = conjunct;
    labelledHop._labels = labels;

    return true;
}

bool matchEndpointLabelledHop(FilterOp filter, EndpointLabelledHop& labelledHop) {
    // Every column the filter cuts has to be one the hop bound, or the fused hop has nothing
    // of its own to hand back in its place. And nothing outside the filter may read the hop,
    // or that reader would go on seeing the rows the labels turn away.
    Operation* const hop = filteredSource(filter);
    const bool fusesLabels = isa_and_nonnull<GetOutEdges, GetInEdges, GetOutEdgesByType, GetInEdgesByType>(hop);
    if (!fusesLabels || !readsOnlyIntoTheFilter(hop, filter)) {
        return false;
    }

    labelledHop._filter = filter;
    labelledHop._hop = hop;

    return matchAConjunct(filter, [hop, &labelledHop](Value conjunct) {
        return matchEndpointLabelledHopConjunct(conjunct, hop, labelledHop);
    });
}

template <typename ByLabelOp>
Operation* createByLabelHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();

    ByLabelOp byLabelHop = builder.create<ByLabelOp>(hop->getLoc(),
                                                     results[0].getType(),
                                                     results[1].getType(),
                                                     results[2].getType(),
                                                     results[3].getType(),
                                                     results.drop_front(hopFixedResultCount).getTypes(),
                                                     hop->getOperand(0),
                                                     labels,
                                                     hop->getOperands().drop_front());

    return byLabelHop.getOperation();
}

template <typename ByTypeAndLabelOp>
Operation* createByTypeAndLabelHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();
    const ArrayAttr edgeTypes = hop->getAttrOfType<ArrayAttr>("edge_types");

    ByTypeAndLabelOp byTypeAndLabelHop = builder.create<ByTypeAndLabelOp>(hop->getLoc(),
                                                                          results[0].getType(),
                                                                          results[1].getType(),
                                                                          results[2].getType(),
                                                                          results[3].getType(),
                                                                          results.drop_front(hopFixedResultCount).getTypes(),
                                                                          hop->getOperand(0),
                                                                          edgeTypes,
                                                                          labels,
                                                                          hop->getOperands().drop_front());

    return byTypeAndLabelHop.getOperation();
}

Operation* createEndpointLabelledHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    if (isa<GetOutEdges>(hop)) {
        return createByLabelHop<GetOutEdgesByLabel>(hop, labels, builder);
    } else if (isa<GetInEdges>(hop)) {
        return createByLabelHop<GetInEdgesByLabel>(hop, labels, builder);
    } else if (isa<GetOutEdgesByType>(hop)) {
        return createByTypeAndLabelHop<GetOutEdgesByTypeAndLabel>(hop, labels, builder);
    } else {
        return createByTypeAndLabelHop<GetInEdgesByTypeAndLabel>(hop, labels, builder);
    }
}

void fuseEdgesByEndpointLabel(EndpointLabelledHop& labelledHop, mlir::RewriterBase& rewriter) {
    Operation* const hop = labelledHop._hop;

    rewriter.setInsertionPoint(hop);

    // A by-label hop declares the same four fixed results and the same carry set behind
    // them, so the plain hop's results map onto it one for one. Which one it becomes is the
    // hop the query wrote: an out-hop reaches its target, an in-hop its source, and a by-type
    // hop keeps its types.
    Operation* const byLabelHop = createEndpointLabelledHop(hop, labelledHop._labels, rewriter);

    rewriter.replaceOp(hop, byLabelHop->getResults());
    dropConjunct(labelledHop._filter, labelledHop._conjunct, rewriter);
}

struct FuseEdgesByEndpointLabel : public impl::FuseEdgesByEndpointLabelBase<FuseEdgesByEndpointLabel> {
    void runOnOperation() override {
        runFilterWorklist<EndpointLabelledHop>(getOperation(), matchEndpointLabelledHop, fuseEdgesByEndpointLabel);
    }
};

// A label check over nodes already known to carry every label of one of its alternatives
// keeps every row.
struct RedundantLabelChecks {
    FilterOp _filter;
    llvm::SmallVector<Value, 2> _conjuncts;
};

bool isRedundantLabelCheck(Value conjunct) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    llvm::DenseSet<Attribute> knownLabels;
    collectKnownLabels(labelSet.getInputNodes(), knownLabels);

    const auto isKnown = [&knownLabels](Attribute label) {
        return knownLabels.contains(label);
    };

    const auto isGuaranteed = [&isKnown](Attribute alternative) {
        return llvm::all_of(cast<ArrayAttr>(alternative), isKnown);
    };

    return llvm::any_of(check.getAlternatives(), isGuaranteed);
}

bool matchRedundantLabelChecks(FilterOp filter, RedundantLabelChecks& redundant) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    redundant._filter = filter;
    llvm::copy_if(conjuncts, std::back_inserter(redundant._conjuncts), isRedundantLabelCheck);

    return !redundant._conjuncts.empty();
}

void removeRedundantLabelChecks(RedundantLabelChecks& redundant, mlir::RewriterBase& rewriter) {
    dropConjuncts(redundant._filter, redundant._conjuncts, rewriter);
}

struct RemoveRedundantLabelChecks : public impl::RemoveRedundantLabelChecksBase<RemoveRedundantLabelChecks> {
    void runOnOperation() override {
        runFilterWorklist<RedundantLabelChecks>(getOperation(), matchRedundantLabelChecks, removeRedundantLabelChecks);
    }
};

// A path exploration whose rows are then cut down to those ending on labelled nodes: the
// end constraint spelled the long way, since the walk itself can keep to those ends and
// never build the rows the filter goes on to drop.
struct EndConstrainedExploration {
    FilterOp _filter;
    ExplorePaths _exploration;
    llvm::SmallVector<Value, 2> _conjuncts;
    llvm::SmallVector<Attribute, 4> _labels;
};

// The labels @param conjunct asks of every end of @param exploration, or null
ArrayAttr endLabelsAsked(Value conjunct, ExplorePaths exploration) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    GetNodeLabelSet labelSet = check ? check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>() : GetNodeLabelSet();
    if (!labelSet || labelSet.getInputNodes() != exploration.getTgtids()) {
        return ArrayAttr();
    }

    return check.getConjunction();
}

bool matchEndConstrainedExploration(FilterOp filter, EndConstrainedExploration& constrained) {
    // Every column the filter cuts has to be one the exploration bound, or the constrained
    // exploration has nothing of its own to hand back in its place. And nothing outside the
    // filter may read the exploration, or that reader would go on seeing the rows the end
    // labels turn away.
    ExplorePaths exploration = dyn_cast_or_null<ExplorePaths>(filteredSource(filter));
    if (!exploration || !readsOnlyIntoTheFilter(exploration, filter)) {
        return false;
    }

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        const ArrayAttr labels = endLabelsAsked(conjunct, exploration);
        if (!labels) {
            continue;
        }

        constrained._conjuncts.push_back(conjunct);
        for (const Attribute label : labels) {
            if (!llvm::is_contained(constrained._labels, label)) {
                constrained._labels.push_back(label);
            }
        }
    }

    constrained._filter = filter;
    constrained._exploration = exploration;

    return !constrained._conjuncts.empty();
}

// The end labels the exploration already carries joined with the filter's, each once
ArrayAttr mergedEndLabels(ExplorePaths exploration, ArrayAttr filterLabels, mlir::OpBuilder& builder) {
    const std::optional<ArrayAttr> current = exploration.getEndLabels();
    if (!current) {
        return filterLabels;
    }

    llvm::SmallVector<Attribute> merged(current->begin(), current->end());
    for (const Attribute label : filterLabels) {
        if (!llvm::is_contained(merged, label)) {
            merged.push_back(label);
        }
    }

    return builder.getArrayAttr(merged);
}

void fuseExploreEndConstraint(EndConstrainedExploration& constrained, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = constrained._exploration;
    const ArrayAttr endLabels = mergedEndLabels(exploration, rewriter.getArrayAttr(constrained._labels), rewriter);

    rewriter.modifyOpInPlace(exploration, [&exploration, endLabels]() {
        exploration.setEndLabelsAttr(endLabels);
    });

    dropConjuncts(constrained._filter, constrained._conjuncts, rewriter);
}

struct FuseExploreEndConstraint : public impl::FuseExploreEndConstraintBase<FuseExploreEndConstraint> {
    void runOnOperation() override {
        runFilterWorklist<EndConstrainedExploration>(getOperation(), matchEndConstrainedExploration, fuseExploreEndConstraint);
    }
};

// A conjunct of a hop region's yield that only asks the hop's end node for labels
CheckLabelConstraint endLabelCheckOf(Value conjunct, BlockArgument end) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check || !conjunct.hasOneUse()) {
        return {};
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    const bool readsTheEnd = labelSet && labelSet.getInputNodes() == end;
    const bool isConjunction = static_cast<bool>(check.getConjunction());

    return readsTheEnd && isConjunction ? check : CheckLabelConstraint {};
}

// The conjuncts of a hop region's yield, and the `and` ops joining them, each read by its
// parent alone, listed parent first
void collectHopConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts, llvm::SmallVectorImpl<AndOp>& joins) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value conjunct = pending.pop_back_val();

        AndOp conjunction = conjunct.getDefiningOp<AndOp>();
        if (conjunction && conjunct.hasOneUse()) {
            joins.push_back(conjunction);
            pending.push_back(conjunction.getRhs());
            pending.push_back(conjunction.getLhs());
        } else {
            conjuncts.push_back(conjunct);
        }
    }
}

void setHopMask(Region& hop, Yield yield, llvm::ArrayRef<Value> residual, mlir::RewriterBase& rewriter) {
    if (residual.empty()) {
        rewriter.eraseBlock(&hop.front());
        return;
    }

    rewriter.setInsertionPoint(yield);
    const Type boolType = boolColumnType(rewriter.getContext());

    Value mask = residual.front();
    for (const Value conjunct : llvm::drop_begin(residual)) {
        mask = rewriter.create<AndOp>(yield.getLoc(), boolType, mask, conjunct).getResult();
    }

    rewriter.modifyOpInPlace(yield, [&yield, mask]() {
        yield->setOperand(0, mask);
    });
}

// The labels the hop region of a step asks of the hop's end node join the step's labels. The
// region keeps its other conjuncts, and goes when there is none left.
bool fuseStepHopLabels(ExplorePaths exploration, size_t step, llvm::SmallVectorImpl<Attribute>& labels, mlir::RewriterBase& rewriter) {
    Region& hop = exploration.getHops()[step];
    if (hop.empty()) {
        return false;
    }

    Block& block = hop.front();
    Yield yield = dyn_cast<Yield>(block.getTerminator());
    if (!yield || yield->getNumOperands() != 1) {
        return false;
    }

    llvm::SmallVector<Value> conjuncts;
    llvm::SmallVector<AndOp> joins;
    collectHopConjuncts(yield->getOperand(0), conjuncts, joins);

    const BlockArgument end = block.getArgument(hopEndArgument(step));

    llvm::SmallVector<CheckLabelConstraint> checks;
    llvm::SmallVector<Value> residual;
    for (const Value conjunct : conjuncts) {
        if (CheckLabelConstraint check = endLabelCheckOf(conjunct, end)) {
            checks.push_back(check);
        } else {
            residual.push_back(conjunct);
        }
    }

    if (checks.empty()) {
        return false;
    }

    for (CheckLabelConstraint check : checks) {
        for (const Attribute label : check.getConjunction()) {
            if (!llvm::is_contained(labels, label)) {
                labels.push_back(label);
            }
        }
    }

    setHopMask(hop, yield, residual, rewriter);
    if (residual.empty()) {
        return true;
    }

    for (AndOp join : joins) {
        rewriter.eraseOp(join);
    }

    for (CheckLabelConstraint check : checks) {
        Operation* const labelSet = check.getLabelsetIds().getDefiningOp();
        rewriter.eraseOp(check);
        eraseIfUnused(labelSet, rewriter);
    }

    return true;
}

void fuseHopLabels(ExplorePaths exploration, mlir::RewriterBase& rewriter) {
    const size_t stepCount = exploration.getStepCount();

    llvm::SmallVector<Attribute> stepLabels;
    bool fused = false;
    for (size_t step = 0; step < stepCount; step++) {
        llvm::SmallVector<Attribute> labels;
        if (const ArrayAttr current = exploration.getStepHopLabels(step)) {
            labels.append(current.begin(), current.end());
        }

        fused |= fuseStepHopLabels(exploration, step, labels, rewriter);
        stepLabels.push_back(rewriter.getArrayAttr(labels));
    }

    if (!fused) {
        return;
    }

    exploration.setHopLabelsAttr(rewriter.getArrayAttr(stepLabels));

    const bool hasPredicate = hasHopPredicate(exploration.getHops());
    if (!hasPredicate) {
        exploration.getHopImportsMutable().clear();
    }
}

struct FuseExploreHopLabels : public impl::FuseExploreHopLabelsBase<FuseExploreHopLabels> {
    void runOnOperation() override {
        mlir::IRRewriter rewriter(&getContext());
        getOperation()->walk([&rewriter](ExplorePaths exploration) {
            fuseHopLabels(exploration, rewriter);
        });
    }
};

// A filter carrying a named path cuts the rows before the path is built: the path is built
// again behind it, over the columns the filter keeps. A mask reading the path still reads
// the build ahead of the filter. Null where the filter does not keep every entity.
MakePath sinkPastFilter(MakePath build, FilterOp filter, mlir::OpBuilder& builder) {
    const Value path = build.getResult();
    const OperandRange carried = filter.getColumnsToFilter();

    llvm::SmallVector<Value> keptColumns;
    llvm::SmallVector<Type> keptTypes;
    llvm::SmallVector<size_t> keptIndices(carried.size(), 0);

    for (size_t index = 0; index < carried.size(); index++) {
        const Value column = carried[index];
        if (column == path) {
            continue;
        }

        keptIndices[index] = keptColumns.size();
        keptColumns.push_back(column);
        keptTypes.push_back(column.getType());
    }

    llvm::SmallVector<size_t> entityIndices;
    for (const Value entity : build.getEntities()) {
        const auto keptIt = llvm::find(keptColumns, entity);
        if (keptIt == keptColumns.end()) {
            return nullptr;
        }

        entityIndices.push_back(static_cast<size_t>(keptIt - keptColumns.begin()));
    }

    builder.setInsertionPoint(filter);
    FilterOp narrowed = builder.create<FilterOp>(filter.getLoc(), keptTypes, filter.getMask(), keptColumns);

    llvm::SmallVector<Value> entities;
    for (const size_t entityIndex : entityIndices) {
        entities.push_back(narrowed.getResult(entityIndex));
    }

    builder.setInsertionPointAfter(narrowed);
    MakePath sunk = builder.create<MakePath>(build.getLoc(), path.getType(), entities, build.getReversedPathsAttr());

    const ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        const Value replacement = carried[index] == path ? sunk.getResult() : narrowed.getResult(keptIndices[index]);
        filtered[index].replaceAllUsesWith(replacement);
    }

    filter.erase();

    return sunk;
}

MakePath sinkPastFirstSinkableFilter(MakePath build, mlir::OpBuilder& builder) {
    for (Operation* user : build.getResult().getUsers()) {
        FilterOp filter = dyn_cast<FilterOp>(user);
        if (!filter || filter->getBlock() != build->getBlock()) {
            continue;
        }

        if (const MakePath sunk = sinkPastFilter(build, filter, builder)) {
            return sunk;
        }
    }

    return nullptr;
}

struct SinkMakePath : public impl::SinkMakePathBase<SinkMakePath> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<MakePath> builds;
        getOperation()->walk([&builds](MakePath build) {
            builds.push_back(build);
        });

        while (!builds.empty()) {
            MakePath build = builds.pop_back_val();
            const MakePath sunk = sinkPastFirstSinkableFilter(build, builder);
            if (!sunk) {
                continue;
            }

            builds.push_back(sunk);

            if (build->use_empty()) {
                build.erase();
            } else {
                builds.push_back(build);
            }
        }
    }
};

// A missed row pads a pattern variable with null, and size(), nodes(), relationships() and
// length() of a null are null too. So the pattern computes them before the padding, which
// then carries their result instead of the list or path.
bool matchOptionalVariableRead(Operation* read, OptionalMatch& optional, size_t& resultIndex) {
    if (!isa<Size, PathElements>(read)) {
        return false;
    }

    const OpResult padded = dyn_cast<OpResult>(read->getOperand(0));
    if (!padded) {
        return false;
    }

    optional = dyn_cast<OptionalMatch>(padded.getOwner());
    resultIndex = padded.getResultNumber();

    return optional && resultIndex >= optional.getInputColumns().size();
}

void sinkIntoOptionalMatch(Operation* read, OptionalMatch optional, size_t resultIndex, mlir::OpBuilder& builder) {
    OptionalYield yield = patternYield(optional);

    builder.setInsertionPoint(yield);
    Operation* const sunk = builder.clone(*read);
    sunk->setOperand(0, yield.getColumns()[resultIndex]);

    yield.getColumnsMutable().append(sunk->getResult(0));

    llvm::SmallVector<size_t> keptResults(optional->getNumResults());
    std::iota(keptResults.begin(), keptResults.end(), size_t {0});

    OptionalMatch rebuilt = rebuildOptionalMatch(optional, keptResults, builder);

    read->getResult(0).replaceAllUsesWith(rebuilt->getResults().back());
    read->erase();
}

struct SinkIntoOptionalMatch : public impl::SinkIntoOptionalMatchBase<SinkIntoOptionalMatch> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SetVector<Operation*> reads;
        getOperation()->walk([&reads](Operation* op) {
            if (isa<Size, PathElements>(op)) {
                reads.insert(op);
            }
        });

        while (!reads.empty()) {
            Operation* const read = reads.pop_back_val();

            OptionalMatch optional;
            size_t resultIndex = 0;
            if (!matchOptionalVariableRead(read, optional, resultIndex)) {
                continue;
            }

            const Operation::user_range users = read->getUsers();
            const llvm::SmallVector<Operation*> readers(users.begin(), users.end());
            sinkIntoOptionalMatch(read, optional, resultIndex, builder);
            reads.insert(readers.begin(), readers.end());
        }
    }
};

// A named path over one walk is the node the walk seeds from and its handle column, so
// what nodes(), relationships() and length() read off it is what the handles already
// answer: the seed then each hop's end, the edges, and the depth
struct WalkPath {
    MakePath _build;
    Value _seed;
    Value _path;
    bool _reversed {false};
};

bool matchWalkPath(PathElements elements, WalkPath& walk) {
    MakePath build = elements.getPath().getDefiningOp<MakePath>();
    if (!build) {
        return false;
    }

    const OperandRange entities = build.getEntities();
    if (entities.size() != 2) {
        return false;
    }

    const std::optional<ArrayAttr> reversedPaths = build.getReversedPaths();
    const bool reversed = reversedPaths && !reversedPaths->empty();

    const Value seed = reversed ? entities[1] : entities[0];
    const Value path = reversed ? entities[0] : entities[1];

    const auto elementOf = [](Value value) { return cast<ColumnType>(value.getType()).getType(); };
    if (!isa<storage::NodeIDType>(elementOf(seed)) || !isa<storage::PathRefType>(elementOf(path))) {
        return false;
    }

    walk = WalkPath {._build = build, ._seed = seed, ._path = path, ._reversed = reversed};

    return true;
}

Value walkPathElements(PathElements elements, const WalkPath& walk, mlir::OpBuilder& builder) {
    MLIRContext* const context = builder.getContext();
    const Location loc = elements.getLoc();

    switch (elements.getKind()) {
        case storage::PathElementsKind::Length: {
            const Type countType = ColumnType::get(context, IntegerType::get(context, 64, IntegerType::Unsigned));
            return builder.create<PathLength>(loc, countType, walk._path).getResult();
        }
        break;

        case storage::PathElementsKind::Relationships: {
            const Type listType = ColumnType::get(context, storage::ListType::get(context, storage::EdgeIDType::get(context)));
            return builder.create<ExpandPath>(loc,
                                              listType,
                                              walk._path,
                                              Value(),
                                              storage::PathExpansionKind::Edges,
                                              walk._reversed,
                                              IntegerAttr(),
                                              IntegerAttr()).getResult();
        }
        break;

        case storage::PathElementsKind::Nodes: {
            const Type listType = ColumnType::get(context, storage::ListType::get(context, storage::NodeIDType::get(context)));
            return builder.create<ExpandPath>(loc,
                                              listType,
                                              walk._path,
                                              walk._seed,
                                              storage::PathExpansionKind::Nodes,
                                              walk._reversed,
                                              IntegerAttr(),
                                              IntegerAttr()).getResult();
        }
        break;
    }

    return Value();
}

struct FusePathElements : public impl::FusePathElementsBase<FusePathElements> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<PathElements> reads;
        getOperation()->walk([&reads](PathElements elements) {
            reads.push_back(elements);
        });

        for (PathElements elements : reads) {
            WalkPath walk;
            if (!matchWalkPath(elements, walk)) {
                continue;
            }

            builder.setInsertionPoint(elements);
            const Value handles = walkPathElements(elements, walk, builder);

            Value result = elements.getResult();
            ToNullable widened = builder.create<ToNullable>(elements.getLoc(), result.getType(), handles);

            result.replaceAllUsesWith(widened.getResult());
            elements.erase();
            eraseIfUnused(walk._build);
        }
    }
};

// A to_nullable column holds a value in every row, so the size of one is the size of the
// column it widens
void readSizeOfTheWidenedColumn(Size size) {
    while (ToNullable widened = size.getInput().getDefiningOp<ToNullable>()) {
        size->setOperand(0, widened.getOperand());
        eraseIfUnused(widened);
    }
}

// The list a walk's handles expand to holds one entry per hop, or per repetition of a body
// of several steps, which the depth of the handle answers, and its list of nodes one more
struct FusePathLength : public impl::FusePathLengthBase<FusePathLength> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<Size> sizes;
        getOperation()->walk([&sizes](Size size) {
            sizes.push_back(size);
        });

        for (Size size : sizes) {
            readSizeOfTheWidenedColumn(size);

            ExpandPath expansion = size.getInput().getDefiningOp<ExpandPath>();
            if (!expansion) {
                continue;
            }

            Value result = size.getResult();
            const bool countsNodes = expansion.getKind() == storage::PathExpansionKind::Nodes;

            builder.setInsertionPoint(size);
            PathLength length = builder.create<PathLength>(size.getLoc(),
                                                           result.getType(),
                                                           expansion.getPaths(),
                                                           expansion.getStepsAttr(),
                                                           countsNodes ? builder.getUnitAttr() : UnitAttr());

            result.replaceAllUsesWith(length.getResult());
            size.erase();
            eraseIfUnused(expansion);
        }
    }
};

// A path exploration whose rows are then cut down to those ending on the node a carried
// column already holds, or on the seed the walk left: the bound end spelled the long way,
// since the walk itself can head for that node and never build the rows the filter goes on
// to drop. An absent _endColumn is the seed, which no carried column names.
struct EndBoundExploration {
    FilterOp _filter;
    Value _conjunct;
    ExplorePaths _exploration;
    std::optional<uint64_t> _endColumn;
};

bool matchEndBoundConjunct(Value conjunct, ExplorePaths exploration, EndBoundExploration& bound) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!equality) {
        return false;
    }

    // One side is the end node column, the other the carried copy of the bound variable
    const Value ends = exploration.getTgtids();
    const Value carried = equality.getLhs() == ends ? equality.getRhs() : equality.getLhs();
    if (equality.getLhs() != ends && equality.getRhs() != ends) {
        return false;
    }

    const OpResult carriedResult = dyn_cast<OpResult>(carried);
    if (!carriedResult || carriedResult.getOwner() != exploration.getOperation() || !isNodeColumn(carried)) {
        return false;
    }

    // A walk the pattern brings back to where it started compares its end against its own
    // seed, which is a result of the exploration like a carried column but not one of them
    const size_t resultIndex = carriedResult.getResultNumber();
    const bool endsOnSeed = carried == exploration.getSrcids();
    if (!endsOnSeed && resultIndex < pathFixedResultCount) {
        return false;
    }

    bound._conjunct = conjunct;
    bound._endColumn = endsOnSeed ? std::nullopt : std::optional<uint64_t> {resultIndex - pathFixedResultCount};

    return true;
}

bool matchEndBoundExploration(FilterOp filter, EndBoundExploration& bound) {
    ExplorePaths exploration = dyn_cast_or_null<ExplorePaths>(filteredSource(filter));
    const bool alreadyBound = exploration && (exploration.getEndColumn() || exploration.getEndsOnSeed() || exploration.getEndNodes());
    if (!exploration || alreadyBound || !readsOnlyIntoTheFilter(exploration, filter)) {
        return false;
    }

    bound._filter = filter;
    bound._exploration = exploration;

    return matchAConjunct(filter, [exploration, &bound](Value conjunct) {
        return matchEndBoundConjunct(conjunct, exploration, bound);
    });
}

void fuseExploreEndNodes(EndBoundExploration& bound, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = bound._exploration;
    const std::optional<uint64_t> endColumn = bound._endColumn;

    rewriter.modifyOpInPlace(exploration, [&exploration, &rewriter, endColumn]() {
        if (endColumn) {
            exploration.setEndColumnAttr(unsignedAttribute(*endColumn, rewriter));
        } else {
            exploration.setEndsOnSeed(true);
        }
    });

    dropConjunct(bound._filter, bound._conjunct, rewriter);
}

struct FuseExploreEndNodes : public impl::FuseExploreEndNodesBase<FuseExploreEndNodes> {
    void runOnOperation() override {
        runFilterWorklist<EndBoundExploration>(getOperation(), matchEndBoundExploration, fuseExploreEndNodes);
    }
};

// A path exploration whose end column is one factor of the cross product its seeds come
// from: every row carries the same targets, so the walk can head for them as a set and run
// once per seed instead of once per pair. That factor's rows are the set, so its ops move to
// the head of the function and the product goes on without the column.
struct FactorEndExploration {
    ExplorePaths _exploration;
    CrossProduct _product;
    size_t _endResult {0};
};

// The factor of a product yielding one of its results, and where in its yield
struct FactorColumn {
    Region* _factor {nullptr};
    Region* _other {nullptr};
    size_t _position {0};
    bool _left {false};
};

void locateFactorColumn(CrossProduct product, size_t resultIndex, FactorColumn& column) {
    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();
    const size_t leftCount = factorYieldColumns(leftFactor).size();

    column._left = resultIndex < leftCount;
    column._factor = column._left ? &leftFactor : &rightFactor;
    column._other = column._left ? &rightFactor : &leftFactor;
    column._position = column._left ? resultIndex : resultIndex - leftCount;
}

// Whether the result can be taken out of the product as a set: its factor yields it alone,
// or yields it off a product of its own that nothing but the yield reads, and so on down
bool canHoistFactorColumn(CrossProduct product, size_t resultIndex) {
    CrossProduct current = product;
    size_t currentIndex = resultIndex;
    while (true) {
        FactorColumn column;
        locateFactorColumn(current, currentIndex, column);

        const Operation::operand_range yielded = factorYieldColumns(*column._factor);
        if (yielded.size() == 1) {
            return true;
        }

        const Value value = yielded[column._position];
        CrossProduct inner = value.getDefiningOp<CrossProduct>();
        if (!inner || inner->getParentRegion() != column._factor) {
            return false;
        }

        Operation* const yield = column._factor->front().getTerminator();
        for (const Value result : inner.getResults()) {
            for (Operation* const user : result.getUsers()) {
                if (user != yield) {
                    return false;
                }
            }
        }

        current = inner;
        currentIndex = cast<OpResult>(value).getResultNumber();
    }
}

bool matchFactorEndExploration(ExplorePaths exploration, FactorEndExploration& match) {
    const std::optional<uint64_t> endColumn = exploration.getEndColumn();
    if (!endColumn || exploration.getEndNodes()) {
        return false;
    }

    const Value input = exploration.getInputNodes();
    const Value end = exploration.getColumnsToFilter()[*endColumn];
    CrossProduct product = input.getDefiningOp<CrossProduct>();
    if (!product || end.getDefiningOp() != product.getOperation()) {
        return false;
    }

    // A target drawn from the seed's own factor differs row by row
    const size_t inputResult = cast<OpResult>(input).getResultNumber();
    const size_t endResult = cast<OpResult>(end).getResultNumber();
    FactorColumn seed;
    FactorColumn target;
    locateFactorColumn(product, inputResult, seed);
    locateFactorColumn(product, endResult, target);
    if (seed._factor == target._factor) {
        return false;
    }

    for (const Value result : product.getResults()) {
        for (Operation* const user : result.getUsers()) {
            if (user != exploration.getOperation()) {
                return false;
            }
        }
    }

    if (!canHoistFactorColumn(product, endResult)) {
        return false;
    }

    match = FactorEndExploration {._exploration = exploration, ._product = product, ._endResult = endResult};

    return true;
}

void moveFactorBody(Region& factor, Block* target, Block::iterator position) {
    Block& block = factor.front();
    target->getOperations().splice(position, block.getOperations(), block.begin(), Block::iterator(block.getTerminator()));
}

// One product on the way down to the factor yielding a hoisted result alone
struct FactorColumnLevel {
    CrossProduct _product {nullptr};
    size_t _resultIndex {0};
    FactorColumn _column;
};

Value collapseFactorColumn(const FactorColumnLevel& level, Block* head) {
    CrossProduct product = level._product;
    const FactorColumn& column = level._column;

    const llvm::SmallVector<Value> yielded(factorYieldColumns(*column._factor));
    const llvm::SmallVector<Value> otherYielded(factorYieldColumns(*column._other));

    moveFactorBody(*column._factor, head, head->begin());
    moveFactorBody(*column._other, product->getBlock(), Block::iterator(product));

    const size_t firstOtherResult = column._left ? 1 : 0;
    for (size_t index = 0; index < otherYielded.size(); index++) {
        product.getResult(firstOtherResult + index).replaceAllUsesWith(otherYielded[index]);
    }

    const Value hoisted = yielded.front();
    product.getResult(level._resultIndex).replaceAllUsesWith(hoisted);
    product.erase();

    return hoisted;
}

void narrowFactorColumn(const FactorColumnLevel& level, Value hoisted, mlir::OpBuilder& builder) {
    CrossProduct product = level._product;
    const FactorColumn& column = level._column;
    const size_t resultIndex = level._resultIndex;

    Yield yield = cast<Yield>(column._factor->front().getTerminator());
    yield.getColumnsMutable().erase(static_cast<unsigned>(column._position));

    llvm::SmallVector<Type> narrowedTypes;
    for (size_t index = 0; index < product.getNumResults(); index++) {
        if (index != resultIndex) {
            narrowedTypes.push_back(product.getResult(index).getType());
        }
    }

    builder.setInsertionPoint(product);
    CrossProduct narrowed = builder.create<CrossProduct>(product.getLoc(), narrowedTypes);
    narrowed.getLeftFactor().takeBody(product.getLeftFactor());
    narrowed.getRightFactor().takeBody(product.getRightFactor());

    size_t kept = 0;
    for (size_t index = 0; index < product.getNumResults(); index++) {
        if (index == resultIndex) {
            product.getResult(index).replaceAllUsesWith(hoisted);
        } else {
            product.getResult(index).replaceAllUsesWith(narrowed.getResult(kept));
            kept++;
        }
    }

    product.erase();
}

// Takes the result out of the product as a value at the head of the function and leaves the
// product without it: collapsed to its other factor when the result was all its factor
// yielded, rebuilt one column narrower otherwise
Value hoistFactorColumn(CrossProduct product, size_t resultIndex, Block* head, mlir::OpBuilder& builder) {
    llvm::SmallVector<FactorColumnLevel> levels;
    CrossProduct current = product;
    size_t currentIndex = resultIndex;
    while (true) {
        FactorColumnLevel level {._product = current, ._resultIndex = currentIndex};
        locateFactorColumn(current, currentIndex, level._column);
        levels.push_back(level);

        const Operation::operand_range yielded = factorYieldColumns(*level._column._factor);
        if (yielded.size() == 1) {
            break;
        }

        const Value value = yielded[level._column._position];
        current = value.getDefiningOp<CrossProduct>();
        currentIndex = cast<OpResult>(value).getResultNumber();
    }

    const Value hoisted = collapseFactorColumn(levels.back(), head);
    for (const FactorColumnLevel& level : llvm::reverse(llvm::ArrayRef(levels).drop_back())) {
        narrowFactorColumn(level, hoisted, builder);
    }

    return hoisted;
}

void fuseExploreEndFactor(const FactorEndExploration& match, mlir::OpBuilder& builder) {
    ExplorePaths exploration = match._exploration;
    CrossProduct product = match._product;
    const size_t endColumn = *exploration.getEndColumn();
    const Value endResult = product.getResult(match._endResult);

    const Operation::operand_range carried = exploration.getColumnsToFilter();
    const mlir::ResultRange carriedResults = exploration.getFilteredColumns();

    llvm::SmallVector<Value> keptColumns;
    llvm::SmallVector<Type> resultTypes {exploration.getSrcids().getType(), exploration.getTgtids().getType(), exploration.getPaths().getType()};
    for (size_t index = 0; index < carried.size(); index++) {
        if (index == endColumn) {
            continue;
        }

        keptColumns.push_back(carried[index]);
        resultTypes.push_back(carriedResults[index].getType());
    }

    // The set is the product's own result until the hoist puts the factor's value in its place
    builder.setInsertionPoint(exploration);
    ExplorePaths set = builder.create<ExplorePaths>(exploration.getLoc(),
                                                    resultTypes,
                                                    exploration.getInputNodes(),
                                                    keptColumns,
                                                    endResult,
                                                    exploration.getHopImports(),
                                                    exploration.getDirectionsAttr(),
                                                    exploration.getMinHopsAttr(),
                                                    exploration.getMaxHopsAttr(),
                                                    exploration.getEdgeTypesAttr(),
                                                    exploration.getEndLabelsAttr(),
                                                    exploration.getHopLabelsAttr(),
                                                    IntegerAttr(),
                                                    UnitAttr(),
                                                    exploration.getDistinctAttr(),
                                                    exploration.getStepCount());
    takeHopRegions(exploration, set);

    exploration.getSrcids().replaceAllUsesWith(set.getSrcids());
    exploration.getTgtids().replaceAllUsesWith(set.getTgtids());
    exploration.getPaths().replaceAllUsesWith(set.getPaths());

    const mlir::ResultRange setCarried = set.getFilteredColumns();
    size_t kept = 0;
    for (size_t index = 0; index < carried.size(); index++) {
        if (index == endColumn) {
            carriedResults[index].replaceAllUsesWith(set.getTgtids());
        } else {
            carriedResults[index].replaceAllUsesWith(setCarried[kept]);
            kept++;
        }
    }

    exploration.erase();

    mlir::func::FuncOp function = set->getParentOfType<mlir::func::FuncOp>();
    hoistFactorColumn(product, match._endResult, &function.getBody().front(), builder);
}

struct FuseExploreEndFactor : public impl::FuseExploreEndFactorBase<FuseExploreEndFactor> {
    void runOnOperation() override {
        llvm::SmallVector<ExplorePaths> explorations;
        getOperation()->walk([&](ExplorePaths exploration) {
            explorations.push_back(exploration);
        });

        mlir::OpBuilder builder(&getContext());
        for (ExplorePaths exploration : explorations) {
            FactorEndExploration match;
            if (matchFactorEndExploration(exploration, match)) {
                fuseExploreEndFactor(match, builder);
            }
        }
    }
};

// A conjunct a scan of the graph answers, over the node column it reads: the nodes holding a
// property value, the nodes listed by id, or the ends of a walk whose last edge is listed by
// id. Its last hop reaches those ends over the listed edges, and may reach them over others,
// so that conjunct still tests the rows.
struct NodeScanPredicate {
    Value _column;
    StringAttr _property;
    TypedAttr _value;
    Value _boundValue;
    EntityIDList _nodeIDs;
    EntityIDList _lastEdgeIDs;
    storage::PathDirection _lastHop {storage::PathDirection::Forward};
    bool _listsLastEdges {false};
};

bool matchNodeIDPredicate(Value conjunct, NodeScanPredicate& predicate) {
    const Value column = nodeDisjunctionColumn(conjunct);
    if (!column || !collectIDDisjunction(conjunct, column, predicate._nodeIDs)) {
        return false;
    }

    predicate._column = column;

    return true;
}

bool matchWalkLastEdgePredicate(Value conjunct, NodeScanPredicate& predicate) {
    const Value column = disjunctionColumn(conjunct);
    WalkEndEdge endEdge;
    if (!column || !matchWalkEndEdge(column, endEdge) || endEdge._first) {
        return false;
    }

    if (!collectIDDisjunction(conjunct, column, predicate._lastEdgeIDs)) {
        return false;
    }

    ExplorePaths walk = endEdge._walk;

    predicate._column = walk.getTgtids();
    predicate._lastHop = walk.getStepDirection(walk.getStepCount() - 1);
    predicate._listsLastEdges = true;

    return true;
}

bool matchNodeScanPredicate(Value conjunct, NodeScanPredicate& predicate) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    PropertyEquality property;
    if (!equality || !matchPropertyEquality(equality, property)) {
        return matchNodeIDPredicate(conjunct, predicate) || matchWalkLastEdgePredicate(conjunct, predicate);
    }

    // A read of the write buffer sees nodes no scan of the graph does
    GetNodeProperties read = property._read;
    if (read.getPending() || read.getAllPending()) {
        return false;
    }

    predicate._column = read.getInputNodes();
    predicate._property = read.getPropertyAttr();
    predicate._value = property._value;
    predicate._boundValue = property._boundValue;

    return true;
}

// The labels every node of the column carries, as a by-label read asks for them, or null
ArrayAttr knownLabelsOf(Value column, mlir::OpBuilder& builder) {
    llvm::DenseSet<Attribute> known;
    collectKnownLabels(column, known);
    if (known.empty()) {
        return ArrayAttr();
    }

    const auto orderKey = [](Attribute label) {
        if (const StringAttr name = dyn_cast<StringAttr>(label)) {
            return std::pair(false, name.getValue());
        }

        return std::pair(true, cast<storage::ParameterAttr>(label).getName());
    };

    llvm::SmallVector<Attribute> labels(known.begin(), known.end());
    llvm::sort(labels, [&orderKey](Attribute lhs, Attribute rhs) {
        return orderKey(lhs) < orderKey(rhs);
    });

    return builder.getArrayAttr(labels);
}

Value buildNodeScan(const NodeScanPredicate& predicate, ArrayAttr labels, Location loc, mlir::OpBuilder& builder) {
    const Type nodeColumnType = predicate._column.getType();

    if (predicate._property) {
        ScanNodesByPropertyValue scan = builder.create<ScanNodesByPropertyValue>(loc,
                                                                                 nodeColumnType,
                                                                                 predicate._property,
                                                                                 predicate._value,
                                                                                 predicate._boundValue,
                                                                                 labels);
        computeBoundValuesAhead(scan);

        return scan.getResult();
    }

    llvm::SmallVector<Value> nodes;
    if (predicate._listsLastEdges) {
        nodes.push_back(scanListedEdgeEnds(predicate._lastEdgeIDs, predicate._lastHop, true, loc, builder));
    } else {
        ConstScanNodes listed = builder.create<ConstScanNodes>(loc,
                                                               nodeColumnType,
                                                               predicate._nodeIDs._literals,
                                                               predicate._nodeIDs._bound);
        computeBoundValuesAhead(listed);
        nodes.push_back(listed.getResult());
    }

    if (labels) {
        keepLabelledRows(nodes, labels, loc, builder);
    }

    return nodes.front();
}

// The nodes a column holds on every row that satisfies a scannable predicate some hops away:
// the predicate's scan, walked back over each hop that bound its node from the one before.
// _hops runs from the predicate's node back to _column, which no hop bound; with no hop the
// set is the predicate itself. Nothing in it reads a row, so it can be built ahead of them all.
struct NodeSetChain {
    NodeScanPredicate _predicate;
    llvm::SmallVector<Operation*> _hops;
    Value _column;
};

void deriveNodeSetChain(const NodeScanPredicate& predicate, LineageClimbs& climbs, NodeSetChain& chain) {
    chain._predicate = predicate;

    Value column = climbs.anchorOf(predicate._column);
    while (column) {
        Operation* const hop = column.getDefiningOp();
        const bool reachedByHop = hop && isEdgeHop(hop) && cast<OpResult>(column).getResultNumber() == hopReachedResult(hop);
        if (!reachedByHop) {
            break;
        }

        chain._hops.push_back(hop);
        column = climbs.anchorOf(hop->getOperand(0));
    }

    chain._column = column;
}

// Builds the set at the builder's insertion point. A hop reaches a node once per edge, so
// each node is walked back from once
Value buildNodeSetChain(const NodeSetChain& chain, Location loc, mlir::OpBuilder& builder) {
    const NodeScanPredicate& predicate = chain._predicate;
    Value nodes = buildNodeScan(predicate, knownLabelsOf(predicate._column, builder), loc, builder);

    for (size_t index = 0; index < chain._hops.size(); index++) {
        Operation* const hop = chain._hops[index];

        if (index > 0) {
            RemoveDuplicates distinct = builder.create<RemoveDuplicates>(loc, TypeRange {nodes.getType()}, ValueRange {nodes}, Value());
            nodes = distinct.getResult(0);
        }

        const ArrayAttr labels = knownLabelsOf(hop->getOperand(0), builder);
        Operation* const reversed = createReversedHop(hop, nodes, labels, ValueRange(), builder);
        nodes = reversed->getResult(hopReachedResult(reversed));
    }

    return nodes;
}

// A path exploration whose rows a filter then cuts to those whose end is, or reaches over a
// chain of hops, a node a scan of the graph can list: the walk can head for the ends that
// chain leads back to, and leave unwalked the prefixes that cannot reach one. With no hop
// between, the predicate holds of the end itself and leaves the filter.
struct ExploreEndSet {
    FilterOp _filter;
    ExplorePaths _exploration;
    Value _conjunct;
    NodeSetChain _chain;
};

// The first op of the function's body that writes the graph or holds a write, null if none
Operation* firstGraphWriteOf(mlir::func::FuncOp function) {
    for (Operation& op : function.getBody().front()) {
        const WalkResult walked = op.walk([](Operation* nested) {
            return isGraphWrite(nested) ? WalkResult::interrupt() : WalkResult::advance();
        });

        if (walked.wasInterrupted()) {
            return &op;
        }
    }

    return nullptr;
}

// The end set is built ahead of the whole function, so it leaves out what the query writes
// before the walk
bool writesAheadOf(Operation* firstWrite, Operation* op) {
    if (!firstWrite) {
        return false;
    }

    Operation* const holder = firstWrite->getBlock()->findAncestorOpInBlock(*op);

    return !holder || holder == firstWrite || firstWrite->isBeforeInBlock(holder);
}

bool matchExploreEndSet(FilterOp filter,
                        LineageClimbs& climbs,
                        const llvm::DenseMap<Operation*, Operation*>& firstWrites,
                        size_t maxChainHops,
                        ExploreEndSet& endSet) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    OpAnswers reaching;

    for (const Value conjunct : conjuncts) {
        NodeScanPredicate predicate;
        if (!matchNodeScanPredicate(conjunct, predicate)) {
            continue;
        }

        NodeSetChain chain;
        deriveNodeSetChain(predicate, climbs, chain);
        if (chain._hops.size() > maxChainHops) {
            continue;
        }

        ExplorePaths exploration = chain._column ? chain._column.getDefiningOp<ExplorePaths>() : nullptr;
        if (!exploration || chain._column != exploration.getTgtids()) {
            continue;
        }

        // Every row the walk emits has to reach the filter, or a reader of the rows it cut
        // would miss them
        const bool alreadyBound = exploration.getEndColumn() || exploration.getEndsOnSeed() || exploration.getEndNodes();
        const bool besideTheFilter = exploration->getBlock() == filter->getBlock();
        if (alreadyBound || !besideTheFilter) {
            continue;
        }

        const bool reachesTheFilter = answerOnce(reaching, exploration, [&exploration, &filter]() {
            return rowsReachTheFilter(exploration, filter, keepsTheSourceRows);
        });

        if (!reachesTheFilter) {
            continue;
        }

        Operation* const firstWrite = firstWrites.lookup(exploration->getParentOfType<mlir::func::FuncOp>());
        if (writesAheadOf(firstWrite, exploration)) {
            continue;
        }

        endSet = ExploreEndSet {._filter = filter, ._exploration = exploration, ._conjunct = conjunct, ._chain = chain};

        return true;
    }

    return false;
}

void fuseExploreEndSet(ExploreEndSet& endSet, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = endSet._exploration;
    const NodeSetChain& chain = endSet._chain;

    mlir::func::FuncOp function = exploration->getParentOfType<mlir::func::FuncOp>();
    rewriter.setInsertionPointToStart(&function.getBody().front());
    const Value set = buildNodeSetChain(chain, exploration.getLoc(), rewriter);

    const bool endsOnThePredicate = chain._hops.empty();
    rewriter.modifyOpInPlace(exploration, [&exploration, set, endsOnThePredicate]() {
        exploration.getEndNodesMutable().assign(set);
        if (endsOnThePredicate) {
            exploration.removeEndLabelsAttr();
        }
    });

    const bool holdsTheConjunct = endsOnThePredicate && !chain._predicate._listsLastEdges;
    if (holdsTheConjunct && endSet._conjunct.hasOneUse()) {
        dropConjunct(endSet._filter, endSet._conjunct, rewriter);
    }
}

struct FuseExploreEndSet : public impl::FuseExploreEndSetBase<FuseExploreEndSet> {
    void runOnOperation() override {
        llvm::DenseMap<Operation*, Operation*> firstWrites;
        getOperation()->walk([&firstWrites](mlir::func::FuncOp function) {
            firstWrites[function.getOperation()] = firstGraphWriteOf(function);
        });

        LineageClimbs climbs;

        const auto rewrite = [&climbs](ExploreEndSet& endSet, mlir::RewriterBase& rewriter) {
            fuseExploreEndSet(endSet, rewriter);
            climbs.clear();
        };

        // A walk takes one end set, so every predicate on an end itself, the exact set, is
        // fused before any set reached back over hops can take its walk
        for (const size_t maxChainHops : {size_t(0), SIZE_MAX}) {
            const auto match = [&climbs, &firstWrites, maxChainHops](FilterOp filter, ExploreEndSet& endSet) {
                return matchExploreEndSet(filter, climbs, firstWrites, maxChainHops, endSet);
            };

            runFilterWorklist<ExploreEndSet>(getOperation(), match, rewrite);
        }
    }
};

// A filter keeping the walks whose every hop passes a test, spelled as all() or none() over
// the relationships, the nodes or a group variable of the path: the walk can run the test on
// each hop as it takes it, and never expand a hop that fails.
struct ExploreListPredicate {
    FilterOp _filter;
    ExplorePaths _exploration;
    ListPredicate _predicate;
    ExpandPath _expansion;
    llvm::SmallVector<Value> _imports;
};

bool keepsTheWalkedRows(Operation& op) {
    return isa<FilterOp, ExpandPath, ListPredicate>(op) || computesPerRow(&op);
}

// Whether the body reads only the element, constants and carried columns holding one value per
// seed. Each carried argument the body reads gets the exploration operand to import, the rest null.
bool matchHopTest(ListPredicate predicate, ExplorePaths exploration, llvm::SmallVectorImpl<Value>& imports) {
    Block& body = predicate.getBody().front();
    const BlockArgument tag = body.getArgument(1);
    ComprehensionYield yield = cast<ComprehensionYield>(body.getTerminator());

    const auto readsOnlyTheHop = [&body, tag](Value operand) {
        const BlockArgument argument = dyn_cast<BlockArgument>(operand);
        if (argument && argument.getOwner() == &body) {
            return argument != tag;
        } else if (!argument && operand.getDefiningOp()->getBlock() == &body) {
            return true;
        }

        return ::db::yieldsConstantColumn(operand);
    };

    for (Operation& op : body.without_terminator()) {
        const bool holdsARegion = op.getNumRegions() != 0;
        const bool readsOutsideTheHop = !llvm::all_of(op.getOperands(), readsOnlyTheHop);
        if (holdsARegion || readsOutsideTheHop) {
            return false;
        }
    }

    const bool cutsElements = yield.getRowTags() != tag;
    const bool yieldsOutsideTheHop = !readsOnlyTheHop(yield.getValue());
    if (cutsElements || yieldsOutsideTheHop) {
        return false;
    }

    const Operation::operand_range carried = predicate.getColumnsToFilter();
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (body.getArgument(carriedIndex + 2).use_empty()) {
            imports.push_back({});
            continue;
        }

        const Value seedColumn = seedColumnOf(exploration, carried[carriedIndex]);
        if (!seedColumn) {
            return false;
        }

        imports.push_back(seedColumn);
    }

    return true;
}

bool matchExploreListConjunct(FilterOp filter, Value conjunct, OpAnswers& reaching, ExploreListPredicate& match) {
    ListPredicate predicate = conjunct.getDefiningOp<ListPredicate>();
    if (!predicate || !predicate.getResult().hasOneUse()) {
        return false;
    }

    const storage::ListPredicateKind kind = predicate.getKind();
    const bool holdsForEveryElement = kind == storage::ListPredicateKind::All || kind == storage::ListPredicateKind::None;
    if (!holdsForEveryElement) {
        return false;
    }

    Value source = predicate.getSource();
    if (ToNullable widened = source.getDefiningOp<ToNullable>()) {
        source = widened.getOperand();
    }

    ExpandPath expansion = source.getDefiningOp<ExpandPath>();
    if (!expansion) {
        return false;
    }

    const Value paths = climbFilters(expansion.getPaths());
    ExplorePaths exploration = paths.getDefiningOp<ExplorePaths>();
    const bool walksThePaths = exploration && paths == exploration.getPaths();
    if (!walksThePaths) {
        return false;
    }

    const std::optional<uint64_t> expandedSteps = expansion.getSteps();
    if (expandedSteps && *expandedSteps != exploration.getStepCount()) {
        return false;
    }

    const bool reachesTheFilter = exploration->getBlock() == filter->getBlock()
                               && answerOnce(reaching, exploration, [&exploration, &filter]() {
                                      return rowsReachTheFilter(exploration, filter, keepsTheWalkedRows);
                                  });
    if (!reachesTheFilter) {
        return false;
    }

    llvm::SmallVector<Value> imports;
    if (!matchHopTest(predicate, exploration, imports)) {
        return false;
    }

    const bool importsAColumn = llvm::any_of(imports, [](Value import) {
        return static_cast<bool>(import);
    });

    if (importsAColumn && exploration.getDistinct()) {
        return false;
    }

    match = ExploreListPredicate {._filter = filter,
                                  ._exploration = exploration,
                                  ._predicate = predicate,
                                  ._expansion = expansion,
                                  ._imports = imports};

    return true;
}

bool matchExploreListPredicate(FilterOp filter, ExploreListPredicate& match) {
    OpAnswers reaching;

    return matchAConjunct(filter, [&filter, &reaching, &match](Value conjunct) {
        return matchExploreListConjunct(filter, conjunct, reaching, match);
    });
}

// The argument of a step's hop region holding the element a list of the expansion kind reads
// off that hop: the region opens on the step's earlier nodes and edges
unsigned hopArgumentOf(storage::PathExpansionKind expansionKind, size_t step) {
    switch (expansionKind) {
        case storage::PathExpansionKind::Sources:
            return hopSourceArgument(step);
        break;
        case storage::PathExpansionKind::Edges:
            return hopEdgeArgument(step);
        break;
        case storage::PathExpansionKind::Ends:
        case storage::PathExpansionKind::Nodes:
            return hopEndArgument(step);
        break;
    }

    llvm_unreachable("Unknown path expansion kind");
}

// Clones the predicate's body at the rewriter's insertion point over the element column and
// the columns its carried arguments stand for, and returns the test it yields: negated for
// none(), which holds where all() of the negation does
Value cloneElementTest(ListPredicate predicate, Value element, llvm::ArrayRef<Value> carried, mlir::RewriterBase& rewriter) {
    Block& body = predicate.getBody().front();

    mlir::IRMapping mapping;
    mapping.map(body.getArgument(0), element);
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (carried[carriedIndex]) {
            mapping.map(body.getArgument(carriedIndex + 2), carried[carriedIndex]);
        }
    }

    for (Operation& op : body.without_terminator()) {
        rewriter.clone(op, mapping);
    }

    ComprehensionYield yield = cast<ComprehensionYield>(body.getTerminator());
    const Value test = mapping.lookupOrDefault(yield.getValue());

    if (predicate.getKind() == storage::ListPredicateKind::All) {
        return test;
    }

    return rewriter.create<NotOp>(predicate.getLoc(), boolColumnType(rewriter.getContext()), test).getResult();
}

// The block of a step's hop region, created over the step's hop and the imports the
// exploration already reads when the step had no predicate
Block* hopBlockOf(ExplorePaths exploration, size_t step, mlir::RewriterBase& rewriter) {
    Region& hop = exploration.getHops()[step];
    if (!hop.empty()) {
        return &hop.front();
    }

    mlir::MLIRContext* context = rewriter.getContext();
    const Type nodeType = ColumnType::get(context, storage::NodeIDType::get(context));
    const Type edgeType = ColumnType::get(context, storage::EdgeIDType::get(context));

    llvm::SmallVector<Type> argumentTypes;
    buildHopArgumentTypes(step, nodeType, edgeType, exploration.getHopImports(), argumentTypes);

    const llvm::SmallVector<Location> argumentLocations(argumentTypes.size(), exploration.getLoc());

    const mlir::OpBuilder::InsertionGuard guard(rewriter);

    return rewriter.createBlock(&hop, hop.end(), argumentTypes, argumentLocations);
}

// The yield of a step's hop region, the rewriter left inserting ahead of it: null for a block
// that yields nothing yet, the rewriter then inserting at its end
Yield hopYieldOf(Block* hopBlock, mlir::RewriterBase& rewriter) {
    Yield hopYield = hopBlock->empty() ? Yield {} : dyn_cast<Yield>(hopBlock->getTerminator());
    if (hopYield) {
        rewriter.setInsertionPoint(hopYield);
    } else {
        rewriter.setInsertionPointToEnd(hopBlock);
    }

    return hopYield;
}

// One more test the step's hop must pass, and-ed into what its region yields
void joinHopTest(Yield hopYield, Value hopTest, Location loc, mlir::RewriterBase& rewriter) {
    if (hopYield) {
        const Type boolType = boolColumnType(rewriter.getContext());
        const Value joined = rewriter.create<AndOp>(loc, boolType, hopYield.getColumns().front(), hopTest).getResult();

        rewriter.modifyOpInPlace(hopYield, [&hopYield, joined]() {
            hopYield->setOperand(0, joined);
        });
    } else {
        rewriter.create<Yield>(loc, ValueRange {hopTest});
    }
}

void addHopTest(const ExploreListPredicate& match, size_t step, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = match._exploration;
    ListPredicate predicate = match._predicate;

    Block* hopBlock = hopBlockOf(exploration, step, rewriter);
    Region& hop = *hopBlock->getParent();

    llvm::SmallVector<Value> hopCarried;
    for (const Value import : match._imports) {
        hopCarried.push_back(import ? carryIntoRegion(hop, import, rewriter) : Value {});
    }

    ExpandPath expansion = match._expansion;
    const Value hopElement = hopBlock->getArgument(hopArgumentOf(expansion.getKind(), step));

    const Yield hopYield = hopYieldOf(hopBlock, rewriter);
    const Value hopTest = cloneElementTest(predicate, hopElement, hopCarried, rewriter);

    joinHopTest(hopYield, hopTest, predicate.getLoc(), rewriter);
}

// A list over one step of the body is tested on that step's hops alone, a list over every
// hop on the hops of every step
void fuseExploreListPredicate(ExploreListPredicate& match, mlir::RewriterBase& rewriter) {
    FilterOp filter = match._filter;
    ExplorePaths exploration = match._exploration;
    ListPredicate predicate = match._predicate;
    ExpandPath expansion = match._expansion;

    llvm::SetVector<Value> hopTestReads;
    mlir::getUsedValuesDefinedAbove(predicate.getBody(), hopTestReads);
    hoistDefinitionsAhead(hopTestReads.getArrayRef(), exploration, rewriter);

    const std::optional<uint64_t> expandedStep = expansion.getStep();
    const size_t stepCount = exploration.getStepCount();
    for (size_t step = 0; step < stepCount; step++) {
        if (!expandedStep || *expandedStep == step) {
            addHopTest(match, step, rewriter);
        }
    }

    if (expansion.getKind() != storage::PathExpansionKind::Nodes) {
        dropConjunct(filter, predicate.getResult(), rewriter);
    } else {
        // nodes() holds the seed, which no hop ends on, so the seed is still tested row by row
        const Operation::operand_range predicateCarried = predicate.getColumnsToFilter();
        const llvm::SmallVector<Value> rowCarried(predicateCarried.begin(), predicateCarried.end());

        rewriter.setInsertionPoint(predicate);
        const Value seedTest = cloneElementTest(predicate, expansion.getSrcids(), rowCarried, rewriter);

        rewriter.replaceAllUsesWith(predicate.getResult(), seedTest);
        eraseUnusedRowConeOf(predicate, rewriter);
    }
}

struct FuseExploreListPredicate : public impl::FuseExploreListPredicateBase<FuseExploreListPredicate> {
    void runOnOperation() override {
        runFilterWorklist<ExploreListPredicate>(getOperation(), matchExploreListPredicate, fuseExploreListPredicate);
    }
};

// The ops of a hop region computing a conjunct, in block order, and the latest step whose
// region takes every walk argument they read. Arguments past @param walkArguments are
// imports, which every step takes, and values defined outside the region are constants.
size_t collectConjunctSlice(Value conjunct, Block& block, size_t walkArguments, llvm::SmallVectorImpl<Operation*>& slice) {
    size_t step = 0;
    llvm::SmallPtrSet<Operation*, 16> visited;
    llvm::SmallVector<Value> pending {conjunct};

    while (!pending.empty()) {
        const Value value = pending.pop_back_val();

        if (const BlockArgument argument = dyn_cast<BlockArgument>(value)) {
            const bool readsTheWalk = argument.getOwner() == &block && argument.getArgNumber() < walkArguments;
            if (readsTheWalk) {
                step = std::max(step, firstStepTakingHopArgument(argument.getArgNumber()));
            }

            continue;
        }

        Operation* const op = value.getDefiningOp();
        const bool computedHere = op && op->getBlock() == &block;
        if (!computedHere || !visited.insert(op).second) {
            continue;
        }

        slice.push_back(op);
        op->walk([&pending](Operation* nested) {
            pending.append(nested->operand_begin(), nested->operand_end());
        });
    }

    std::ranges::sort(slice, [](Operation* left, Operation* right) {
        return left->isBeforeInBlock(right);
    });

    return step;
}

// Clones a conjunct of the last step's region into the region of @param step, which takes
// the same walk arguments up to its own end node and the same imports after them
void hoistConjunct(ExplorePaths exploration,
                   Block& lastBlock,
                   size_t lastWalkArguments,
                   Value conjunct,
                   llvm::ArrayRef<Operation*> slice,
                   size_t step,
                   mlir::RewriterBase& rewriter) {
    Block* const hopBlock = hopBlockOf(exploration, step, rewriter);
    const size_t walkArguments = hopWalkArgumentCount(step);

    mlir::IRMapping mapping;
    for (const BlockArgument argument : lastBlock.getArguments()) {
        const size_t argumentNumber = argument.getArgNumber();
        if (argumentNumber < walkArguments) {
            mapping.map(argument, hopBlock->getArgument(static_cast<unsigned>(argumentNumber)));
        } else if (argumentNumber >= lastWalkArguments) {
            const size_t importIndex = argumentNumber - lastWalkArguments;
            mapping.map(argument, hopBlock->getArgument(static_cast<unsigned>(walkArguments + importIndex)));
        }
    }

    const Yield hopYield = hopYieldOf(hopBlock, rewriter);
    for (Operation* const op : slice) {
        rewriter.clone(*op, mapping);
    }

    joinHopTest(hopYield, mapping.lookupOrDefault(conjunct), conjunct.getLoc(), rewriter);
}

// Codegen writes the hop predicate in the last step's region, which takes the whole
// repetition: each conjunct that an earlier step's arguments already cover moves there, so
// the walk drops a prefix as soon as it fails
void hoistHopConjuncts(ExplorePaths exploration, mlir::RewriterBase& rewriter) {
    const size_t lastStep = exploration.getStepCount() - 1;
    Region& lastHop = exploration.getHops()[lastStep];
    if (lastStep == 0 || lastHop.empty()) {
        return;
    }

    Block& lastBlock = lastHop.front();
    Yield yield = dyn_cast<Yield>(lastBlock.getTerminator());
    if (!yield || yield->getNumOperands() != 1) {
        return;
    }

    llvm::SmallVector<Value> conjuncts;
    llvm::SmallVector<AndOp> joins;
    collectHopConjuncts(yield->getOperand(0), conjuncts, joins);

    const size_t lastWalkArguments = hopWalkArgumentCount(lastStep);

    llvm::SmallVector<Value> residual;
    llvm::SmallVector<Operation*> slice;
    for (const Value conjunct : conjuncts) {
        slice.clear();
        const size_t step = collectConjunctSlice(conjunct, lastBlock, lastWalkArguments, slice);

        if (step == lastStep) {
            residual.push_back(conjunct);
        } else {
            hoistConjunct(exploration, lastBlock, lastWalkArguments, conjunct, slice, step, rewriter);
        }
    }

    if (residual.size() == conjuncts.size()) {
        return;
    }

    setHopMask(lastHop, yield, residual, rewriter);
    if (residual.empty()) {
        return;
    }

    llvm::SmallVector<Operation*> computed;
    for (Operation& op : lastBlock.without_terminator()) {
        computed.push_back(&op);
    }

    for (Operation* const op : llvm::reverse(computed)) {
        if (op->use_empty()) {
            rewriter.eraseOp(op);
        }
    }
}

struct HoistHopConjuncts : public impl::HoistHopConjunctsBase<HoistHopConjuncts> {
    void runOnOperation() override {
        mlir::IRRewriter rewriter(&getContext());
        getOperation()->walk([&rewriter](ExplorePaths exploration) {
            hoistHopConjuncts(exploration, rewriter);
        });
    }
};

bool aggregatesDistinctly(llvm::ArrayRef<int64_t> kinds) {
    for (const int64_t kind : kinds) {
        switch (static_cast<storage::GroupAggregateKind>(kind)) {
            case storage::GroupAggregateKind::Min:
            case storage::GroupAggregateKind::Max:
            case storage::GroupAggregateKind::CountDistinct:
            case storage::GroupAggregateKind::SumDistinct:
            case storage::GroupAggregateKind::AvgDistinct:
            case storage::GroupAggregateKind::StDevDistinct:
            case storage::GroupAggregateKind::StDevPDistinct:
            break;

            case storage::GroupAggregateKind::Count:
            case storage::GroupAggregateKind::Sum:
            case storage::GroupAggregateKind::Avg:
            case storage::GroupAggregateKind::CountRows:
            case storage::GroupAggregateKind::StDev:
            case storage::GroupAggregateKind::StDevP:
            case storage::GroupAggregateKind::First:
                return false;
            break;
        }
    }

    return true;
}

// Every list a collect builds dedupes its values, and every aggregate beside them ignores a
// repeated row
bool collectsDistinctly(Collect collect) {
    const llvm::ArrayRef<int64_t> kinds = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {});
    const llvm::ArrayRef<int64_t> distinctValues = collect.getDistinctValues().value_or(llvm::ArrayRef<int64_t> {});
    const llvm::SmallDenseSet<int64_t, 4> dedupedValues(distinctValues.begin(), distinctValues.end());

    return dedupedValues.size() == collectValueCount(collect) && aggregatesDistinctly(kinds);
}

bool readsRowsAsASet(Operation* op, llvm::SmallVectorImpl<Operation*>& passedOn) {
    if (isa<RemoveDuplicates>(op)) {
        return true;
    } else if (Count count = dyn_cast<Count>(op)) {
        return count.getDistinct();
    } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
        return aggregatesDistinctly(groupAggregate.getKinds());
    } else if (Collect collect = dyn_cast<Collect>(op)) {
        return collectsDistinctly(collect);
    } else if (isRowWiseOp(op) || isa<FilterOp, ExplorePaths>(op) || isEdgeHop(op)) {
        passedOn.push_back(op);
        return true;
    }

    return false;
}

// Whether every row the op ever emits is only ever read as a member of a set: a dedup or a
// distinct aggregate settles it, a row-wise op, a filter or a further hop passes the question
// on to its own users, and anything counting, cutting or outputting rows refuses.
bool usersReadRowsAsASet(Operation* op) {
    llvm::SmallPtrSet<Operation*, 16> visited;
    llvm::SmallVector<Operation*> passedOn {op};
    while (!passedOn.empty()) {
        Operation* const producer = passedOn.pop_back_val();

        for (Operation* const user : producer->getUsers()) {
            const bool firstVisit = visited.insert(user).second;
            if (firstVisit && !readsRowsAsASet(user, passedOn)) {
                return false;
            }
        }
    }

    return true;
}

bool matchDistinctEnds(ExplorePaths exploration) {
    if (exploration.getDistinct() || !exploration.getPaths().use_empty()) {
        return false;
    }

    // The search expands a batch of seeds one level at a time, so it has no one row to read
    // a hop import at: a walk whose predicate reads outside the hop stays a walk
    if (!exploration.getHopImports().empty()) {
        return false;
    }

    return usersReadRowsAsASet(exploration.getOperation());
}

struct FuseExploreDistinctEnds : public impl::FuseExploreDistinctEndsBase<FuseExploreDistinctEnds> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        getOperation()->walk([&](ExplorePaths exploration) {
            if (matchDistinctEnds(exploration)) {
                exploration.setDistinctAttr(builder.getUnitAttr());
            }
        });
    }
};

// A count reads no value off a path, and a path is never null, so count(p) tallies the
// rows its build ran over. The tally moves onto the node the path opens on, which the
// build already read: row aligned with the path and never null in its own right.
bool matchBuiltPath(Count count, Value& rows) {
    if (count.getDistinct() || count.getRows()) {
        return false;
    }

    MakePath build = dyn_cast_or_null<MakePath>(count.getInput().getDefiningOp());
    if (!build) {
        return false;
    }

    rows = build.getEntities().front();

    return true;
}

// The list a walk's handles expand to is never null either, so a count of a path or of
// that list tallies the rows of a column its build read: the node the path opens on, or
// the handles themselves
void countBuiltRows(OpOperand& counted) {
    Operation* const build = counted.get().getDefiningOp();

    if (MakePath path = dyn_cast_or_null<MakePath>(build)) {
        counted.set(path.getEntities().front());
    } else if (ExpandPath expansion = dyn_cast_or_null<ExpandPath>(build)) {
        counted.set(expansion.getPaths());
    } else {
        return;
    }

    eraseIfUnused(build);
}

// The aggregate inputs of a grouping op are its trailing operands, one per kind
void countGroupedBuiltRows(Operation* grouping, ArrayRef<int64_t> kinds) {
    const size_t firstInput = grouping->getNumOperands() - kinds.size();
    const int64_t countKind = static_cast<int64_t>(storage::GroupAggregateKind::Count);

    for (size_t index = 0; index < kinds.size(); index++) {
        if (kinds[index] == countKind) {
            countBuiltRows(grouping->getOpOperand(firstInput + index));
        }
    }
}

struct CountPathRows : public impl::CountPathRowsBase<CountPathRows> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<Count> counts;
        llvm::SmallVector<GroupAggregate> groupAggregates;
        llvm::SmallVector<Collect> collects;
        getOperation()->walk([&counts, &groupAggregates, &collects](Operation* op) {
            if (Count count = dyn_cast<Count>(op)) {
                counts.push_back(count);
            } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
                groupAggregates.push_back(groupAggregate);
            } else if (Collect collect = dyn_cast<Collect>(op)) {
                collects.push_back(collect);
            }
        });

        for (Count count : counts) {
            Value rows;
            if (matchBuiltPath(count, rows)) {
                Operation* build = count.getInput().getDefiningOp();

                count->setOperand(0, rows);
                count.setRowsAttr(builder.getUnitAttr());

                if (build->use_empty()) {
                    build->erase();
                }
            } else if (!count.getDistinct()) {
                countBuiltRows(count->getOpOperand(0));
            }
        }

        for (GroupAggregate groupAggregate : groupAggregates) {
            countGroupedBuiltRows(groupAggregate, groupAggregate.getKinds());
        }

        for (Collect collect : collects) {
            const std::optional<ArrayRef<int64_t>> kinds = collect.getKinds();
            if (kinds) {
                countGroupedBuiltRows(collect, *kinds);
            }
        }
    }
};

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

// The argument @param target, a region after @param source, takes @param column of
// @param source as. A column @param source takes itself comes into @param target the same
// way; any other @param source forwards, as an argument of every region after it.
BlockArgument forwardColumn(Region& source, Value column, Region& target, mlir::RewriterBase& rewriter) {
    Block& targetBody = target.front();

    BlockArgument argument = dyn_cast<BlockArgument>(column);
    if (argument && argument.getParentRegion() == &source) {
        Region* forwardingSource = nullptr;
        size_t forwardIndex = 0;
        if (matchForwardedArgument(argument, forwardingSource, forwardIndex)) {
            return targetBody.getArgument(static_cast<unsigned>(forwardedArgumentCount(*forwardingSource) + forwardIndex));
        }

        RegionCarrySet carrySet;
        matchRegionCarrySet(source, carrySet);

        return carriedArgument(target, argument.getArgNumber() - carrySet._argumentOffset);
    }

    Operation* const op = source.getParentOp();
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);
    MutableOperandRange forwarded = carrier.getForwardedColumnsMutable(source);

    const size_t forwardOffset = forwardedArgumentCount(source);
    const OperandRange current = forwarded;
    const size_t forwardIndex = static_cast<size_t>(llvm::find(current, column) - current.begin());

    if (forwardIndex == current.size()) {
        const unsigned position = static_cast<unsigned>(forwardOffset + forwardIndex);
        Operation* const yield = source.front().getTerminator();

        rewriter.modifyOpInPlace(yield, [&forwarded, column] {
            forwarded.append(column);
        });

        for (size_t regionIndex = source.getRegionNumber() + 1; regionIndex < op->getNumRegions(); regionIndex++) {
            Block& later = op->getRegion(static_cast<unsigned>(regionIndex)).front();
            later.insertArgument(position, column.getType(), column.getLoc());
        }
    }

    return targetBody.getArgument(static_cast<unsigned>(forwardOffset + forwardIndex));
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

// A property equality the scan answers in place of the filter, which keeps its other conjuncts
// over the rows the scan by value makes
struct PropertyValueScanChain {
    FilterOp _filter;
    Value _conjunct;
    ScanSource _source;
    StringAttr _property;
    TypedAttr _value;
    Value _boundValue;
};

bool matchPropertyValueScanChain(FilterOp filter, PropertyValueScanChain& chain) {
    if (!matchSoleScanSource(filter, chain._source) || !readsOnlyIntoTheFilter(chain._source._op, filter)) {
        return false;
    }

    // The first property equality in the mask wins, not the most selective one: there are
    // no column statistics to choose with, so `n.gender = 'M' AND n.ssn = '...'` fuses the
    // unselective conjunct and leaves the selective one a residual filter.
    return matchAConjunct(filter, [&filter, &chain](Value conjunct) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        PropertyEquality property;
        if (!equality || !matchPropertyEquality(equality, property)) {
            return false;
        }

        if (property._read.getInputNodes() != chain._source._column) {
            return false;
        }

        chain._filter = filter;
        chain._conjunct = conjunct;
        chain._property = property._read.getPropertyAttr();
        chain._value = property._value;
        chain._boundValue = property._boundValue;

        return true;
    });
}

void fuseScanByPropertyValue(PropertyValueScanChain& chain, mlir::RewriterBase& rewriter) {
    Operation* const scan = chain._source._op;

    rewriter.setInsertionPoint(scan);
    ScanNodesByPropertyValue fused = rewriter.create<ScanNodesByPropertyValue>(chain._filter.getLoc(),
                                                                               chain._source._column.getType(),
                                                                               chain._property,
                                                                               chain._value,
                                                                               chain._boundValue,
                                                                               chain._source._labels);
    computeBoundValuesAhead(fused);

    rewriter.replaceOp(scan, fused.getResult());
    dropConjunct(chain._filter, chain._conjunct, rewriter);
}

struct FuseScanByPropertyValue : public impl::FuseScanByPropertyValueBase<FuseScanByPropertyValue> {
    void runOnOperation() override {
        runFilterWorklist<PropertyValueScanChain>(getOperation(), matchPropertyValueScanChain, fuseScanByPropertyValue);
    }
};

// The ops whose results hold the rows of their operands selected, reordered or repeated as
// a whole, values untouched, so a property read of an operand and carried through is the
// column a read of the result would have produced. group_aggregate and collect are not
// among them - a group's rows are not its input's - and neither is remove_duplicates,
// whose dedup key is every column it carries, so a wider carry set drops different rows.
bool mapsRowsThrough(Operation* op) {
    return isEdgeHop(op) || isa<FilterOp, ExplorePaths, Unwind, Limit, Skip, Sort>(op);
}

bool matchPropertyRead(Operation* op, StringAttr& property, bool& nodeProperty) {
    if (GetNodeProperties read = dyn_cast<GetNodeProperties>(op)) {
        property = read.getPropertyAttr();
        nodeProperty = true;
        return true;
    } else if (GetEdgeProperties read = dyn_cast<GetEdgeProperties>(op)) {
        property = read.getPropertyAttr();
        nodeProperty = false;
        return true;
    }

    return false;
}

// What a read takes off each row of a node or an edge column: the op reading it, and the
// property it names, null for the label sets of a node or the types of an edge
struct EntityRead {
    TypeID _op;
    StringAttr _property;

    bool operator==(const EntityRead& other) const = default;
};

EntityRead entityReadOf(Operation* read) {
    StringAttr property;
    bool nodeProperty = false;
    matchPropertyRead(read, property, nodeProperty);

    return EntityRead {._op = read->getName().getTypeID(), ._property = property};
}

bool matchPropertyWrite(Operation* op, StringAttr& property, bool& nodeProperty) {
    if (SetNodeProperty write = dyn_cast<SetNodeProperty>(op)) {
        property = write.getPropertyAttr();
        nodeProperty = true;
        return true;
    } else if (SetEdgeProperty write = dyn_cast<SetEdgeProperty>(op)) {
        property = write.getPropertyAttr();
        nodeProperty = false;
        return true;
    }

    return false;
}

bool matchPropertiesWrite(Operation* op, bool& nodeProperty) {
    if (isa<SetNodeProperties>(op)) {
        nodeProperty = true;
        return true;
    } else if (isa<SetEdgeProperties>(op)) {
        nodeProperty = false;
        return true;
    }

    return false;
}

Value carryThrough(Operation* op, Value column, mlir::RewriterBase& rewriter) {
    CarrySet carrySet;
    const bool carries = matchCarrySet(op, carrySet);
    bioassert(carries, "A row-mapping op has a carry set");

    for (size_t carriedIndex = 0; carriedIndex < carrySet._count; carriedIndex++) {
        if (op->getOperand(carrySet._operandOffset + carriedIndex) == column) {
            return op->getResult(carrySet._resultOffset + carriedIndex);
        }
    }

    return appendCarriedColumn(op, column, rewriter);
}

Value canonicalArgument(Value rows) {
    BlockArgument argument = dyn_cast<BlockArgument>(rows);
    const Value carried = argument ? carriedColumnOf(argument) : Value();
    if (!carried) {
        return rows;
    }

    Region& region = *argument.getParentRegion();

    RegionCarrySet carrySet;
    matchRegionCarrySet(region, carrySet);

    const OperandRange carriedColumns = region.getParentOp()->getOperands().slice(carrySet._operandOffset, carrySet._count);
    const size_t firstIndex = static_cast<size_t>(llvm::find(carriedColumns, carried) - carriedColumns.begin());

    return carriedArgument(region, firstIndex);
}

// Two columns hold the same values row for row where they come from the same column: two
// arguments of a body carrying one column in, and two results of an op mapping rows through,
// as a hop hands back the node it walks from and carries it where the rows in flight hold it.
// Both are known as the first of them.
Value canonicalRows(Value rows) {
    const OpResult result = dyn_cast<OpResult>(rows);
    Operation* const owner = result ? result.getOwner() : nullptr;
    if (!owner || !mapsRowsThrough(owner)) {
        return canonicalArgument(rows);
    }

    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(owner);
    const Value rowSource = carrier.getRowSource(result);
    if (!rowSource) {
        return rows;
    }

    const Value sourceRows = canonicalArgument(rowSource);
    const auto sibling = llvm::find_if(owner->getResults(), [&carrier, sourceRows](OpResult other) {
        const Value otherSource = carrier.getRowSource(other);
        return otherSource && canonicalArgument(otherSource) == sourceRows;
    });

    return *sibling;
}

// The column the op making @param rows outputs beside them holding what @param entityRead
// takes off each of their rows, null where it outputs none
Value outputReadColumnOf(Value rows, const EntityRead& entityRead) {
    const OpResult result = dyn_cast<OpResult>(rows);
    EdgeTypesOutputOpInterface producer = result ? dyn_cast<EdgeTypesOutputOpInterface>(result.getOwner()) : nullptr;

    const bool readsTheTypes = entityRead == EntityRead {._op = TypeID::get<GetEdgeTypes>()};
    if (!producer || !readsTheTypes) {
        return {};
    }

    return producer.getEdgeTypesOf(result);
}

// The columns reuse_property_reads and carry_label_and_type_reads can hand a later read of
// the same rows: the reads they kept, each serving the ops after it in its block, and the
// columns they carried down to a row column or the op making the rows outputs beside them,
// which serve every op reading those rows. Keyed by the row column, so a read finds an
// earlier one without visiting every other user of its rows
class KnownReadColumns {
public:
    Value find(Value rows, const EntityRead& entityRead, Operation* useSite) const {
        const Value canonical = canonicalRows(rows);
        if (const Value output = outputReadColumnOf(canonical, entityRead)) {
            return output;
        }

        const auto rowsIt = _columns.find(canonical);
        if (rowsIt == _columns.end()) {
            return {};
        }

        Value carriedColumn;
        for (const KnownColumn& known : rowsIt->second) {
            if (known._entityRead != entityRead) {
                continue;
            }

            if (!known._read) {
                carriedColumn = carriedColumn ? carriedColumn : known._column;
                continue;
            }

            const bool standsBeforeTheUse = known._read->getBlock() == useSite->getBlock()
                                            && known._read->isBeforeInBlock(useSite);
            if (standsBeforeTheUse) {
                return known._column;
            }
        }

        return carriedColumn;
    }

    void addRead(Operation* read) {
        _columns[canonicalRows(read->getOperand(0))].push_back(KnownColumn {entityReadOf(read), read->getResult(0), read});
    }

    void addCarried(Value rows, const EntityRead& entityRead, Value column) {
        _columns[canonicalRows(rows)].push_back(KnownColumn {entityRead, column, nullptr});
    }

    // Moves what is known of @param replaced, the results of an op since rebuilt, onto
    // @param replacements, the results standing for them in the op rebuilt
    void rebind(llvm::ArrayRef<Value> replaced, llvm::ArrayRef<Value> replacements) {
        llvm::DenseMap<Value, Value> replacementOf;
        for (size_t index = 0; index < replaced.size(); index++) {
            replacementOf[replaced[index]] = replacements[index];
        }

        for (size_t index = 0; index < replaced.size(); index++) {
            const auto rowsIt = _columns.find(replaced[index]);
            if (rowsIt == _columns.end()) {
                continue;
            }

            llvm::SmallVector<KnownColumn, 1> known = rowsIt->second;
            _columns.erase(rowsIt);

            for (KnownColumn& column : known) {
                const auto columnIt = replacementOf.find(column._column);
                if (columnIt != replacementOf.end()) {
                    column._column = columnIt->second;
                }
            }

            _columns[replacements[index]] = known;
        }
    }

private:
    struct KnownColumn {
        EntityRead _entityRead;
        Value _column;
        Operation* _read {nullptr};
    };

    llvm::DenseMap<Value, llvm::SmallVector<KnownColumn, 1>> _columns;
};

enum class RowsStepKind {
    MapsRows,
    IntoRegion,
    FromForwardingRegion,
    ReadsRowConstant,
    LaysOutConstant,
    OutOfRegion,
    RunConstantOutOfRegion,
    HandsBack,
};

// The rows _rows come from _outerRows across one op: _op maps them through or reads them as a
// run constant, or they come into _body, out of its yield, out of the op holding it, or out of
// _body, a region before theirs that forwards columns. Into the body, from a forwarding region
// and handed back, _position is the column's place in the op's region carry set.
struct RowsStep {
    RowsStepKind _kind {RowsStepKind::MapsRows};
    Operation* _op {nullptr};
    Block* _body {nullptr};
    size_t _position {0};
    Value _rows;
    Value _outerRows;
};

MutableOperandRange yieldedColumnsOf(Block& body) {
    Operation* const terminator = body.getTerminator();

    if (Yield yield = dyn_cast<Yield>(terminator)) {
        return yield.getColumnsMutable();
    } else if (SubqueryYield yield = dyn_cast<SubqueryYield>(terminator)) {
        return yield.getColumnsMutable();
    }

    OptionalYield yield = cast<OptionalYield>(terminator);

    return yield.getColumnsMutable();
}

bool matchMapsRowsStep(OpResult result, RowsStep& step) {
    Operation* const carrier = result.getOwner();
    if (!mapsRowsThrough(carrier)) {
        return false;
    }

    const Value rowSource = cast<ColumnCarrierOpInterface>(carrier).getRowSource(result);
    if (!rowSource) {
        return false;
    }

    step = RowsStep {._op = carrier, ._position = result.getResultNumber(), ._rows = result, ._outerRows = rowSource};

    return true;
}

bool matchHandedBackResult(OpResult result, RowsStep& step) {
    Operation* const op = result.getOwner();
    if (op->getNumRegions() == 0) {
        return false;
    }

    Region& region = op->getRegion(0);

    RegionCarrySet carrySet;
    const size_t carriedIndex = result.getResultNumber();
    const bool handsBack = matchRegionCarrySet(region, carrySet) && carrySet._handsBack && carriedIndex < carrySet._count;
    if (!handsBack) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::HandsBack,
                     ._body = &region.front(),
                     ._position = carriedIndex,
                     ._rows = result,
                     ._outerRows = op->getOperand(carrySet._operandOffset + carriedIndex)};

    return true;
}

// The steps from the column @param body yields back for its carried column at @param carriedIndex
// down to the argument it comes in as, nearest the yield first: ops mapping rows through, and
// ops handing back a column they carry in
bool matchHandedBackChain(Block& body, size_t carriedIndex, llvm::SmallVectorImpl<RowsStep>& chain) {
    const Value argument = canonicalArgument(carriedArgument(*body.getParent(), carriedIndex));
    const OperandRange yielded = yieldedColumnsOf(body);

    Value rows = yielded[carriedIndex];
    while (canonicalArgument(rows) != argument) {
        const OpResult result = dyn_cast<OpResult>(rows);

        RowsStep step;
        const bool stepsInTheBody = result && result.getOwner()->getBlock() == &body
                                 && (matchMapsRowsStep(result, step) || matchHandedBackResult(result, step));
        if (!stepsInTheBody) {
            return false;
        }

        chain.push_back(step);
        rows = step._outerRows;
    }

    return true;
}

// Whether a column carried in alongside the one at @param carriedIndex can be handed back: along
// the steps the body hands that one back through, and through the body of each op among them
// that yields its carried columns back too
bool handsBackAlong(Block& body, const RegionCarrySet& carrySet, size_t carriedIndex) {
    if (!carrySet._yieldsBack) {
        return true;
    }

    llvm::SmallVector<std::pair<Block*, size_t>> pending {{&body, carriedIndex}};
    while (!pending.empty()) {
        const auto [handingBody, handedIndex] = pending.pop_back_val();

        llvm::SmallVector<RowsStep> chain;
        if (!matchHandedBackChain(*handingBody, handedIndex, chain)) {
            return false;
        }

        for (const RowsStep& step : chain) {
            RegionCarrySet stepCarrySet;
            const bool yieldsBack = step._kind == RowsStepKind::HandsBack
                                 && matchRegionCarrySet(*step._body->getParent(), stepCarrySet)
                                 && stepCarrySet._yieldsBack;
            if (yieldsBack) {
                pending.push_back({step._body, step._position});
            }
        }
    }

    return true;
}

bool matchRowConstantStep(OpResult result, RowsStep& step) {
    RowConstant constant = dyn_cast<RowConstant>(result.getOwner());
    if (!constant) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::ReadsRowConstant,
                     ._op = constant.getOperation(),
                     ._rows = result,
                     ._outerRows = constant.getRow()};

    return true;
}

bool matchIntoRegionStep(BlockArgument argument, RowsStep& step) {
    const Value carried = carriedColumnOf(argument);
    if (!carried) {
        return false;
    }

    Block* const body = argument.getOwner();

    RegionCarrySet carrySet;
    matchRegionCarrySet(*body->getParent(), carrySet);

    const size_t carriedIndex = argument.getArgNumber() - carrySet._argumentOffset;
    if (!handsBackAlong(*body, carrySet, carriedIndex)) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::IntoRegion,
                     ._body = body,
                     ._position = carriedIndex,
                     ._rows = argument,
                     ._outerRows = carried};

    return true;
}

bool matchOutOfRegionStep(OpResult result, RowsStep& step) {
    Operation* const op = result.getOwner();
    ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(op);
    if (!carrier) {
        return false;
    }

    const size_t resultIndex = result.getResultNumber();
    for (Region& region : op->getRegions()) {
        size_t resultOffset = 0;
        if (!carrier.getYieldedResultOffset(region, resultOffset)) {
            continue;
        }

        Block& body = region.front();
        const OperandRange yielded = yieldedColumnsOf(body);

        const bool comesOutOfTheRegion = resultIndex >= resultOffset && resultIndex < resultOffset + yielded.size();
        if (comesOutOfTheRegion) {
            const size_t yieldIndex = resultIndex - resultOffset;
            step = RowsStep {._kind = RowsStepKind::OutOfRegion,
                             ._body = &body,
                             ._position = yieldIndex,
                             ._rows = result,
                             ._outerRows = yielded[yieldIndex]};

            return true;
        }
    }

    return false;
}

bool matchHandsBackStep(OpResult result, RowsStep& step) {
    if (!matchHandedBackResult(result, step)) {
        return false;
    }

    Block& body = *step._body;

    RegionCarrySet carrySet;
    matchRegionCarrySet(*body.getParent(), carrySet);

    return handsBackAlong(body, carrySet, step._position);
}

Value runConstantArgument(Value column) {
    RowConstant constant = column ? column.getDefiningOp<RowConstant>() : RowConstant();

    return constant ? canonicalArgument(constant.getRow()) : Value();
}

// The column of the rows @param value holds one value for: itself, or for a value computed per
// row, the first column it is computed over that is not a constant
Value rowsOf(Value value, Block& body) {
    Value rows = value;
    while (true) {
        Operation* const op = rows.getDefiningOp();
        if (!op || op->getBlock() != &body || !computesAValuePerRow(op)) {
            return rows;
        }

        const OperandRange operands = op->getOperands();
        const auto rowOperand = llvm::find_if(operands, [](Value operand) {
            return !::db::yieldsConstantColumn(operand);
        });
        if (rowOperand == operands.end()) {
            return rows;
        }

        rows = *rowOperand;
    }
}

// The argument whose run constant @param column holds on every row, mapped down from it by the
// ops of @param body; null where it holds none
Value heldRunConstantArgument(Value column, Block& body) {
    Value rows = column;
    while (rows) {
        if (const Value argument = runConstantArgument(rows)) {
            return argument;
        }

        const OpResult result = dyn_cast<OpResult>(rows);
        Operation* const op = result ? result.getOwner() : nullptr;

        const bool mapsRowsInTheBody = op && op->getBlock() == &body && mapsRowsThrough(op);
        if (!mapsRowsInTheBody) {
            return {};
        }

        rows = cast<ColumnCarrierOpInterface>(op).getRowSource(result);
    }

    return {};
}

Value runConstantOf(Block& body, Value argument) {
    for (const BlockArgument candidate : body.getArguments()) {
        if (canonicalArgument(candidate) != argument) {
            continue;
        }

        for (Operation* const user : candidate.getUsers()) {
            if (RowConstant constant = dyn_cast<RowConstant>(user)) {
                return constant.getResult();
            }
        }
    }

    return {};
}

// The rows @param body yields, as the first yielded column holding one value per row is over
Value yieldedRowsOf(Block& body) {
    const OperandRange yielded = yieldedColumnsOf(body);
    for (const Value column : yielded) {
        const Value rows = rowsOf(column, body);
        if (!::db::yieldsConstantColumn(rows)) {
            return rows;
        }
    }

    return {};
}

// The ops mapping rows through from the rows of @param yielded, a column @param body yields,
// down to the first with a result holding the run constant read off @param argument, which
// @param holder is set to; nearest the yield first
bool matchRunConstantChain(Block& body, Value yielded, Value argument, llvm::SmallVectorImpl<Operation*>& chain, Value& holder) {
    Value rows = rowsOf(yielded, body);
    while (rows) {
        const OpResult result = dyn_cast<OpResult>(rows);
        Operation* const op = result ? result.getOwner() : nullptr;

        const bool mapsRowsInTheBody = op && op->getBlock() == &body && mapsRowsThrough(op);
        if (!mapsRowsInTheBody) {
            return false;
        }

        const auto holding = llvm::find_if(op->getResults(), [&body, argument](OpResult other) {
            return heldRunConstantArgument(other, body) == argument;
        });

        if (holding != op->getResults().end()) {
            holder = *holding;
            return true;
        }

        rows = cast<ColumnCarrierOpInterface>(op).getRowSource(result);
        chain.push_back(op);
    }

    return false;
}

// The steps from the rows a body run per row yields down to a column holding the run constant
// its carried column at @param carriedIndex is read as, along the first yielded column they
// lead from. Where no column holds it, @param holder is the run constant itself, which is laid
// out over the yielded rows.
bool matchRunConstantChainOf(Block& body, size_t carriedIndex, llvm::SmallVectorImpl<Operation*>& chain, Value& holder) {
    const Value argument = canonicalArgument(carriedArgument(*body.getParent(), carriedIndex));

    const OperandRange yielded = yieldedColumnsOf(body);
    for (const Value column : yielded) {
        chain.clear();
        if (matchRunConstantChain(body, column, argument, chain, holder)) {
            return true;
        }
    }

    chain.clear();
    holder = runConstantOf(body, argument);

    return holder && yieldedRowsOf(body);
}

// A body run per row yields rows of one run, so the carried column a result hands back holds
// on each of them the run constant the body reads it as. The rows come out of the body from
// a column holding that constant, which the yielded rows map from.
bool matchRunConstantOutOfRegionStep(OpResult result, RowsStep& step) {
    RowsStep handsBack;
    if (!matchHandedBackResult(result, handsBack)) {
        return false;
    }

    Block& body = *handsBack._body;
    Region& region = *body.getParent();

    RegionCarrySet carrySet;
    matchRegionCarrySet(region, carrySet);

    size_t resultOffset = 0;
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(result.getOwner());
    const bool yieldsRowForRow = !carrySet._yieldsBack && carrier.getYieldedResultOffset(region, resultOffset);
    if (!yieldsRowForRow) {
        return false;
    }

    llvm::SmallVector<Operation*> chain;
    Value holder;
    if (!matchRunConstantChainOf(body, handsBack._position, chain, holder)) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::RunConstantOutOfRegion,
                     ._op = chain.empty() ? body.getTerminator() : chain.back(),
                     ._body = &body,
                     ._position = handsBack._position,
                     ._rows = result,
                     ._outerRows = holder};

    return true;
}

// A non-optional CALL hands its carried columns back out of its body, so a result doing both
// steps out of the body: a read there reaches it, and a read before the op through the body
bool matchRowsStep(Value rows, RowsStep& step) {
    if (const BlockArgument argument = dyn_cast<BlockArgument>(rows)) {
        return matchIntoRegionStep(argument, step);
    }

    const OpResult result = cast<OpResult>(rows);

    return matchMapsRowsStep(result, step)
        || matchRowConstantStep(result, step)
        || matchOutOfRegionStep(result, step)
        || matchRunConstantOutOfRegionStep(result, step)
        || matchHandsBackStep(result, step);
}

// A walk into a body through the run constant a result hands back, and out of it through the
// argument that constant is read off, only passes the op: one step handing the column back
void collapseRunConstantRoundTrips(llvm::SmallVectorImpl<RowsStep>& steps) {
    for (size_t stepIndex = 0; stepIndex < steps.size(); stepIndex++) {
        if (steps[stepIndex]._kind != RowsStepKind::RunConstantOutOfRegion) {
            continue;
        }

        Block* const body = steps[stepIndex]._body;
        const auto exitIt = std::find_if(steps.begin() + stepIndex + 1, steps.end(), [body](const RowsStep& step) {
            return step._kind == RowsStepKind::IntoRegion && step._body == body;
        });
        if (exitIt == steps.end()) {
            continue;
        }

        RowsStep handsBack;
        const bool handsBackTheColumn = matchHandedBackResult(cast<OpResult>(steps[stepIndex]._rows), handsBack);
        bioassert(handsBackTheColumn, "A run constant comes out of a body as a column its op hands back");

        steps.erase(steps.begin() + stepIndex + 1, exitIt + 1);
        steps[stepIndex] = handsBack;
    }
}

// The op a column of the rows a step reaches has to stand before
Operation* outerUseSite(const RowsStep& step) {
    switch (step._kind) {
        case RowsStepKind::MapsRows:
        case RowsStepKind::ReadsRowConstant:
        case RowsStepKind::RunConstantOutOfRegion:
            return step._op;
        break;
        case RowsStepKind::IntoRegion:
        case RowsStepKind::HandsBack:
            return step._body->getParentOp();
        break;
        case RowsStepKind::OutOfRegion:
        case RowsStepKind::FromForwardingRegion:
            return step._body->getTerminator();
        break;
        case RowsStepKind::LaysOutConstant:
            return nullptr;
        break;
    }

    return nullptr;
}

// A run constant holds its one value for every row of its body, so a column read off it
// reaches the rows mapped from it without being carried, laid out over the rows the walk
// mapped down from
void layOutRunConstant(llvm::SmallVectorImpl<RowsStep>& steps) {
    size_t firstMapping = steps.size();
    while (firstMapping > 0 && steps[firstMapping - 1]._kind == RowsStepKind::MapsRows) {
        firstMapping--;
    }

    if (firstMapping == steps.size()) {
        return;
    }

    const Value entryRows = steps[firstMapping]._rows;
    steps.truncate(firstMapping);
    steps.push_back(RowsStep {._kind = RowsStepKind::LaysOutConstant, ._rows = entryRows});
}

// An op handing back the column it carries in at _carriedIndex for the rows of the one at
// _rowsIndex, and that column carried up _chain toward the body's yield, _nextStep steps of
// it still to go
struct HandBack {
    Operation* _op {nullptr};
    size_t _carriedIndex {0};
    size_t _rowsIndex {0};
    llvm::SmallVector<RowsStep> _chain;
    size_t _nextStep {0};
    Value _column;
};

// Hands a read the column holding what it reads of its rows: one read before it, or one read
// of rows its own come from, carried down to it through the ops between
class ReadColumnWalk {
public:
    ReadColumnWalk(KnownReadColumns& known, mlir::RewriterBase& rewriter)
        : _known(known),
          _rewriter(rewriter)
    {
    }

    // Null where there is none: this adds no read. A walk starts again once handing a column
    // back through a region has rebuilt ops a step of it stood on.
    Value columnOf(Operation* read) {
        _entityRead = entityReadOf(read);

        Value column;
        bool rebuilt = true;
        while (rebuilt) {
            llvm::SmallVector<RowsStep> steps;
            column = findKnownColumn(read, steps);
            if (!column) {
                return {};
            }

            rebuilt = false;
            for (size_t stepIndex = steps.size(); stepIndex > 0 && !rebuilt; stepIndex--) {
                column = carryBack(steps[stepIndex - 1], column, rebuilt);
            }
        }

        return column;
    }

private:
    KnownReadColumns& _known;
    mlir::RewriterBase& _rewriter;
    EntityRead _entityRead;

    Value findKnownColumn(Operation* read, llvm::SmallVectorImpl<RowsStep>& steps) {
        Value rows = read->getOperand(0);
        Operation* useSite = read;

        while (true) {
            if (isa_and_present<RowConstant>(rows.getDefiningOp())) {
                layOutRunConstant(steps);
            }

            const Value known = _known.find(rows, _entityRead, useSite);
            if (known) {
                collapseRunConstantRoundTrips(steps);
                return known;
            }

            RowsStep step;
            if (!matchRowsStep(rows, step)) {
                return {};
            }

            RowsStep forwarding;
            if (const Value forwarded = findInForwardingRegion(step, forwarding)) {
                steps.push_back(forwarding);
                collapseRunConstantRoundTrips(steps);
                return forwarded;
            }

            steps.push_back(step);
            rows = step._outerRows;
            useSite = outerUseSite(step);
        }
    }

    // The column a region before the one @param intoRegion steps into holds for the same
    // carried rows, nearest first, where that region forwards columns
    Value findInForwardingRegion(const RowsStep& intoRegion, RowsStep& forwarding) {
        if (intoRegion._kind != RowsStepKind::IntoRegion) {
            return {};
        }

        Region& region = *intoRegion._body->getParent();
        Operation* const op = region.getParentOp();
        ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);

        for (size_t regionIndex = region.getRegionNumber(); regionIndex > 0; regionIndex--) {
            Region& earlier = op->getRegion(static_cast<unsigned>(regionIndex - 1));
            if (!carrier.forwardsColumns(earlier)) {
                continue;
            }

            Block& earlierBody = earlier.front();
            const BlockArgument earlierRows = carriedArgument(earlier, intoRegion._position);

            const Value known = _known.find(earlierRows, _entityRead, earlierBody.getTerminator());
            if (known) {
                forwarding = RowsStep {._kind = RowsStepKind::FromForwardingRegion,
                                       ._body = &earlierBody,
                                       ._position = intoRegion._position,
                                       ._rows = intoRegion._rows,
                                       ._outerRows = earlierRows};
                return known;
            }
        }

        return {};
    }

    Value carryBack(const RowsStep& step, Value column, bool& rebuilt) {
        switch (step._kind) {
            case RowsStepKind::MapsRows:
                return carryThroughRows(step, column);
            break;
            case RowsStepKind::IntoRegion:
                return carryIntoBody(step, column, rebuilt);
            break;
            case RowsStepKind::FromForwardingRegion:
                return forwardIntoRegion(step, column);
            break;
            case RowsStepKind::ReadsRowConstant:
                return readAsRowConstant(step, column);
            break;
            case RowsStepKind::LaysOutConstant:
                return layOutConstant(step, column);
            break;
            case RowsStepKind::OutOfRegion:
                return carryOutOf(*step._body, column);
            break;
            case RowsStepKind::RunConstantOutOfRegion:
                return carryRunConstantOutOfBody(step, column);
            break;
            case RowsStepKind::HandsBack:
                return carryThroughBody(step, column);
            break;
        }

        return column;
    }

    Value carryThroughRows(const RowsStep& step, Value column) {
        const Value carried = carryThroughOp(step._op, column);
        Operation* const carrying = carried.getDefiningOp();

        _known.addCarried(carrying->getResult(static_cast<unsigned>(step._position)), _entityRead, carried);

        return carried;
    }

    Value carryThroughOp(Operation* carrier, Value column) {
        const llvm::SmallVector<Value> carrierResults(carrier->getResults());

        const Value carried = carryThrough(carrier, column, _rewriter);

        Operation* const carrying = carried.getDefiningOp();
        if (carrying != carrier) {
            llvm::SmallVector<Value> replacements;
            for (const Value result : carrying->getResults()) {
                if (result != carried) {
                    replacements.push_back(result);
                }
            }

            _known.rebind(carrierResults, replacements);
        }

        return carried;
    }

    Value carryIntoBody(const RowsStep& step, Value column, bool& rebuilt) {
        Block& body = *step._body;
        Operation* const op = body.getParentOp();

        bool appended = false;
        const size_t carriedIndex = carryIntoRegions(op, column, _rewriter, appended);

        RegionCarrySet carrySet;
        matchRegionCarrySet(*body.getParent(), carrySet);

        if (appended && carrySet._handsBack) {
            handBack(op, carriedIndex, step._position);
            rebuilt = carrySet._yieldsBack;
        }

        const BlockArgument argument = carriedArgument(*body.getParent(), carriedIndex);
        _known.addCarried(step._rows, _entityRead, argument);

        return argument;
    }

    Value forwardIntoRegion(const RowsStep& step, Value column) {
        Region& source = *step._body->getParent();
        Region& target = *cast<BlockArgument>(step._rows).getParentRegion();

        const BlockArgument argument = forwardColumn(source, column, target, _rewriter);
        _known.addCarried(step._rows, _entityRead, argument);

        return argument;
    }

    Value carryThroughBody(const RowsStep& step, Value column) {
        Operation* op = step._body->getParentOp();

        bool appended = false;
        const size_t carriedIndex = carryIntoRegions(op, column, _rewriter, appended);
        if (appended) {
            op = handBack(op, carriedIndex, step._position);
        }

        return op->getResult(static_cast<unsigned>(carriedIndex));
    }

    // @param op hands back the column it has just carried in at @param carriedIndex as a new
    // result. A body yielding its carried columns back yields it along the one at
    // @param rowsIndex, carried through each step between, and an op among them handing back
    // what it carries in hands this column back the same way before the steps above it go on.
    Operation* handBack(Operation* op, size_t carriedIndex, size_t rowsIndex) {
        llvm::SmallVector<HandBack> handBacks;
        pushHandBack(op, carriedIndex, rowsIndex, handBacks);

        while (true) {
            HandBack& current = handBacks.back();

            if (current._nextStep == 0) {
                const size_t handedIndex = current._carriedIndex;
                Operation* const rebuilt = finishHandBack(current);

                handBacks.pop_back();
                if (handBacks.empty()) {
                    return rebuilt;
                }

                handBacks.back()._column = rebuilt->getResult(static_cast<unsigned>(handedIndex));
                continue;
            }

            current._nextStep--;
            const RowsStep step = current._chain[current._nextStep];

            if (step._kind == RowsStepKind::MapsRows) {
                current._column = carryThroughOp(step._op, current._column);
                continue;
            }

            Operation* const handing = step._body->getParentOp();

            bool appended = false;
            const size_t handedIndex = carryIntoRegions(handing, current._column, _rewriter, appended);
            if (appended) {
                pushHandBack(handing, handedIndex, step._position, handBacks);
            } else {
                current._column = handing->getResult(static_cast<unsigned>(handedIndex));
            }
        }
    }

    void pushHandBack(Operation* op, size_t carriedIndex, size_t rowsIndex, llvm::SmallVectorImpl<HandBack>& handBacks) {
        Region& region = op->getRegion(0);

        RegionCarrySet carrySet;
        matchRegionCarrySet(region, carrySet);

        HandBack handBack {._op = op, ._carriedIndex = carriedIndex, ._rowsIndex = rowsIndex};
        if (carrySet._yieldsBack) {
            const bool chained = matchHandedBackChain(region.front(), rowsIndex, handBack._chain);
            bioassert(chained, "A column is carried into a body only along one it yields back");

            handBack._nextStep = handBack._chain.size();
            handBack._column = carriedArgument(region, carriedIndex);
        }

        handBacks.push_back(handBack);
    }

    Operation* finishHandBack(const HandBack& handBack) {
        Operation* const op = handBack._op;
        Region& region = op->getRegion(0);

        RegionCarrySet carrySet;
        matchRegionCarrySet(region, carrySet);

        if (carrySet._yieldsBack) {
            insertYieldedColumn(region.front(), handBack._carriedIndex, handBack._column);
        }

        const Value column = op->getOperand(static_cast<unsigned>(carrySet._operandOffset + handBack._carriedIndex));
        Operation* const rebuilt = insertResult(op, handBack._carriedIndex, column.getType());

        _known.addCarried(rebuilt->getResult(static_cast<unsigned>(handBack._rowsIndex)),
                          _entityRead,
                          rebuilt->getResult(static_cast<unsigned>(handBack._carriedIndex)));

        return rebuilt;
    }

    Value readAsRowConstant(const RowsStep& step, Value column) {
        _rewriter.setInsertionPointAfter(step._op);
        const Value constant = _rewriter.create<RowConstant>(step._op->getLoc(), column).getResult();

        _known.addCarried(step._rows, _entityRead, constant);

        return constant;
    }

    Value layOutConstant(const RowsStep& step, Value constant) {
        _rewriter.setInsertionPointAfterValue(step._rows);
        const Value laidOut = _rewriter.create<BroadcastConstant>(step._rows.getLoc(), constant, step._rows).getResult();

        _known.addCarried(step._rows, _entityRead, laidOut);

        return laidOut;
    }

    Value carryRunConstantOutOfBody(const RowsStep& step, Value column) {
        Block& body = *step._body;

        llvm::SmallVector<Operation*> chain;
        Value holder;
        const bool chained = matchRunConstantChainOf(body, step._position, chain, holder);
        bioassert(chained, "A run constant comes out of a body along the rows it yields");

        if (runConstantArgument(holder)) {
            Operation* const terminator = body.getTerminator();
            _rewriter.setInsertionPoint(terminator);

            const Value rows = yieldedRowsOf(body);
            const Value laidOut = _rewriter.create<BroadcastConstant>(terminator->getLoc(), column, rows).getResult();

            return carryOutOf(body, laidOut);
        }

        Value carried = column;
        for (size_t chainIndex = chain.size(); chainIndex > 0; chainIndex--) {
            carried = carryThroughOp(chain[chainIndex - 1], carried);
        }

        return carryOutOf(body, carried);
    }

    Value carryOutOf(Block& body, Value column) {
        Operation* op = body.getParentOp();

        size_t resultOffset = 0;
        const bool yields = cast<ColumnCarrierOpInterface>(op).getYieldedResultOffset(*body.getParent(), resultOffset);
        bioassert(yields, "A column comes out of a region whose yield the op's results stand for");

        const OperandRange yielded = yieldedColumnsOf(body);
        const auto yieldedIt = llvm::find(yielded, column);
        const size_t yieldIndex = static_cast<size_t>(yieldedIt - yielded.begin());

        if (yieldedIt == yielded.end()) {
            insertYieldedColumn(body, yieldIndex, column);
            op = insertResult(op, resultOffset + yieldIndex, column.getType());
        }

        return op->getResult(static_cast<unsigned>(resultOffset + yieldIndex));
    }

    void insertYieldedColumn(Block& body, size_t yieldIndex, Value column) {
        MutableOperandRange yielded = yieldedColumnsOf(body);
        const OperandRange current = yielded;

        llvm::SmallVector<Value> columns(current.begin(), current.end());
        columns.insert(columns.begin() + yieldIndex, column);

        _rewriter.modifyOpInPlace(body.getTerminator(), [&yielded, &columns] {
            yielded.assign(columns);
        });
    }

    // Rebuilds @param op with a result of @param type at @param resultIndex, the ones from there
    // on moving up one, and its regions moved over
    Operation* insertResult(Operation* op, size_t resultIndex, Type type) {
        llvm::SmallVector<Type> types(op->getResultTypes());
        types.insert(types.begin() + resultIndex, type);

        OperationState state(op->getLoc(), op->getName());
        state.addOperands(op->getOperands());
        state.addTypes(types);
        state.addAttributes(op->getAttrs());

        Operation* const rebuilt = rebuildOp(op, state, _rewriter);

        const llvm::SmallVector<Value> replaced(op->getResults());
        llvm::SmallVector<Value> replacements;
        for (size_t index = 0; index < replaced.size(); index++) {
            const size_t rebuiltIndex = index < resultIndex ? index : index + 1;
            replacements.push_back(rebuilt->getResult(static_cast<unsigned>(rebuiltIndex)));
        }

        _known.rebind(replaced, replacements);
        _rewriter.replaceOp(op, replacements);

        return rebuilt;
    }
};

void collectReads(Operation* root, llvm::function_ref<bool(Operation*)> isShared, llvm::SmallVectorImpl<Operation*>& reads) {
    root->walk([isShared, &reads](Operation* op) {
        if (isShared(op)) {
            reads.push_back(op);
        }
    });
}

using RegionReaderKey = std::tuple<Block*, Value, TypeID, Attribute>;

RegionReaderKey regionReaderKey(Block* block, Value rows, const EntityRead& entityRead) {
    return RegionReaderKey {block, canonicalRows(rows), entityRead._op, entityRead._property};
}

// The first op of a block whose regions read what a read takes off rows of that block, found
// along the steps the walk climbs from each read inside a region
void collectRegionReaders(llvm::ArrayRef<Operation*> reads, llvm::DenseMap<RegionReaderKey, Operation*>& readers) {
    llvm::DenseSet<std::tuple<Value, TypeID, Attribute>> climbed;
    for (Operation* const read : reads) {
        const EntityRead entityRead = entityReadOf(read);

        Value rows = read->getOperand(0);
        RowsStep step;
        while (matchRowsStep(rows, step)) {
            if (step._kind == RowsStepKind::IntoRegion) {
                Operation* const reader = step._body->getParentOp();
                const RegionReaderKey key = regionReaderKey(reader->getBlock(), step._outerRows, entityRead);

                const auto [readerIt, isFirst] = readers.try_emplace(key, reader);
                if (!isFirst && reader->isBeforeInBlock(readerIt->second)) {
                    readerIt->second = reader;
                }
            }

            rows = step._outerRows;
            if (!climbed.insert({rows, entityRead._op, entityRead._property}).second) {
                break;
            }
        }
    }
}

// A read runs over every row of its rows wherever it stands in their block, so one behind an
// op whose regions read the same moves in front of it, where the walk carries it in
void hoistReadsAboveRegionReaders(llvm::ArrayRef<Operation*> reads, mlir::RewriterBase& rewriter) {
    llvm::DenseMap<RegionReaderKey, Operation*> readers;
    collectRegionReaders(reads, readers);

    for (Operation* const read : reads) {
        const auto readerIt = readers.find(regionReaderKey(read->getBlock(), read->getOperand(0), entityReadOf(read)));
        if (readerIt != readers.end() && readerIt->second->isBeforeInBlock(read)) {
            rewriter.moveOpBefore(read, readerIt->second);
        }
    }
}

// Hands each read @param isShared picks the column a read before it, or one of rows its own
// come from, already holds, carried down to it
void shareReads(Operation* root, llvm::function_ref<bool(Operation*)> isShared, mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Operation*> reads;
    collectReads(root, isShared, reads);
    hoistReadsAboveRegionReaders(reads, rewriter);

    reads.clear();
    collectReads(root, isShared, reads);

    KnownReadColumns knownColumns;
    ReadColumnWalk walk(knownColumns, rewriter);
    for (Operation* const read : reads) {
        const Value shared = walk.columnOf(read);
        if (!shared) {
            knownColumns.addRead(read);
            continue;
        }

        rewriter.replaceOp(read, shared);
    }
}

struct ReusePropertyReads : public impl::ReusePropertyReadsBase<ReusePropertyReads> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        llvm::DenseSet<Attribute> writtenNodeProperties;
        llvm::DenseSet<Attribute> writtenEdgeProperties;
        bool writesEveryNodeProperty = false;
        bool writesEveryEdgeProperty = false;
        root->walk([&](Operation* op) {
            StringAttr writtenProperty;
            bool writesNodes = false;

            if (matchPropertyWrite(op, writtenProperty, writesNodes)) {
                if (writesNodes) {
                    writtenNodeProperties.insert(writtenProperty);
                } else {
                    writtenEdgeProperties.insert(writtenProperty);
                }
            } else if (matchPropertiesWrite(op, writesNodes)) {
                if (writesNodes) {
                    writesEveryNodeProperty = true;
                } else {
                    writesEveryEdgeProperty = true;
                }
            }
        });

        const auto isShared = [&](Operation* op) {
            StringAttr property;
            bool nodeProperty = false;
            if (!matchPropertyRead(op, property, nodeProperty)) {
                return false;
            }

            // A read standing before this one saw what the property held before the write,
            // and a projection behind a SET reads what the statement wrote
            const llvm::DenseSet<Attribute>& written = nodeProperty ? writtenNodeProperties : writtenEdgeProperties;
            const bool writesEveryProperty = nodeProperty ? writesEveryNodeProperty : writesEveryEdgeProperty;

            return !writesEveryProperty && !written.contains(property);
        };

        mlir::IRRewriter rewriter(&getContext());
        shareReads(root, isShared, rewriter);
    }
};

bool isLabelOrTypeRead(Operation* op) {
    return isa<GetNodeLabelSet, GetEdgeTypes>(op);
}

// Codegen reads the label sets of a node column, or the types of an edge column, once per
// test over it, and a predicate no check fusion folds keeps every one of those reads.
struct ReuseLabelAndTypeReads : public impl::ReuseLabelAndTypeReadsBase<ReuseLabelAndTypeReads> {
    void runOnOperation() override {
        llvm::SmallVector<Operation*> reads;
        collectReads(getOperation(), isLabelOrTypeRead, reads);

        mlir::IRRewriter rewriter(&getContext());
        llvm::DenseMap<std::tuple<Block*, Value, OperationName>, Operation*> firstReads;
        for (Operation* const read : reads) {
            if (const Value output = outputReadColumnOf(read->getOperand(0), entityReadOf(read))) {
                rewriter.replaceOp(read, output);
                continue;
            }

            const std::tuple<Block*, Value, OperationName> key {read->getBlock(), read->getOperand(0), read->getName()};
            const auto [entry, isFirst] = firstReads.try_emplace(key, read);
            if (!isFirst) {
                Operation* const firstRead = entry->second;
                rewriter.replaceOp(read, firstRead->getResults());
            }
        }
    }
};

struct CarryLabelAndTypeReads : public impl::CarryLabelAndTypeReadsBase<CarryLabelAndTypeReads> {
    void runOnOperation() override {
        mlir::IRRewriter rewriter(&getContext());
        shareReads(getOperation(), isLabelOrTypeRead, rewriter);
    }
};

// A cross product cut by one equality between a column of each factor, and the filter
// applying it: what a hash join is made of. _buildsTheLeftFactor says which way round the
// two go into the join, whose right factor is the built side.
struct EqualityCross {
    CrossProduct _product {nullptr};
    EqOp _equality {nullptr};
    FilterOp _filter {nullptr};
    JoinKeySide _left;
    JoinKeySide _right;
    bool _buildsTheLeftFactor {false};
};

// Reads into side the one product column a key is computed over, along with the ops
// computing one from the other. False when the key reads no column, more than one, or one
// a different cross product made - product names the one already matched, null for the
// first side, and is set to the one this side found.
bool matchKeySide(Value key, JoinKeySide& side, CrossProduct& product) {
    side._key = key;
    collectRowCone({key}, key.getParentBlock(), side._cone);

    if (side._cone._inputs.size() != 1) {
        return false;
    }

    side._column = side._cone._inputs.front();

    CrossProduct keyProduct = side._column.getDefiningOp<CrossProduct>();
    if (!keyProduct || (product && keyProduct != product)) {
        return false;
    }

    product = keyProduct;

    return true;
}

// Whether nothing but the equality reads the ops rebuilding a key. The cone is sunk into
// the factor and dropped from here, so a reader left behind would lose its operand - the
// exception being the filter carrying the key itself, which the join yields among its own
// columns and hands to that reader instead.
bool keyConeIsPrivateTo(const JoinKeySide& side, EqOp equality, FilterOp filter) {
    llvm::SmallPtrSet<Operation*, 8> coneOps;
    for (Operation* const coneOp : side._cone._ops) {
        coneOps.insert(coneOp);
    }

    Operation* const equalityOp = equality.getOperation();
    Operation* const filterOp = filter.getOperation();
    Operation* const keyOp = side._key.getDefiningOp();

    for (Operation* const coneOp : side._cone._ops) {
        const bool holdsTheKey = coneOp == keyOp;

        for (Operation* const user : coneOp->getUsers()) {
            const bool isTheEquality = user == equalityOp;
            const bool isAnotherConeOp = coneOps.contains(user);
            const bool carriesTheKey = holdsTheKey && user == filterOp;

            if (!isTheEquality && !isAnotherConeOp && !carriesTheKey) {
                return false;
            }
        }
    }

    return true;
}

// Whether every column the filter carries is one the join hands back as a result of its
// own: a column the product made, or a key, which the join yields beside the columns its
// factor already had. One computed from them outside was computed over the rows the join
// no longer produces.
bool carriesJoinColumnsOnly(FilterOp filter, const EqualityCross& match) {
    for (const Value carried : filter.getColumnsToFilter()) {
        const bool isAProductColumn = carried.getDefiningOp<CrossProduct>() == match._product;
        const bool isAKey = carried == match._left._key || carried == match._right._key;

        if (!isAProductColumn && !isAKey) {
            return false;
        }
    }

    return true;
}

// Whether a factor's rows are the product of two relations'. The built side is buffered
// whole while the probed side streams a chunk at a time, so a factor holding a product -
// which is where the cascade of a comma pattern puts one - is the side to probe: its rows
// multiply where a scan or a traversal contributes them linearly.
bool factorHoldsAProduct(Region& factor) {
    const WalkResult walked = factor.walk([](Operation* op) {
        if (isa<CrossProduct>(op)) {
            return WalkResult::interrupt();
        }

        return WalkResult::advance();
    });

    return walked.wasInterrupted();
}

// The label set the rows of one side are scanned under, when the db level knows one: the
// labels of a by-label node scan, and none at all otherwise - every node of the graph,
// which is what the v2 planner estimates a variable with no label constraint at.
void keySideLabels(const JoinKeySide& side,
                   size_t factorFirstResult,
                   const ::db::GraphMetadata& metadata,
                   ::db::LabelSet& labels) {
    Yield yield = cast<Yield>(side._factor->front().getTerminator());
    const size_t columnIndex = cast<OpResult>(side._column).getResultNumber() - factorFirstResult;

    ScanNodesByLabel scan = yield.getColumns()[columnIndex].getDefiningOp<ScanNodesByLabel>();
    if (!scan) {
        return;
    }

    collectScanLabels(scan.getLabels(), metadata, labels);
}

// An op reading the rows of a cut, and how many of those rows the skips between them drop
struct CutRowsReader {
    Operation* _op {nullptr};
    uint64_t _skipped {0};
};

// The rows the cut must produce for a limit downstream of it to be met, and none when no
// limit bounds them. The budget reaches the cut only through ops handing each row on as at
// least one row as they read it, which the filter keeping other conjuncts than the equality
// does not. A skip adds the rows it drops to the limit after it, and a LIMIT 0 needs no row:
// the loops stop before their first step. A count bound when the query runs bounds nothing
// the plan can know.
std::optional<uint64_t> limitOverTheCut(const EqualityCross& match) {
    FilterOp filter = match._filter;
    EqOp equality = match._equality;
    if (filter.getMask() != equality.getResult()) {
        return std::nullopt;
    }

    Block* const cutBlock = filter->getBlock();

    llvm::SmallVector<CutRowsReader, 8> pending;
    llvm::SmallPtrSet<Operation*, 8> visited;
    const auto pushReaders = [&](Operation* op, uint64_t skipped) {
        for (Operation* const user : op->getUsers()) {
            Operation* const reader = cutBlock->findAncestorOpInBlock(*user);
            if (visited.insert(reader).second) {
                pending.push_back(CutRowsReader {reader, skipped});
            }
        }
    };

    pushReaders(filter, 0);

    std::optional<uint64_t> budget;
    while (!pending.empty()) {
        const CutRowsReader reader = pending.pop_back_val();
        Operation* const op = reader._op;

        if (Limit limit = dyn_cast<Limit>(op)) {
            const std::optional<uint64_t> count = limit.getCount();
            if (!count) {
                return std::nullopt;
            }

            const uint64_t rows = *count == 0 ? 0 : llvm::SaturatingAdd(reader._skipped, *count);
            budget = std::max(budget.value_or(0), rows);
        } else if (Skip skip = dyn_cast<Skip>(op)) {
            const std::optional<uint64_t> count = skip.getCount();
            if (!count) {
                return std::nullopt;
            }

            pushReaders(op, llvm::SaturatingAdd(reader._skipped, *count));
        } else if (computesAValuePerRow(op) || isa<OptionalMatch>(op)) {
            pushReaders(op, reader._skipped);
        } else {
            return std::nullopt;
        }
    }

    return budget;
}

// Whether the cut is better left as the product it stands as. The join reads its build
// side whole before emitting a row and indexes every key it holds, which a small product
// - or a small limit over a large one - never repays, so the shape alone does not settle
// which form to run: this is the v2 planner's verdict (ReadStmtGenerator::
// shouldPlaceValueHashJoin), answered by the same CardinalityEstimation over the node
// counts of each side's labels.
bool prefersTheProduct(const EqualityCross& match, const DBPassContext& context) {
    if (context._forcesHashJoin) {
        return false;
    } else if (!context._usesHashJoin) {
        return true;
    } else if (!context._view) {
        return false;
    }

    const ::db::GraphView& view = *context._view;
    const ::db::GraphMetadata& metadata = view.metadata();

    CrossProduct product = match._product;
    const size_t leftFactorColumns = factorYieldColumns(product.getLeftFactor()).size();

    ::db::LabelSet leftLabels;
    ::db::LabelSet rightLabels;
    keySideLabels(match._left, 0, metadata, leftLabels);
    keySideLabels(match._right, leftFactorColumns, metadata, rightLabels);

    const ::db::CardinalityEstimation estimation(view);

    return estimation.shouldPreferCartesian(leftLabels, rightLabels, limitOverTheCut(match));
}

// What the db level puts on rows it cannot count - the figure the v2 planner reads a
// yielded item at.
constexpr size_t uncountableRows = 10;

// System R's share of rows a predicate keeps (Selinger et al., 1979): the graph counts
// nodes per label set and edges per end, but no value per property.
constexpr double RANGE_SELECTIVITY = 1.0 / 3.0;
constexpr double MEMBERSHIP_SELECTIVITY_CEILING = 0.5;

// PostgreSQL's count of distinct values in a column it holds no statistics of
// (DEFAULT_NUM_DISTINCT): as many as the rows, up to this many, and two in a boolean one
constexpr double DEFAULT_DISTINCT_VALUES = 200;
constexpr double BOOLEAN_DISTINCT_VALUES = 2;

std::optional<int64_t> literalInteger(Value value) {
    ConstantOp constant = value.getDefiningOp<ConstantOp>();
    const IntegerAttr integer = constant ? dyn_cast<IntegerAttr>(constant.getValue()) : IntegerAttr();
    if (!integer) {
        return std::nullopt;
    }

    return integer.getInt();
}

double estimateUnwoundRows(Unwind unwind) {
    Range range = unwind.getSource().getDefiningOp<Range>();
    if (!range) {
        return static_cast<double>(uncountableRows);
    }

    const Value stepColumn = range.getStep();
    const std::optional<int64_t> start = literalInteger(range.getStart());
    const std::optional<int64_t> end = literalInteger(range.getEnd());
    const std::optional<int64_t> step = stepColumn ? literalInteger(stepColumn) : std::optional<int64_t>(1);

    const bool countsLiterals = start && end && step && *step != 0;
    if (!countsLiterals) {
        return static_cast<double>(uncountableRows);
    }

    const double steps = (static_cast<double>(*end) - static_cast<double>(*start)) / static_cast<double>(*step);

    return steps < 0 ? 0 : floor(steps) + 1;
}

using KnownLabels = llvm::DenseMap<Value, ::db::LabelSet>;

// The edges a hop walks as far as the estimate tells them apart: the labels known on the
// node it leaves and on the node it reaches, and the types it keeps, every type until one
// is named
struct HopEdges {
    ::db::LabelSet _near;
    ::db::LabelSet _far;
    llvm::SmallVector<::db::EdgeTypeID, 2> _types;
    bool _isTyped {false};
    bool _leavesSource {true};
    bool _walksBoth {false};
};

// The hop a walk last took at a node, and whether the node is the source of its edges
struct NodeSide {
    Operation* _hop {nullptr};
    bool _isOut {true};
};

// What an estimate walking a pattern's ops knows of its rows: the labels each node carries,
// the edges each hop walked, found from the hop and from the node it reached, and the hop
// the walk last took at each node
struct WalkKnowledge {
    KnownLabels _labels;
    llvm::DenseMap<Operation*, HopEdges> _hops;
    llvm::DenseMap<Value, Operation*> _reachingHops;
    llvm::DenseMap<Value, NodeSide> _lastSides;

    // A result an op hands on holds values of the column its rows come from, so it is
    // known as that column is: a filter keeps the labels of the nodes it reads
    void handOn(Operation* op) {
        ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(op);
        if (!carrier) {
            return;
        }

        for (const OpResult result : op->getResults()) {
            if (const Value rowSource = carrier.getRowSource(result)) {
                handOn(rowSource, result);
            }
        }
    }

    void handOn(Value rowSource, Value column) {
        const auto labels = _labels.find(rowSource);
        if (labels != _labels.end()) {
            const ::db::LabelSet sourceLabels = labels->second;
            _labels[column] = sourceLabels;
        }

        if (Operation* const reachingHop = _reachingHops.lookup(rowSource)) {
            _reachingHops[column] = reachingHop;
        }

        const auto lastSide = _lastSides.find(rowSource);
        if (lastSide != _lastSides.end()) {
            const NodeSide sourceSide = lastSide->second;
            _lastSides[column] = sourceSide;
        }
    }
};

// What a hop makes and reads per row it reads
struct HopEstimate {
    double _rows {0};
    double _edgesRead {0};
};

// The edge types a list of names stands for, a name the schema lacks standing for no edge
void collectEdgeTypes(ArrayAttr names, const ::db::GraphMetadata& metadata, llvm::SmallVectorImpl<::db::EdgeTypeID>& edgeTypes) {
    const ::db::EdgeTypeMap& edgeTypeMap = metadata.edgeTypes();
    for (const Attribute nameAttr : names) {
        const StringAttr name = dyn_cast<StringAttr>(nameAttr);
        if (!name) {
            continue;
        }

        const llvm::StringRef nameRef = name.getValue();
        const std::optional<::db::EdgeTypeID> edgeType = edgeTypeMap.get(std::string_view(nameRef.data(), nameRef.size()));
        if (edgeType && !llvm::is_contained(edgeTypes, *edgeType)) {
            edgeTypes.push_back(*edgeType);
        }
    }
}

// The hop or edge scan a column of edge types was read from: its own type column, or the
// types of its edge column
Operation* findTypedHop(Value edgeTypes, llvm::function_ref<Value(Value)> nodeOf) {
    Value column = nodeOf(edgeTypes);
    size_t resultIndex = EDGE_TYPES_RESULT;
    if (GetEdgeTypes read = column.getDefiningOp<GetEdgeTypes>()) {
        column = nodeOf(read.getInputEdges());
        resultIndex = EDGE_IDS_RESULT;
    }

    const OpResult result = dyn_cast<OpResult>(column);
    if (!result || result.getResultNumber() != resultIndex) {
        return nullptr;
    }

    Operation* const owner = result.getOwner();
    if (!isEdgeHop(owner) && !isEdgeScan(owner)) {
        return nullptr;
    }

    return owner;
}

class RowEstimator {
public:
    explicit RowEstimator(const ::db::GraphView& view)
        : _metadata(view.metadata()),
        _estimation(view),
        _nodeCount(static_cast<double>(_estimation.estimateNodeCount(::db::LabelSet {}))),
        _edgeCount(static_cast<double>(_estimation.estimateEdgeCount()))
    {
    }

    double getNodeCount() const { return _nodeCount; }
    double getEdgeCount() const { return _edgeCount; }

    double getLabelledNodes(const ::db::LabelSet& labels) const {
        if (labels.empty()) {
            return _nodeCount;
        }

        return static_cast<double>(_estimation.estimateNodeCount(labels));
    }

    // The nodes a scan reads: those carrying every label @param labelNames names, every node
    // when it names none, whose labels go into @param knowledge for the column @param nodes
    double scanNodes(Value nodes, ArrayAttr labelNames, WalkKnowledge& knowledge) const {
        if (!labelNames) {
            return _nodeCount;
        }

        ::db::LabelSet labels;
        if (!collectScanLabels(labelNames, _metadata, labels)) {
            return 0;
        }

        knowledge._labels[nodes] = labels;

        return getLabelledNodes(labels);
    }

    // The nodes a scan by property value makes: of those scanNodes reads, the ones holding
    // one of the property's distinct values
    double scanNodesByPropertyValue(ScanNodesByPropertyValue scan, WalkKnowledge& knowledge) const {
        const std::optional<ArrayAttr> labels = scan.getLabels();
        const double scanned = scanNodes(scan.getResult(), labels ? *labels : ArrayAttr(), knowledge);

        const TypedAttr value = scan.getValueAttr();
        const Type element = value ? value.getType() : ::db::declaredColumnElement(scan.getBoundValue());

        return scanned * getEqualityShare(getPropertyDistinctValues(element, scanned));
    }

    // The rows an edge scan makes: the edges of the types @param typeNames names, every type
    // when it names none, from the nodes carrying every label @param sourceNames names to
    // those carrying every label @param targetNames names, an end with no names being any
    // node. What it walked goes into @param knowledge, as a hop out of its sources would.
    double scanEdges(Operation* scan,
                     ArrayAttr sourceNames,
                     ArrayAttr targetNames,
                     ArrayAttr typeNames,
                     WalkKnowledge& knowledge) const {
        HopEdges edges;
        const bool resolvesSource = !sourceNames || collectScanLabels(sourceNames, _metadata, edges._near);
        const bool resolvesTarget = !targetNames || collectScanLabels(targetNames, _metadata, edges._far);
        if (typeNames) {
            narrowTypes(typeNames, edges);
        }

        const Value sources = scan->getResult(0);
        const Value targets = scan->getResult(EDGE_TARGETS_RESULT);

        knowledge._labels[sources] = edges._near;
        knowledge._labels[targets] = edges._far;
        knowledge._hops[scan] = edges;
        knowledge._reachingHops[targets] = scan;
        knowledge._lastSides[sources] = NodeSide {._hop = scan, ._isOut = true};
        knowledge._lastSides[targets] = NodeSide {._hop = scan, ._isOut = false};

        if (!resolvesSource || !resolvesTarget) {
            return 0;
        }

        return countHopEdges(edges);
    }

    // The rows a hop makes and the edges it reads per row it reads: the edges it walks per
    // node carrying the labels known on @param from, of its own types or else of @param
    // keptTypes, which a filter of its type column keeps, and every edge on that side of the
    // node, since a node's edges of one type are scattered through all of them. A node the
    // walk took a hop at before has the edges of the nodes that hop leads to, not the
    // average. What it walked goes into @param knowledge.
    HopEstimate walkHop(Operation* hop,
                        bool walksBackward,
                        Value from,
                        Value to,
                        ArrayAttr keptTypes,
                        WalkKnowledge& knowledge) const {
        HopEdges edges;
        edges._near = knowledge._labels.lookup(from);
        edges._far = knowledge._labels.lookup(to);
        edges._leavesSource = isReverseHop(hop) == walksBackward;
        edges._walksBoth = isa<GetEdges>(hop);

        bool reachesLabelledNodes = true;
        if (isEdgeHop(hop)) {
            const ArrayAttr hopTypes = hop->getAttrOfType<ArrayAttr>("edge_types");
            const ArrayAttr types = hopTypes ? hopTypes : keptTypes;
            if (types) {
                narrowTypes(types, edges);
            }

            const ArrayAttr reachedLabels = hop->getAttrOfType<ArrayAttr>("labels");
            if (reachedLabels && !walksBackward) {
                reachesLabelledNodes = collectScanLabels(reachedLabels, _metadata, edges._far);
            }
        }

        HopEdges read;
        read._near = edges._near;
        read._leavesSource = edges._leavesSource;
        read._walksBoth = edges._walksBoth;

        const bool relatesToEarlierHops = isEdgeHop(hop) && !edges._walksBoth;

        double rowsLift = 1;
        double readLift = 1;
        const auto previous = knowledge._lastSides.find(from);
        if (relatesToEarlierHops && previous != knowledge._lastSides.end()) {
            const NodeSide previousSide = previous->second;
            const HopEdges& previousEdges = knowledge._hops.find(previousSide._hop)->second;
            const ::db::DegreeSelection previousDegree = selectionOf(previousEdges, previousSide._isOut);

            rowsLift = getDegreeLift(edges._near, previousDegree, selectionOf(edges, edges._leavesSource));
            readLift = getDegreeLift(edges._near, previousDegree, selectionOf(read, read._leavesSource));
        }

        knowledge._labels[to] = edges._far;
        knowledge._hops[hop] = edges;
        knowledge._reachingHops[to] = hop;

        if (relatesToEarlierHops) {
            knowledge._lastSides[from] = NodeSide {._hop = hop, ._isOut = edges._leavesSource};
            knowledge._lastSides[to] = NodeSide {._hop = hop, ._isOut = !edges._leavesSource};
        } else {
            knowledge._lastSides.erase(from);
            knowledge._lastSides.erase(to);
        }

        const double nodes = getLabelledNodes(edges._near);
        if (nodes == 0) {
            return HopEstimate {};
        }

        HopEstimate estimate;
        estimate._edgesRead = readLift * countHopEdges(read) / nodes;
        if (reachesLabelledNodes) {
            estimate._rows = rowsLift * countHopEdges(edges) / nodes;
        }

        return estimate;
    }

    // The share of rows a label check on @param node keeps, which then adds the labels every
    // alternative asks for to what @param knowledge knows of the node
    double keepLabelled(ArrayAttr alternatives, Value node, WalkKnowledge& knowledge) const {
        const double kept = shareLabelled(alternatives, node, knowledge);

        ::db::LabelSet shared;
        addSharedLabels(alternatives, shared);
        knowledge._labels[node].merge(shared);

        const auto reaching = knowledge._reachingHops.find(node);
        if (reaching != knowledge._reachingHops.end()) {
            knowledge._hops[reaching->second]._far.merge(shared);
        }

        return kept;
    }

    // @param nodeOf names the node a column holds, whose labels @param knowledge keeps
    double getSelectivity(Value mask, llvm::function_ref<Value(Value)> nodeOf, WalkKnowledge& knowledge) const {
        llvm::SmallVector<Value, 4> conjuncts;
        collectConjuncts(mask, conjuncts);

        double kept = 1;
        for (const Value conjunct : conjuncts) {
            const LabelCheck check = readLabelCheck(conjunct);
            CheckEdgeTypeConstraint typeCheck = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();

            if (check._alternatives) {
                kept *= keepLabelled(check._alternatives, nodeOf(check._nodes), knowledge);
            } else if (typeCheck) {
                kept *= keepTyped(typeCheck, nodeOf, knowledge);
            } else {
                kept *= getPredicateSelectivity(conjunct, nodeOf, knowledge);
            }
        }

        return kept;
    }

    // A flat OR or AND chain is as long as the query, so the walk keeps its own stack
    double getPredicateSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        llvm::DenseMap<Value, double> kept;
        llvm::SmallVector<Value> pending {predicate};

        while (!pending.empty()) {
            const Value current = pending.back();
            if (kept.count(current)) {
                pending.pop_back();
                continue;
            }

            Operation* const def = current.getDefiningOp();
            if (!isa_and_nonnull<AndOp, OrOp, XorOp, NotOp>(def)) {
                kept[current] = getLeafSelectivity(current, nodeOf, knowledge);
                pending.pop_back();
                continue;
            }

            bool operandsPending = false;
            for (const Value operand : def->getOperands()) {
                if (!kept.count(operand)) {
                    pending.push_back(operand);
                    operandsPending = true;
                }
            }

            if (operandsPending) {
                continue;
            }

            const double first = kept.lookup(def->getOperand(0));
            const double second = def->getNumOperands() > 1 ? kept.lookup(def->getOperand(1)) : 0;

            if (isa<AndOp>(def)) {
                kept[current] = first * second;
            } else if (isa<OrOp>(def)) {
                kept[current] = first + second - first * second;
            } else if (isa<XorOp>(def)) {
                kept[current] = first + second - 2 * first * second;
            } else {
                kept[current] = 1 - first;
            }

            pending.pop_back();
        }

        return kept.lookup(predicate);
    }

private:
    // The edge counts and degree products asked for, read off the graph once each: the walks
    // the estimate compares ask for the same ones over and over
    using StatisticKey = llvm::SmallVector<uint64_t, 16>;

    static constexpr uint64_t EDGE_COUNT_KEY = 0;
    static constexpr uint64_t DEGREE_PRODUCTS_KEY = 1;

    const ::db::GraphMetadata& _metadata;
    ::db::CardinalityEstimation _estimation;
    double _nodeCount {0};
    double _edgeCount {0};
    mutable std::map<StatisticKey, double> _statistics;

    struct LabelCheck {
        ArrayAttr _alternatives;
        Value _nodes;
    };

    static LabelCheck readLabelCheck(Value predicate) {
        CheckLabelConstraint check = predicate.getDefiningOp<CheckLabelConstraint>();
        if (!check) {
            return {};
        }

        GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
        if (!labelSet) {
            return {};
        }

        return LabelCheck {._alternatives = check.getAlternatives(), ._nodes = labelSet.getInputNodes()};
    }

    static void appendLabels(const ::db::LabelSet& labels, StatisticKey& key) {
        for (const uint64_t word : labels.integers()) {
            key.push_back(word);
        }
    }

    static void appendDegree(const ::db::DegreeSelection& degree, StatisticKey& key) {
        key.push_back(degree._isOut ? 1 : 0);
        key.push_back(degree._isEveryType ? 1 : 0);
        key.push_back(degree._edgeTypes.size());
        for (const ::db::EdgeTypeID edgeType : degree._edgeTypes) {
            key.push_back(edgeType.getValue());
        }
    }

    double countEdges(const ::db::LabelSet& source, const HopEdges& edges, const ::db::LabelSet& target) const {
        StatisticKey key {EDGE_COUNT_KEY};
        appendLabels(source, key);
        appendLabels(target, key);
        appendDegree(selectionOf(edges, true), key);

        const auto [known, isNew] = _statistics.try_emplace(key, 0);
        if (!isNew) {
            return known->second;
        }

        double count = 0;
        if (!edges._isTyped) {
            count = static_cast<double>(_estimation.estimateEdgeCount(source, target));
        } else {
            for (const ::db::EdgeTypeID edgeType : edges._types) {
                count += static_cast<double>(_estimation.estimateEdgeCount(source, edgeType, target));
            }
        }

        known->second = count;

        return count;
    }

    double sumDegreeProducts(const ::db::LabelSet& labels,
                             const ::db::DegreeSelection& first,
                             const ::db::DegreeSelection& second) const {
        StatisticKey key {DEGREE_PRODUCTS_KEY};
        appendLabels(labels, key);
        appendDegree(first, key);
        appendDegree(second, key);

        const auto [known, isNew] = _statistics.try_emplace(key, 0);
        if (isNew) {
            known->second = static_cast<double>(_estimation.estimateDegreeProducts(labels, first, second));
        }

        return known->second;
    }

    double countHopEdges(const HopEdges& edges) const {
        const ::db::LabelSet& source = edges._leavesSource ? edges._near : edges._far;
        const ::db::LabelSet& target = edges._leavesSource ? edges._far : edges._near;

        double count = countEdges(source, edges, target);
        if (edges._walksBoth) {
            count += countEdges(target, edges, source);
        }

        return count;
    }

    static ::db::DegreeSelection selectionOf(const HopEdges& edges, bool isOut) {
        return ::db::DegreeSelection {._edgeTypes = edges._types, ._isEveryType = !edges._isTyped, ._isOut = isOut};
    }

    // The edges @param degree selects on the nodes carrying @param labels
    double countSelectedEdges(const ::db::LabelSet& labels, const ::db::DegreeSelection& degree) const {
        HopEdges edges;
        edges._near = labels;
        edges._types.assign(degree._edgeTypes.begin(), degree._edgeTypes.end());
        edges._isTyped = !degree._isEveryType;
        edges._leavesSource = degree._isOut;

        return countHopEdges(edges);
    }

    // How many times the label set's average of the edges @param next selects a node carrying
    // @param labels has when the walk took the edges @param previous selects there before:
    // a walk comes to a node with many edges more often than to one with few
    double getDegreeLift(const ::db::LabelSet& labels,
                         const ::db::DegreeSelection& previous,
                         const ::db::DegreeSelection& next) const {
        const double nodes = getLabelledNodes(labels);
        const double previousEdges = countSelectedEdges(labels, previous);
        const double nextEdges = countSelectedEdges(labels, next);
        if (nodes == 0 || previousEdges == 0 || nextEdges == 0) {
            return 1;
        }

        const double products = sumDegreeProducts(labels, previous, next);

        return (products / previousEdges) / (nextEdges / nodes);
    }

    // Keeps the types of @param edges the names ask for, every one of them when the edges
    // were of any type
    void narrowTypes(ArrayAttr typeNames, HopEdges& edges) const {
        llvm::SmallVector<::db::EdgeTypeID, 2> named;
        collectEdgeTypes(typeNames, _metadata, named);

        if (edges._isTyped) {
            llvm::erase_if(edges._types, [&named](::db::EdgeTypeID edgeType) {
                return !llvm::is_contained(named, edgeType);
            });
        } else {
            edges._types.assign(named.begin(), named.end());
            edges._isTyped = true;
        }
    }

    void addSharedLabels(ArrayAttr alternatives, ::db::LabelSet& labels) const {
        llvm::DenseSet<Attribute> sharedNames;
        addSharedLabelNames(alternatives, sharedNames);

        const ::db::LabelMap& labelMap = _metadata.labels();
        for (const Attribute name : sharedNames) {
            const StringAttr nameAttr = dyn_cast<StringAttr>(name);
            if (!nameAttr) {
                continue;
            }

            const llvm::StringRef nameRef = nameAttr.getValue();
            const std::optional<::db::LabelID> label = labelMap.get(std::string_view(nameRef.data(), nameRef.size()));
            if (label) {
                labels.set(*label);
            }
        }
    }

    // The share of nodes carrying @param known that carry the labels of one alternative
    double shareLabelled(ArrayAttr alternatives, const ::db::LabelSet& known) const {
        const double knownNodes = getLabelledNodes(known);
        if (knownNodes == 0) {
            return 0;
        }

        double kept = 0;
        for (const Attribute alternative : alternatives) {
            ::db::LabelSet labels;
            if (collectScanLabels(cast<ArrayAttr>(alternative), _metadata, labels)) {
                labels.merge(known);
                kept += getLabelledNodes(labels);
            }
        }

        return std::min(1.0, kept / knownNodes);
    }

    // A node a hop reached is checked among the edges that hop walked, so a type that only
    // ever reaches nodes of a label keeps all of them; any other among the nodes it is
    // known to carry the labels of
    double shareLabelled(ArrayAttr alternatives, Value node, const WalkKnowledge& knowledge) const {
        const auto reaching = knowledge._reachingHops.find(node);
        if (reaching == knowledge._reachingHops.end()) {
            return shareLabelled(alternatives, knowledge._labels.lookup(node));
        }

        const HopEdges& edges = knowledge._hops.find(reaching->second)->second;
        const double walked = countHopEdges(edges);
        if (walked == 0) {
            return 0;
        }

        double kept = 0;
        for (const Attribute alternative : alternatives) {
            HopEdges labelled = edges;
            if (collectScanLabels(cast<ArrayAttr>(alternative), _metadata, labelled._far)) {
                kept += countHopEdges(labelled);
            }
        }

        return std::min(1.0, kept / walked);
    }

    // The share of rows a type check keeps: of the edges its hop walked, or of the graph's
    // when no hop the estimate walked made them
    double shareTyped(CheckEdgeTypeConstraint check,
                      llvm::function_ref<Value(Value)> nodeOf,
                      const WalkKnowledge& knowledge) const {
        Operation* const hop = findTypedHop(check.getEdgeTypeIds(), nodeOf);
        const auto walkedEdges = hop ? knowledge._hops.find(hop) : knowledge._hops.end();

        HopEdges edges = walkedEdges != knowledge._hops.end() ? walkedEdges->second : HopEdges {};
        const double walked = countHopEdges(edges);
        if (walked == 0) {
            return 0;
        }

        narrowTypes(check.getEdgeTypes(), edges);

        return countHopEdges(edges) / walked;
    }

    double keepTyped(CheckEdgeTypeConstraint check,
                     llvm::function_ref<Value(Value)> nodeOf,
                     WalkKnowledge& knowledge) const {
        const double kept = shareTyped(check, nodeOf, knowledge);

        Operation* const hop = findTypedHop(check.getEdgeTypeIds(), nodeOf);
        const auto walkedEdges = hop ? knowledge._hops.find(hop) : knowledge._hops.end();
        if (walkedEdges != knowledge._hops.end()) {
            narrowTypes(check.getEdgeTypes(), walkedEdges->second);
        }

        return kept;
    }

    // With nothing to count the values by, one in DEFAULT_DISTINCT_VALUES: PostgreSQL's
    // DEFAULT_EQ_SEL
    static double getEqualityShare(double distinctValues) {
        return distinctValues >= 1 ? 1 / distinctValues : 1 / DEFAULT_DISTINCT_VALUES;
    }

    static double getPropertyDistinctValues(Type element, double nodes) {
        const bool isBoolean = element && (isa<storage::BoolType>(element) || element.isInteger(1));

        return isBoolean ? BOOLEAN_DISTINCT_VALUES : std::min(DEFAULT_DISTINCT_VALUES, nodes);
    }

    // The distinct values a side of an equality holds: one per node in a node column, and
    // none in a value read off no node
    double getDistinctValues(Value side, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        const Value entityColumn = comparedEntityColumn(side);
        GetNodeProperties propertyRead = nodeOf(side).getDefiningOp<GetNodeProperties>();

        if (entityColumn && isNodeColumn(entityColumn)) {
            return getLabelledNodes(knowledge._labels.lookup(nodeOf(entityColumn)));
        } else if (propertyRead) {
            const Value propertyColumn = propertyRead.getResult();
            const double nodes = getLabelledNodes(knowledge._labels.lookup(nodeOf(propertyRead.getInputNodes())));

            return getPropertyDistinctValues(::db::declaredColumnElement(propertyColumn), nodes);
        }

        return 0;
    }

    // An equality keeps one row in the distinct values of the side holding more, as
    // PostgreSQL estimates one over columns it holds no statistics of
    double getEqualitySelectivity(Value lhs, Value rhs, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        const double lhsValues = getDistinctValues(lhs, nodeOf, knowledge);
        const double rhsValues = getDistinctValues(rhs, nodeOf, knowledge);

        return getEqualityShare(std::max(lhsValues, rhsValues));
    }

    double getMembershipSelectivity(InOp membership, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        ConstantOp constant = membership.getRhs().getDefiningOp<ConstantOp>();
        const ArrayAttr elements = constant ? dyn_cast<ArrayAttr>(constant.getValue()) : ArrayAttr();
        if (!elements) {
            return MEMBERSHIP_SELECTIVITY_CEILING;
        }

        const double elementShare = getEqualityShare(getDistinctValues(membership.getLhs(), nodeOf, knowledge));

        return std::min(MEMBERSHIP_SELECTIVITY_CEILING, elementShare * static_cast<double>(elements.size()));
    }

    double getLeafSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        Operation* const def = predicate.getDefiningOp();

        if (EqOp equality = dyn_cast_or_null<EqOp>(def)) {
            return getEqualitySelectivity(equality.getLhs(), equality.getRhs(), nodeOf, knowledge);
        } else if (NeqOp inequality = dyn_cast_or_null<NeqOp>(def)) {
            return 1 - getEqualitySelectivity(inequality.getLhs(), inequality.getRhs(), nodeOf, knowledge);
        } else if (isa_and_nonnull<GtOp, LtOp, GteOp, LteOp>(def)) {
            return RANGE_SELECTIVITY;
        } else if (InOp membership = dyn_cast_or_null<InOp>(def)) {
            return getMembershipSelectivity(membership, nodeOf, knowledge);
        } else if (CheckEdgeTypeConstraint typeCheck = dyn_cast_or_null<CheckEdgeTypeConstraint>(def)) {
            return shareTyped(typeCheck, nodeOf, knowledge);
        } else if (const LabelCheck check = readLabelCheck(predicate); check._alternatives) {
            return shareLabelled(check._alternatives, nodeOf(check._nodes), knowledge);
        }

        return 1;
    }
};

// A list bound when the query runs has a length no statistic of the graph measures, so it
// reads at the figure an uncountable source does
double estimateListedEntities(size_t literalCount, mlir::OperandRange boundIDs) {
    double listed = static_cast<double>(literalCount);
    for (const Value bound : boundIDs) {
        const bool listsIDs = isa<storage::ListType>(::db::declaredColumnElement(bound));
        listed += listsIDs ? static_cast<double>(uncountableRows) : 1.0;
    }

    return listed;
}

// The rows an op seeds a factor with, and nothing at all when it seeds none - a fetch, a
// filter or a hop reads a column rather than making one. A listed set of IDs is its own
// count, a literal list one row per element, a by-label scan what the graph holds under
// those labels, and a property scan the share of those an equality keeps.
std::optional<double> estimateSourceRows(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge) {
    if (ConstScanNodes constScan = dyn_cast<ConstScanNodes>(op)) {
        return estimateListedEntities(constScan.getNodeIDs().size(), constScan.getBoundIDs());
    } else if (ConstScanEdges constScan = dyn_cast<ConstScanEdges>(op)) {
        const double listed = estimateListedEntities(constScan.getEdgeIDs().size(), constScan.getBoundIDs());
        return constScan.getUndirected() ? 2 * listed : listed;
    } else if (UnwindConst unwind = dyn_cast<UnwindConst>(op)) {
        return static_cast<double>(unwind.getElements().size());
    } else if (ScanNodesByLabel byLabel = dyn_cast<ScanNodesByLabel>(op)) {
        return estimator.scanNodes(byLabel.getResult(), byLabel.getLabels(), knowledge);
    } else if (ScanNodes scan = dyn_cast<ScanNodes>(op)) {
        return estimator.scanNodes(scan.getResult(), ArrayAttr(), knowledge);
    } else if (ScanNodesByPropertyValue byValue = dyn_cast<ScanNodesByPropertyValue>(op)) {
        return estimator.scanNodesByPropertyValue(byValue, knowledge);
    } else if (EdgeScanLabels scanLabels; readEdgeScanLabels(op, scanLabels)) {
        return estimator.scanEdges(op, scanLabels._source, scanLabels._target, ArrayAttr(), knowledge);
    } else if (ScanEdgesByType byType = dyn_cast<ScanEdgesByType>(op)) {
        return estimator.scanEdges(op, ArrayAttr(), ArrayAttr(), byType.getEdgeTypes(), knowledge);
    }

    return std::nullopt;
}

// The ratio an op multiplies the rows it reads by, and nothing when it hands them on as
// they come. A hop makes a row per edge it walks of the nodes it reads, of the types and to
// the labels it keeps, and an undirected one walks both directions. It is a ratio rather
// than a count because a graph with fewer edges than nodes has a hop shrink its input
// rather than grow it. An unwind makes a row per element of each row's list, whose length
// is a property of the rows themselves: no statistic of the graph measures it, so it reads
// at the figure an uncountable source does - where a literal list or a range between
// literal bounds, whose elements are in the IR, is counted exactly instead.
std::optional<double> estimateRowMultiplier(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge) {
    if (Unwind unwind = dyn_cast<Unwind>(op)) {
        return estimateUnwoundRows(unwind);
    } else if (!isEdgeHop(op)) {
        return std::nullopt;
    }

    return estimator.walkHop(op, false, op->getOperand(0), op->getResult(hopReachedResult(op)), ArrayAttr(), knowledge)._rows;
}

struct RegionRows {
    double _rows {1};
    bool _countedASource {false};
};

double estimateRegionRows(const RegionRows& region) {
    return region._countedASource ? region._rows : static_cast<double>(uncountableRows);
}

// The rows a factor makes: the product of what its sources seed it with, a factor crossing
// two of them holding a row per pair, grown or shrunk by every hop and unwind reading from
// one and cut by every filter. A join makes as many as its smaller side, each key taken as
// distinct on its side: the db level counts no distinct values. An aggregate with no
// grouping key makes one row, whatever it reads. A factor seeded from somewhere the db
// level cannot count - a procedure, a load - counts as an uncountable source's rows.
double estimateFactorRows(Region& factor, const RowEstimator& estimator) {
    llvm::DenseMap<Region*, RegionRows> regionRows;
    WalkKnowledge knowledge;

    const auto columnItself = [](Value column) {
        return column;
    };

    const auto countRows = [&](Operation* op) {
        RegionRows nested;
        if (HashJoin join = dyn_cast<HashJoin>(op)) {
            const double leftRows = estimateRegionRows(regionRows.lookup(&join.getLeftFactor()));
            const double rightRows = estimateRegionRows(regionRows.lookup(&join.getRightFactor()));
            nested = RegionRows {std::min(leftRows, rightRows), true};
        } else {
            for (Region& region : op->getRegions()) {
                const RegionRows counted = regionRows.lookup(&region);
                nested._rows *= counted._rows;
                nested._countedASource = nested._countedASource || counted._countedASource;
            }
        }

        RegionRows& rows = regionRows[op->getParentRegion()];
        rows._rows *= nested._rows;
        rows._countedASource = rows._countedASource || nested._countedASource;

        if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            rows._rows *= estimator.getSelectivity(filter.getMask(), columnItself, knowledge);
            return;
        }

        AggregateOpInterface aggregate = dyn_cast<AggregateOpInterface>(op);
        if (aggregate && aggregate.getGroupingKeys().empty()) {
            rows = RegionRows {1, true};
            return;
        }

        const std::optional<double> seeded = estimateSourceRows(op, estimator, knowledge);
        if (seeded) {
            rows._countedASource = true;
            rows._rows *= *seeded;
            return;
        }

        const std::optional<double> multiplier = estimateRowMultiplier(op, estimator, knowledge);
        if (multiplier) {
            rows._rows *= *multiplier;
        }
    };

    factor.walk([&](Operation* op) {
        countRows(op);
        knowledge.handOn(op);
    });

    return estimateRegionRows(regionRows.lookup(&factor));
}

// Which way round the two sides go into the join, whose right factor is the built one. The
// built side is buffered whole and indexed while the probed side streams a chunk at a
// time, so the side to build is the one making the fewer rows.
bool buildsTheLeftFactor(const EqualityCross& match, const DBPassContext& context) {
    CrossProduct product = match._product;
    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();

    // With no graph to count either side against, a factor holding a product of its own is
    // the one to probe - its rows multiply where a scan's are linear - and with neither or
    // both holding one the right factor is built, the way db.hash_join reads its regions.
    // The estimate needs no such proxy: it multiplies through the product itself.
    if (!context._view) {
        const bool leftHoldsAProduct = factorHoldsAProduct(leftFactor);
        const bool rightHoldsAProduct = factorHoldsAProduct(rightFactor);

        return rightHoldsAProduct && !leftHoldsAProduct;
    }

    const RowEstimator estimator(*context._view);

    return estimateFactorRows(leftFactor, estimator) < estimateFactorRows(rightFactor, estimator);
}

bool matchEqualityConjunct(FilterOp filter, Value conjunct, OpAnswers& readAlone, EqualityCross& match) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!equality || !equality.getResult().hasOneUse()) {
        return false;
    }

    match = EqualityCross {};
    match._filter = filter;
    match._equality = equality;

    // Either operand may name either factor, so the two sides are told apart by the
    // factor each key's column belongs to rather than by the order they are written in.
    CrossProduct product;
    JoinKeySide first;
    JoinKeySide second;
    if (!matchKeySide(equality.getLhs(), first, product)
        || !matchKeySide(equality.getRhs(), second, product)) {
        return false;
    }

    if (product->getBlock() != filter->getBlock()) {
        return false;
    }

    match._product = product;

    const size_t leftCount = factorYieldColumns(product.getLeftFactor()).size();
    const auto yieldedByTheLeftFactor = [leftCount](const JoinKeySide& side) {
        return cast<OpResult>(side._column).getResultNumber() < leftCount;
    };

    // One key on each side is what makes this a join; two keys of one factor are a
    // single-variable predicate, which the filter applies where it already stands.
    if (yieldedByTheLeftFactor(first) == yieldedByTheLeftFactor(second)) {
        return false;
    }

    const bool firstIsOnTheLeft = yieldedByTheLeftFactor(first);
    match._left = firstIsOnTheLeft ? first : second;
    match._right = firstIsOnTheLeft ? second : first;
    match._left._factor = &product.getLeftFactor();
    match._right._factor = &product.getRightFactor();

    if (!isHashableKey(match._left) || !isHashableKey(match._right)) {
        return false;
    }

    const bool conesArePrivate = keyConeIsPrivateTo(match._left, equality, filter)
                                 && keyConeIsPrivateTo(match._right, equality, filter);
    if (!conesArePrivate) {
        return false;
    }

    // The join emits the rows the equality holds on, so a reader of the product's own rows
    // would go from seeing every pair to seeing only the matching ones
    return carriesJoinColumnsOnly(filter, match) && answerOnce(readAlone, product, [&product, &filter]() {
        return readsOnlyIntoTheFilter(product, filter);
    });
}

// Sinks the ops rebuilding a side's key into its factor, so the key becomes a column the
// factor yields and the join can index it as that side is read. Answers where the key
// lands in the yield: the column's own place when the key is that column, a fresh last
// place when it is computed from it.
size_t sinkKeyIntoFactor(JoinKeySide& side, size_t factorFirstResult, mlir::OpBuilder& builder) {
    Yield yield = cast<Yield>(side._factor->front().getTerminator());
    const size_t columnIndex = cast<OpResult>(side._column).getResultNumber() - factorFirstResult;

    if (side._cone._ops.empty()) {
        return columnIndex;
    }

    mlir::IRMapping mapping;
    mapping.map(side._column, yield.getColumns()[columnIndex]);

    builder.setInsertionPoint(yield);
    for (Operation* const coneOp : side._cone._ops) {
        builder.clone(*coneOp, mapping);
    }

    const size_t keyColumn = yield.getNumOperands();
    yield->insertOperands(static_cast<unsigned>(keyColumn), mapping.lookup(side._key));

    return keyColumn;
}

void fuseHashJoin(EqualityCross& match, mlir::RewriterBase& rewriter) {
    CrossProduct product = match._product;

    Yield leftYield = cast<Yield>(product.getLeftFactor().front().getTerminator());
    Yield rightYield = cast<Yield>(product.getRightFactor().front().getTerminator());
    const size_t leftCount = leftYield.getNumOperands();
    const size_t rightCount = rightYield.getNumOperands();

    const size_t leftKey = sinkKeyIntoFactor(match._left, 0, rewriter);
    const size_t rightKey = sinkKeyIntoFactor(match._right, leftCount, rewriter);

    // The join probes its left factor and builds its right, so the two sides go in the
    // way round the match chose rather than the way round the product held them.
    const bool buildsTheLeftFactor = match._buildsTheLeftFactor;
    Yield probeYield = buildsTheLeftFactor ? rightYield : leftYield;
    Yield buildYield = buildsTheLeftFactor ? leftYield : rightYield;
    Region& probeFactor = buildsTheLeftFactor ? product.getRightFactor() : product.getLeftFactor();
    Region& buildFactor = buildsTheLeftFactor ? product.getLeftFactor() : product.getRightFactor();

    llvm::SmallVector<Type> resultTypes;
    llvm::append_range(resultTypes, probeYield.getColumns().getTypes());
    llvm::append_range(resultTypes, buildYield.getColumns().getTypes());

    const size_t probeKey = buildsTheLeftFactor ? rightKey : leftKey;
    const size_t buildKey = buildsTheLeftFactor ? leftKey : rightKey;

    rewriter.setInsertionPoint(product);
    HashJoin join = rewriter.create<HashJoin>(product.getLoc(), resultTypes, probeKey, buildKey);
    join.getLeftFactor().takeBody(probeFactor);
    join.getRightFactor().takeBody(buildFactor);

    // Sinking a key widens that factor's yield, so a column of the side the join reads
    // second sits further along in its results than it did in the product's.
    const size_t joinProbeCount = probeYield.getNumOperands();
    const size_t leftFirstResult = buildsTheLeftFactor ? joinProbeCount : 0;
    const size_t rightFirstResult = buildsTheLeftFactor ? 0 : joinProbeCount;

    llvm::SmallVector<Value> joinColumns;
    for (size_t columnIndex = 0; columnIndex < leftCount; columnIndex++) {
        joinColumns.push_back(join.getResult(leftFirstResult + columnIndex));
    }
    for (size_t columnIndex = 0; columnIndex < rightCount; columnIndex++) {
        joinColumns.push_back(join.getResult(rightFirstResult + columnIndex));
    }

    // A sunk key sits past the columns its factor already yielded, so it is none of those.
    const Value leftKeyColumn = join.getResult(leftFirstResult + leftKey);
    const Value rightKeyColumn = join.getResult(rightFirstResult + rightKey);

    // The join keeps only the rows the equality held on, so what the filter carries now
    // comes from the join directly - a carried key from the column the join yields for it,
    // which is that key over the joined rows.
    FilterOp filter = match._filter;
    const Operation::operand_range carried = filter.getColumnsToFilter();
    rewriter.modifyOpInPlace(filter, [&]() {
        for (size_t columnIndex = 0; columnIndex < carried.size(); columnIndex++) {
            const Value carriedColumn = carried[columnIndex];
            const unsigned operandIndex = static_cast<unsigned>(columnIndex) + 1;

            if (carriedColumn == match._left._key) {
                filter->setOperand(operandIndex, leftKeyColumn);
            } else if (carriedColumn == match._right._key) {
                filter->setOperand(operandIndex, rightKeyColumn);
            }
        }
    });

    for (size_t columnIndex = 0; columnIndex < joinColumns.size(); columnIndex++) {
        rewriter.replaceAllUsesWith(product.getResult(columnIndex), joinColumns[columnIndex]);
    }

    dropConjunct(filter, match._equality.getResult(), rewriter);
    rewriter.eraseOp(product);
}

struct FuseHashJoin : public impl::FuseHashJoinBase<FuseHashJoin> {
    FuseHashJoin() {}

    FuseHashJoin(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        const DBPassContext& context = *_context;

        const auto match = [&context](FilterOp filter, EqualityCross& cross) {
            OpAnswers readAlone;

            return matchAConjunct(filter, [&filter, &cross, &context, &readAlone](Value conjunct) {
                return matchEqualityConjunct(filter, conjunct, readAlone, cross) && !prefersTheProduct(cross, context);
            });
        };

        const auto fuse = [&context](EqualityCross& cross, mlir::RewriterBase& rewriter) {
            cross._buildsTheLeftFactor = buildsTheLeftFactor(cross, context);
            fuseHashJoin(cross, rewriter);
        };

        runFilterWorklist<EqualityCross>(getOperation(), match, fuse);
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

// A whole node scan whose column reaches an equality filter through ops that only hand it
// on, the other side of the equality being the IDs to fetch. Each link is the result an op
// hands the column on as, from the one the equality reads back to the scan.
struct FetchNodesChain {
    FilterOp _filter {nullptr};
    EqOp _equality {nullptr};
    llvm::SmallVector<Value, 2> _otherConjuncts;
    Value _nodes;
    Value _ids;
    Operation* _scan {nullptr};
    ArrayAttr _labels;
    llvm::SmallVector<OpResult> _links;
    llvm::SmallVector<std::pair<BroadcastConstant, Value>> _redrivenBroadcasts;
};

// A db.broadcast_constant reads no value of its driver, only how many rows it holds
bool readsTheRowsAlone(OpOperand& use) {
    BroadcastConstant broadcast = dyn_cast<BroadcastConstant>(use.getOwner());

    return broadcast && &use != &broadcast.getValueMutable();
}

// The one op reading the values of @param column, or null when there is not exactly one use
// reading them
Operation* soleValueReader(Value column) {
    Operation* reader = nullptr;
    for (OpOperand& use : column.getUses()) {
        if (readsTheRowsAlone(use)) {
            continue;
        } else if (reader) {
            return nullptr;
        }

        reader = use.getOwner();
    }

    return reader;
}

// A column @param reader hands on beside @param column, so one holding the same rows, or
// null when it hands on no other
Value siblingColumnOf(Value column, Operation* reader) {
    OperandRange candidates = reader->getOperands();

    CarrySet carrySet;
    if (matchCarrySet(reader, carrySet)) {
        candidates = candidates.slice(carrySet._operandOffset, carrySet._count);
    } else if (!isa<Yield>(reader)) {
        return {};
    }

    for (const Value candidate : candidates) {
        if (candidate != column) {
            return candidate;
        }
    }

    return {};
}

// The broadcasts counting the rows of @param column, which the fetch removes, each paired
// with the column it counts instead: one @param reader hands on beside it, else
// @param fallback, else none when the column was the only one holding those rows. False
// when that column is not bound ahead of a broadcast reading it.
bool collectRedrivenBroadcasts(Value column, Operation* reader, Value fallback, FetchNodesChain& chain) {
    const Value sibling = siblingColumnOf(column, reader);
    mlir::DominanceInfo dominance;

    for (OpOperand& use : column.getUses()) {
        if (!readsTheRowsAlone(use)) {
            continue;
        }

        BroadcastConstant broadcast = cast<BroadcastConstant>(use.getOwner());

        Value driver;
        if (sibling && dominance.properlyDominates(sibling, broadcast)) {
            driver = sibling;
        } else if (fallback && dominance.properlyDominates(fallback, broadcast)) {
            driver = fallback;
        } else if (sibling || fallback) {
            return false;
        }

        chain._redrivenBroadcasts.emplace_back(broadcast, driver);
    }

    return true;
}

// The column a result is, as handed on by its op: a column carried through a filter, an
// unwind or a node fetch, or one a product's factor yields. Null for any other result.
Value handedOnFrom(OpResult result) {
    Operation* const op = result.getOwner();
    const size_t resultIndex = result.getResultNumber();

    if (isa<FilterOp>(op)) {
        return op->getOperand(resultIndex + 1);
    } else if (isa<Unwind, FetchNodes>(op)) {
        return resultIndex == 0 ? Value() : op->getOperand(resultIndex);
    } else if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
        return productFactorColumn(product, resultIndex);
    }

    return {};
}

bool isFactorOfTheScanAlone(Block* block) {
    const bool inAProduct = isa_and_nonnull<CrossProduct>(block->getParentOp());

    return inAProduct
        && llvm::hasNItems(block->getOperations(), 2)
        && cast<Yield>(block->getTerminator()).getNumOperands() == 1;
}

// A factor yielding the scanned column alone is left empty once the column is dropped,
// which only its product going away can account for - so the scan must be all it holds.
// A filter carrying the scanned column alone would be left a filter of nothing.
bool keepsAColumnOnceDropped(OpResult link, Value handedOn) {
    Operation* const op = link.getOwner();

    if (isa<CrossProduct>(op)) {
        Block* const factor = cast<Yield>(*handedOn.getUsers().begin())->getBlock();
        const bool yieldsOtherColumns = factor->getTerminator()->getNumOperands() > 1;
        const bool holdsTheScanAlone = isa_and_nonnull<ScanNodes, ScanNodesByLabel>(handedOn.getDefiningOp())
                                       && isFactorOfTheScanAlone(factor);

        return yieldsOtherColumns || holdsTheScanAlone;
    } else if (FilterOp filter = dyn_cast<FilterOp>(op)) {
        return filter.getColumnsToFilter().size() > 1;
    }

    return true;
}

bool matchFetchNodesChain(Value nodes, Value ids, FetchNodesChain& chain) {
    if (nodes == ids) {
        return false;
    }

    for (OpOperand& use : nodes.getUses()) {
        Operation* const user = use.getOwner();
        const bool readByTheEquality = user == chain._equality.getOperation();
        const bool carriedByTheFilter = user == chain._filter.getOperation() && use.getOperandNumber() > 0;

        if (!readByTheEquality && !carriedByTheFilter && !readsTheRowsAlone(use)) {
            return false;
        }
    }

    chain._links.clear();
    chain._redrivenBroadcasts.clear();

    if (!collectRedrivenBroadcasts(nodes, chain._filter, ids, chain)) {
        return false;
    }

    Value column = nodes;
    while (!isa_and_nonnull<ScanNodes, ScanNodesByLabel>(column.getDefiningOp())) {
        const OpResult link = dyn_cast<OpResult>(column);
        if (!link) {
            return false;
        }

        const Value handedOn = handedOnFrom(link);
        if (!handedOn) {
            return false;
        }

        Operation* const reader = soleValueReader(handedOn);
        if (!reader || !keepsAColumnOnceDropped(link, handedOn)) {
            return false;
        }

        if (!collectRedrivenBroadcasts(handedOn, reader, Value(), chain)) {
            return false;
        }

        chain._links.push_back(link);
        column = handedOn;
    }

    Operation* const scan = column.getDefiningOp();
    Block* const scanBlock = scan->getBlock();

    const bool scanStandsBesideTheFilter = scanBlock == chain._filter->getBlock();
    if (!scanStandsBesideTheFilter && !isFactorOfTheScanAlone(scanBlock)) {
        return false;
    }

    chain._nodes = nodes;
    chain._ids = ids;
    chain._scan = scan;

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        chain._labels = scanByLabel.getLabels();
    }

    return true;
}

// Whether @param conjunct is computed by row computations from the columns @param filter
// carries alone, so it can be rebuilt over the rows a fusion makes in place of those columns
bool computesFromTheCarriedColumns(FilterOp filter, Value conjunct) {
    const Operation::operand_range carried = filter.getColumnsToFilter();
    Block* const block = filter->getBlock();

    llvm::SmallVector<Value> pending {conjunct};
    llvm::DenseSet<Value> visited;
    while (!pending.empty()) {
        const Value value = pending.pop_back_val();
        if (llvm::is_contained(carried, value) || !visited.insert(value).second) {
            continue;
        }

        Operation* const def = value.getDefiningOp();
        if (def && def->getBlock() != block && ::db::yieldsConstantColumn(value)) {
            continue;
        } else if (!def || def->getBlock() != block || !computesFromTheRow(def)) {
            return false;
        }

        llvm::append_range(pending, def->getOperands());
    }

    return true;
}

bool matchFetchNodesConjunct(FilterOp filter, Value conjunct, FetchNodesChain& chain) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!isEqualityOfTheFilter(equality, filter)) {
        return false;
    }

    chain._filter = filter;
    chain._equality = equality;

    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    return matchFetchNodesChain(lhs, rhs, chain) || matchFetchNodesChain(rhs, lhs, chain);
}

// The fetch takes one conjunct and rebuilds the others over the fetched rows, so at most one
// conjunct may be one it cannot rebuild, and that one has to be the one it takes
bool matchFetchNodes(FilterOp filter, FetchNodesChain& chain) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    Value unrebuilt;
    for (const Value conjunct : conjuncts) {
        if (computesFromTheCarriedColumns(filter, conjunct)) {
            continue;
        } else if (unrebuilt && unrebuilt != conjunct) {
            return false;
        }

        unrebuilt = conjunct;
    }

    for (const Value conjunct : conjuncts) {
        const bool rebuildsTheOthers = !unrebuilt || unrebuilt == conjunct;
        if (!rebuildsTheOthers || !matchFetchNodesConjunct(filter, conjunct, chain)) {
            continue;
        }

        chain._otherConjuncts.assign(conjuncts.begin(), conjuncts.end());
        llvm::erase(chain._otherConjuncts, conjunct);

        return true;
    }

    return false;
}

// Cuts @param columns, the rows a fusion makes in place of the columns @param filter carries,
// one for one, to the rows @param conjuncts keep, their computation rebuilt over them. Fills
// @param cone with the ops computing them over the filter's own rows.
void keepConjunctsOver(FilterOp filter,
                       llvm::ArrayRef<Value> conjuncts,
                       llvm::SmallVectorImpl<Value>& columns,
                       MaskCone& cone,
                       mlir::RewriterBase& rewriter) {
    if (conjuncts.empty()) {
        return;
    }

    Block* const block = filter->getBlock();
    const auto computedFromTheRows = [block](Operation* op) {
        return op->getBlock() == block && computesFromTheRow(op);
    };

    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value conjunct : conjuncts) {
        collectConePostOrder(conjunct, computedFromTheRows, visited, cone);
    }

    mlir::IRMapping mapping;
    for (const auto [carried, column] : llvm::zip_equal(filter.getColumnsToFilter(), columns)) {
        mapping.map(carried, column);
    }

    cloneRowCone(cone._ops, mapping, rewriter);

    llvm::SmallVector<Value, 4> rebuilt;
    for (const Value conjunct : conjuncts) {
        rebuilt.push_back(mapping.lookupOrDefault(conjunct));
    }

    const ValueRange columnRange(columns);
    const Value mask = conjoin(rebuilt, filter.getLoc(), rewriter);
    FilterOp kept = rewriter.create<FilterOp>(filter.getLoc(), columnRange.getTypes(), mask, columnRange);

    columns.assign(kept.getResults().begin(), kept.getResults().end());
}

CrossProduct dropProductColumn(CrossProduct product, size_t resultIndex, mlir::RewriterBase& rewriter) {
    Operation* const productOp = product.getOperation();
    const size_t leftCount = factorYield(productOp, 0).getNumOperands();

    llvm::SmallBitVector keep(productOp->getNumResults(), true);
    keep.reset(resultIndex);

    llvm::SmallVector<Type> keptTypes;
    keptResultTypes(productOp, keep, keptTypes);

    rewriter.setInsertionPoint(product);
    CrossProduct trimmed = rewriter.create<CrossProduct>(product.getLoc(), keptTypes);

    replaceWithTrimmedFactors(productOp, trimmed.getOperation(), keep, leftCount);
    rewriter.eraseOp(productOp);

    return trimmed;
}

void dropCarriedColumn(Operation* op, size_t resultIndex, mlir::RewriterBase& rewriter) {
    CarrySet carrySet;
    const bool carries = matchCarrySet(op, carrySet);
    bioassert(carries, "A fetch chain link without a carry set");

    const size_t droppedIndex = resultIndex - carrySet._resultOffset;

    llvm::SmallVector<size_t> kept;
    for (size_t carriedIndex = 0; carriedIndex < carrySet._count; carriedIndex++) {
        if (carriedIndex != droppedIndex) {
            kept.push_back(carriedIndex);
        }
    }

    trimCarrySet(op, carrySet, kept, rewriter);
    rewriter.eraseOp(op);
}

// A product one of whose factors yields nothing makes the other factor's rows alone
void inlineTheFactorLeft(CrossProduct product, mlir::RewriterBase& rewriter) {
    Operation* const productOp = product.getOperation();
    const bool leftIsEmpty = factorYield(productOp, 0).getNumOperands() == 0;
    const bool rightIsEmpty = factorYield(productOp, 1).getNumOperands() == 0;

    if (!leftIsEmpty && !rightIsEmpty) {
        return;
    }

    Region& keptFactor = leftIsEmpty ? product.getRightFactor() : product.getLeftFactor();
    Block& keptBlock = keptFactor.front();
    Yield keptYield = cast<Yield>(keptBlock.getTerminator());

    const llvm::SmallVector<Value> yielded(keptYield.getColumns().begin(), keptYield.getColumns().end());

    product->getBlock()->getOperations().splice(Block::iterator(productOp),
                                                keptBlock.getOperations(),
                                                keptBlock.begin(),
                                                Block::iterator(keptYield));

    for (size_t index = 0; index < yielded.size(); index++) {
        product.getResult(index).replaceAllUsesWith(yielded[index]);
    }

    rewriter.eraseOp(product);
}

void fuseFetchNodes(FetchNodesChain& chain, mlir::RewriterBase& rewriter) {
    for (const std::pair<BroadcastConstant, Value>& redriven : chain._redrivenBroadcasts) {
        BroadcastConstant broadcast = redriven.first;
        const Value driver = redriven.second;

        rewriter.modifyOpInPlace(broadcast, [&broadcast, driver] {
            mlir::MutableOperandRange driverOperand = broadcast.getDriverMutable();
            if (driver) {
                driverOperand.assign(driver);
            } else {
                driverOperand.clear();
            }
        });
    }

    FilterOp filter = chain._filter;
    const Location loc = filter.getLoc();

    const Operation::operand_range carried = filter.getColumnsToFilter();
    const ResultRange filtered = filter.getFilteredColumns();

    llvm::SmallVector<Value> fetchCarried;
    llvm::SmallVector<Type> fetchTypes {chain._nodes.getType()};
    for (const Value column : carried) {
        if (column != chain._nodes) {
            fetchCarried.push_back(column);
            fetchTypes.push_back(column.getType());
        }
    }

    rewriter.setInsertionPoint(filter);
    FetchNodes fetch = rewriter.create<FetchNodes>(loc, fetchTypes, chain._ids, fetchCarried);

    llvm::SmallVector<Value> fetched(fetch->getResults().begin(), fetch->getResults().end());
    if (chain._labels) {
        keepLabelledRows(fetched, chain._labels, loc, rewriter);
    }

    llvm::SmallVector<Value> columns;
    size_t fetchedIndex = 1;
    for (const Value column : carried) {
        const bool holdsTheNodes = column == chain._nodes;
        columns.push_back(holdsTheNodes ? fetched.front() : fetched[fetchedIndex++]);
    }

    MaskCone otherConjunctsCone;
    keepConjunctsOver(filter, chain._otherConjuncts, columns, otherConjunctsCone, rewriter);

    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    const Value mask = filter.getMask();
    rewriter.eraseOp(filter);
    eraseUnusedConjunction(mask, rewriter);
    rewriter.eraseOp(chain._equality);
    eraseUnusedRowCone(otherConjunctsCone._ops, rewriter);

    llvm::SmallVector<CrossProduct> products;
    for (const OpResult link : chain._links) {
        Operation* const op = link.getOwner();
        const size_t resultIndex = link.getResultNumber();

        if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
            products.push_back(dropProductColumn(product, resultIndex, rewriter));
        } else {
            dropCarriedColumn(op, resultIndex, rewriter);
        }
    }

    rewriter.eraseOp(chain._scan);

    for (CrossProduct product : llvm::reverse(products)) {
        inlineTheFactorLeft(product, rewriter);
    }
}

struct FuseFetchNodes : public impl::FuseFetchNodesBase<FuseFetchNodes> {
    void runOnOperation() override {
        runFilterWorklist<FetchNodesChain>(getOperation(), matchFetchNodes, fuseFetchNodes);
    }
};

struct PatternSeed {
    PatternJunction _junction;
    SeedPattern _pattern;
    FilterOp _filter {nullptr};
    EqOp _equality {nullptr};
    Value _seeded;
    Value _ids;
    const PatternStep* _fetchedStep {nullptr};
    llvm::SmallVector<WalkEvent> _events;
};

// The junction column each column of a block is carried from through the filters after it,
// kept so the filters of one chain trace it once rather than once per filter. A reroot
// rewrites the block, so clear() follows every one.
class JunctionTraces {
public:
    OpResult trace(Value column, Block* block) {
        llvm::SmallVector<Value> climbed;
        Value junction;

        for (;;) {
            const OpResult result = dyn_cast<OpResult>(column);
            if (!result || result.getOwner()->getBlock() != block) {
                break;
            }

            const auto tracedIt = _junctions.find(column);
            if (tracedIt != _junctions.end()) {
                junction = tracedIt->second;
                break;
            }

            climbed.push_back(column);

            Operation* const op = result.getOwner();
            FilterOp filter = dyn_cast<FilterOp>(op);
            if (isa<CrossProduct, Unwind>(op)) {
                junction = column;
                break;
            } else if (!filter) {
                break;
            }

            column = filter.getColumnsToFilter()[result.getResultNumber()];
        }

        for (const Value value : climbed) {
            _junctions[value] = junction;
        }

        return junction ? cast<OpResult>(junction) : nullptr;
    }

    void clear() {
        _junctions.clear();
    }

private:
    llvm::DenseMap<Value, Value> _junctions;
};

bool matchSeedJunction(OpResult seededResult, PatternJunction& junction, SeedPattern& pattern) {
    Operation* const op = seededResult.getOwner();

    if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
        const size_t leftCount = factorYield(op, 0).getNumOperands();
        const bool patternOnTheLeft = seededResult.getResultNumber() < leftCount;
        readProductJunction(product, patternOnTheLeft, junction);

        Region& patternFactor = patternOnTheLeft ? product.getLeftFactor() : product.getRightFactor();

        const size_t outputIndex = llvm::find(junction._patternResults, Value(seededResult)) - junction._patternResults.begin();
        Operation* const root = findPatternRoot(junction._patternOutputs[outputIndex]);
        if (!root || root->getParentRegion() != &patternFactor) {
            return false;
        }

        Block& block = patternFactor.front();
        llvm::SmallVector<Operation*> orderedOps;
        for (Operation& patternOp : llvm::make_range(block.begin(), Block::iterator(block.getTerminator()))) {
            pattern._ops.insert(&patternOp);
            orderedOps.push_back(&patternOp);
        }

        pattern._root = root;
        return analyzePattern(pattern, orderedOps);
    }

    Unwind unwind = cast<Unwind>(op);
    if (seededResult.getResultNumber() == 0) {
        return false;
    }

    readUnwindJunction(unwind, junction);

    Operation* const root = findPatternRoot(junction._patternOutputs[seededResult.getResultNumber() - 1]);
    if (!root || root->getBlock() != op->getBlock()) {
        return false;
    }

    collectPatternOps(root, op, pattern);

    llvm::SmallVector<Operation*> orderedOps;
    for (Operation& patternOp : llvm::make_range(Block::iterator(root), Block::iterator(op))) {
        if (pattern._ops.contains(&patternOp)) {
            orderedOps.push_back(&patternOp);
        }
    }

    if (!unwindsOverThePatternAlone(junction, pattern, orderedOps)) {
        return false;
    }

    return analyzePattern(pattern, orderedOps);
}

// @param visited holds the values an earlier call reached without finding the seed, so a
// caller asking of every filter along a chain walks the chain once
bool readsTheSeed(Value value, const PatternJunction& junction, llvm::DenseSet<Value>& visited) {
    llvm::SmallVector<Value> worklist {value};

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();
        if (!visited.insert(current).second) {
            continue;
        }

        if (llvm::is_contained(junction._otherResults, current)) {
            return true;
        }

        Operation* const def = current.getDefiningOp();
        if (!standsFromTheJunction(def, junction)) {
            continue;
        }

        if (FilterOp carrying = dyn_cast<FilterOp>(def)) {
            const size_t resultIndex = cast<OpResult>(current).getResultNumber();
            worklist.push_back(carrying.getColumnsToFilter()[resultIndex]);
        } else {
            llvm::append_range(worklist, def->getOperands());
        }
    }

    return false;
}

bool isComputedFromTheSeed(Value ids, const PatternJunction& junction, Operation* filter) {
    llvm::SmallVector<Value> worklist {ids};
    llvm::DenseSet<Value> visited;
    llvm::DenseSet<Value> masksVisited;

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();
        if (!visited.insert(current).second || llvm::is_contained(junction._otherResults, current)) {
            continue;
        }

        Operation* const def = current.getDefiningOp();
        if (!standsFromTheJunction(def, junction)) {
            continue;
        }

        if (def == junction._op || !def->isBeforeInBlock(filter)) {
            return false;
        }

        if (FilterOp carrying = dyn_cast<FilterOp>(def)) {
            if (readsTheSeed(carrying.getMask(), junction, masksVisited)) {
                return false;
            }

            const size_t resultIndex = cast<OpResult>(current).getResultNumber();
            worklist.push_back(carrying.getColumnsToFilter()[resultIndex]);
        } else if (!computesPerRow(def)) {
            return false;
        } else {
            llvm::append_range(worklist, def->getOperands());
        }
    }

    return true;
}

// The junction's rows reach the equality's filter through filters and row-wise ops alone,
// which keep their meaning over the rows the equality would have kept
bool readsTheJunctionRowWise(const PatternJunction& junction, FilterOp filter) {
    llvm::SmallPtrSet<Operation*, 16> readers;
    Operation* const junctionOp = junction._op;

    for (Operation& op : llvm::make_range(std::next(Block::iterator(junctionOp)), Block::iterator(filter.getOperation()))) {
        const bool readsTheRows = llvm::any_of(op.getOperands(), [&readers, junctionOp](Value operand) {
            Operation* const def = operand.getDefiningOp();
            return def == junctionOp || (def && readers.contains(def));
        });

        if (!readsTheRows) {
            continue;
        }

        if (!isa<FilterOp>(op) && !computesPerRow(&op)) {
            return false;
        }

        readers.insert(&op);
    }

    readers.insert(junctionOp);
    for (Operation* const reader : readers) {
        for (Operation* const user : reader->getUsers()) {
            if (user != filter.getOperation() && !readers.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

// The values db.fetch_edges reads an edge out of: an edge, an integer, or a type-erased cell
bool namesEdgesByID(Value ids) {
    const Type element = ::db::declaredColumnElement(ids);

    return element.isSignlessInteger(64) || isa<storage::EdgeIDType, storage::AnyType>(element);
}

bool matchPatternSeedSide(FilterOp filter, EqOp equality, Value seeded, Value ids, JunctionTraces& traces, PatternSeed& seed) {
    const bool seedsAnEdge = isEdgeColumn(seeded) && namesEdgesByID(ids);
    if (!isNodeColumn(seeded) && !seedsAnEdge) {
        return false;
    }

    Block* const block = filter->getBlock();
    const OpResult seededResult = traces.trace(seeded, block);
    if (!seededResult) {
        return false;
    }

    // GCC 13 reads moving an empty event list into the seed as a memmove past its end
    // (-Warray-bounds), so the seed is rebuilt in place rather than assigned a fresh one
    std::destroy_at(&seed);
    std::construct_at(&seed);

    if (!matchSeedJunction(seededResult, seed._junction, seed._pattern)) {
        return false;
    }

    const PatternJunction& junction = seed._junction;
    const size_t outputIndex = llvm::find(junction._patternResults, Value(seededResult)) - junction._patternResults.begin();
    seed._seeded = patternOrigin(junction._patternOutputs[outputIndex], seed._pattern);
    seed._fetchedStep = seedsAnEdge ? findEdgeStep(seed._pattern, seed._seeded) : nullptr;

    const bool seedsAPatternEntity = seedsAnEdge ? static_cast<bool>(seed._fetchedStep) : isPatternNode(seed._pattern, seed._seeded);
    const bool walkableFromTheSeed = seedsAPatternEntity && isWalkableFrom(seed._pattern, seed._seeded, seed._fetchedStep);
    if (!walkableFromTheSeed) {
        return false;
    }

    const bool idsOverTheSeedRows = isComputedFromTheSeed(ids, junction, filter) && readsTheJunctionRowWise(junction, filter);
    if (!idsOverTheSeedRows) {
        return false;
    }

    // A literal list equated to the root or to an edge folds into an ID disjunction over the
    // pattern, which the scans by ID turn into a const scan listing the nodes or the edges at
    // plan time
    UnwindEqualityCross unwindEquality;
    CrossProduct product = dyn_cast<CrossProduct>(junction._op);
    const bool seedsTheRoot = seed._seeded == seed._pattern._root->getResult(0);
    const bool listsTheSeeds = seed._fetchedStep || seedsTheRoot;
    const bool foldsIntoAConstScan = product && listsTheSeeds && matchUnwindEqualityCross(product, unwindEquality);
    if (foldsIntoAConstScan) {
        return false;
    }

    const llvm::SmallPtrSet<Operation*, 1> noneApplied;
    if (!planWalk(seed._pattern, seed._seeded, seed._fetchedStep, noneApplied, false, seed._events)) {
        return false;
    }

    seed._filter = filter;
    seed._equality = equality;
    seed._ids = ids;

    return true;
}

bool matchPatternSeed(FilterOp filter, JunctionTraces& traces, PatternSeed& seed) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        if (!isEqualityOfTheFilter(equality, filter)) {
            continue;
        }

        const Value lhs = equality.getLhs();
        const Value rhs = equality.getRhs();

        if (matchPatternSeedSide(filter, equality, lhs, rhs, traces, seed) || matchPatternSeedSide(filter, equality, rhs, lhs, traces, seed)) {
            return true;
        }
    }

    return false;
}

Value cloneSeedIDs(Value ids, const PatternJunction& junction, mlir::IRMapping& seedColumns, mlir::OpBuilder& builder) {
    llvm::SmallVector<Value> worklist {ids};

    while (!worklist.empty()) {
        const Value value = worklist.back();
        if (seedColumns.contains(value)) {
            worklist.pop_back();
            continue;
        }

        Operation* const def = value.getDefiningOp();
        if (!standsFromTheJunction(def, junction)) {
            seedColumns.map(value, value);
            worklist.pop_back();
            continue;
        }

        const size_t resultIndex = cast<OpResult>(value).getResultNumber();

        if (FilterOp carrying = dyn_cast<FilterOp>(def)) {
            const Value carried = carrying.getColumnsToFilter()[resultIndex];
            if (seedColumns.contains(carried)) {
                seedColumns.map(value, seedColumns.lookup(carried));
                worklist.pop_back();
            } else {
                worklist.push_back(carried);
            }

            continue;
        }

        bool operandsPending = false;
        for (const Value operand : llvm::reverse(def->getOperands())) {
            if (!seedColumns.contains(operand)) {
                worklist.push_back(operand);
                operandsPending = true;
            }
        }

        if (operandsPending) {
            continue;
        }

        Operation* const cloned = builder.clone(*def, seedColumns);
        seedColumns.map(value, cloned->getResult(resultIndex));
        worklist.pop_back();
    }

    return seedColumns.lookup(ids);
}

// Whether every row of a column names a node the graph holds, so that a fetch would keep
// it as it is: a scan's nodes, a fetch's, or either end of a hop or a walk
bool namesGraphNodes(Value column) {
    for (;;) {
        const OpResult result = dyn_cast<OpResult>(column);
        if (!result) {
            return false;
        }

        Operation* const op = result.getOwner();
        const size_t resultIndex = result.getResultNumber();

        if (isa<ScanNodes, ScanNodesByLabel, ScanNodesByPropertyValue>(op)) {
            return true;
        } else if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            column = filter.getColumnsToFilter()[resultIndex];
        } else if (isa<FetchNodes>(op) && resultIndex > 0) {
            column = op->getOperand(resultIndex);
        } else if (isa<FetchNodes>(op)) {
            return true;
        } else if (isa<FetchEdges>(op) && resultIndex >= hopFixedResultCount) {
            column = op->getOperand(1 + resultIndex - hopFixedResultCount);
        } else if (isa<FetchEdges>(op)) {
            return resultIndex == 0 || resultIndex == EDGE_TARGETS_RESULT;
        } else if (isEdgeHop(op) && resultIndex >= hopFixedResultCount) {
            column = op->getOperand(1 + resultIndex - hopFixedResultCount);
        } else if (isEdgeHop(op)) {
            return resultIndex == hopInputResult(op) || resultIndex == hopReachedResult(op);
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op); exploration && resultIndex >= pathFixedResultCount) {
            column = exploration.getColumnsToFilter()[resultIndex - pathFixedResultCount];
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
            return column == exploration.getSrcids() || column == exploration.getTgtids();
        } else {
            return false;
        }
    }
}

void reroot(PatternSeed& seed, mlir::RewriterBase& rewriter) {
    PatternJunction& junction = seed._junction;
    Operation* const junctionOp = junction._op;
    const Location loc = junctionOp->getLoc();

    rewriter.setInsertionPoint(junctionOp);

    // The seed's rows on their own: the other factor's body, or the list unwound again
    // without the pattern's columns
    llvm::SmallVector<Value> seedColumns;
    if (junction._otherFactor) {
        const Operation::operand_range yielded = factorYieldColumns(*junction._otherFactor);
        seedColumns.assign(yielded.begin(), yielded.end());

        Block& seedBlock = junction._otherFactor->front();
        junctionOp->getBlock()->getOperations().splice(Block::iterator(junctionOp),
                                                       seedBlock.getOperations(),
                                                       seedBlock.begin(),
                                                       Block::iterator(seedBlock.getTerminator()));
    } else {
        Unwind unwind = cast<Unwind>(junctionOp);
        const llvm::SmallVector<Type> types {unwind.getElement().getType()};
        Unwind seedUnwind = rewriter.create<Unwind>(loc, types, unwind.getSource(), ValueRange());
        seedColumns.push_back(seedUnwind.getElement());
    }

    mlir::IRMapping seedMapping;
    for (size_t index = 0; index < seedColumns.size(); index++) {
        seedMapping.map(junction._otherResults[index], seedColumns[index]);
    }

    const Value ids = cloneSeedIDs(seed._ids, junction, seedMapping, rewriter);
    const PatternStep* const fetchedStep = seed._fetchedStep;

    llvm::SmallVector<Value> seededColumns {ids};
    size_t seededCount = 1;
    if (fetchedStep) {
        Operation* const hop = fetchedStep->_op;

        llvm::SmallVector<Type> fetchTypes;
        for (size_t index = 0; index < hopFixedResultCount; index++) {
            fetchTypes.push_back(hop->getResult(index).getType());
        }

        for (const Value column : seedColumns) {
            fetchTypes.push_back(column.getType());
        }

        FetchEdges fetch = rewriter.create<FetchEdges>(loc, fetchTypes, ids, seedColumns, isa<GetEdges>(hop));
        seededColumns.assign(fetch->getResults().begin(), fetch->getResults().end());
        seededCount = hopFixedResultCount;
    } else if (namesGraphNodes(ids)) {
        llvm::append_range(seededColumns, seedColumns);
    } else {
        llvm::SmallVector<Type> fetchTypes {seed._seeded.getType()};
        for (const Value column : seedColumns) {
            fetchTypes.push_back(column.getType());
        }

        FetchNodes fetch = rewriter.create<FetchNodes>(loc, fetchTypes, ids, seedColumns);
        seededColumns.assign(fetch->getResults().begin(), fetch->getResults().end());
    }

    PatternReplay replay(seed._pattern, rewriter, loc);
    for (size_t index = 0; index < seedColumns.size(); index++) {
        replay.bind(junction._otherResults[index], seededColumns[seededCount + index]);
    }

    if (fetchedStep) {
        Operation* const hop = fetchedStep->_op;

        replay.bind(fetchedStep->_from, seededColumns[hopInputResult(hop)]);
        replay.bind(fetchedStep->_to, seededColumns[hopReachedResult(hop)]);
        replay.bind(hop->getResult(EDGE_IDS_RESULT), seededColumns[EDGE_IDS_RESULT]);
        replay.bind(hop->getResult(EDGE_TYPES_RESULT), seededColumns[EDGE_TYPES_RESULT]);
    } else {
        replay.bind(seed._seeded, seededColumns.front());
    }

    replay.walk(seed._events);

    for (size_t index = 0; index < junction._patternResults.size(); index++) {
        junction._patternResults[index].replaceAllUsesWith(replay.materialize(junction._patternOutputs[index]));
    }

    for (Value seedResult : junction._otherResults) {
        seedResult.replaceAllUsesWith(replay.currentOf(seedResult));
    }

    dropConjunct(seed._filter, seed._equality.getResult(), rewriter);

    if (junction._otherFactor) {
        rewriter.eraseOp(junctionOp);
        return;
    }

    llvm::SmallVector<Operation*> patternOps;
    for (Operation& op : llvm::make_range(Block::iterator(seed._pattern._root), Block::iterator(junctionOp))) {
        if (seed._pattern._ops.contains(&op)) {
            patternOps.push_back(&op);
        }
    }

    rewriter.eraseOp(junctionOp);
    for (Operation* const op : llvm::reverse(patternOps)) {
        op->dropAllUses();
        rewriter.eraseOp(op);
    }
}

struct RerootPatternAtSeed : public impl::RerootPatternAtSeedBase<RerootPatternAtSeed> {
    void runOnOperation() override {
        JunctionTraces traces;

        const auto matchSeed = [&traces](FilterOp filter, PatternSeed& seed) {
            return matchPatternSeed(filter, traces, seed);
        };

        const auto rerootAtSeed = [&traces](PatternSeed& seed, mlir::RewriterBase& rewriter) {
            traces.clear();
            reroot(seed, rewriter);
        };

        runFilterWorklist<PatternSeed>(getOperation(), matchSeed, rerootAtSeed);
    }
};

// A pattern one of whose steps binds the edges a disjunction of ID tests reads after the
// pattern: a hop, rebuilt from the edges the disjunction lists and walked out from both their
// ends, or a walk starting on them, rebuilt from the ends the listed edges leave. The walk
// leaves those ends over other edges too, so the disjunction still tests its rows.
struct ListedEdgesPattern {
    SeedPattern _pattern;
    FilterOp _filter {nullptr};
    const PatternStep* _listedStep {nullptr};
    EntityIDList _edgeIDs;
    llvm::SmallVector<Operation*> _patternOps;
    llvm::SmallVector<WalkEvent> _events;
};

// Nothing but the pattern and the filter reads what the pattern binds, and the filter carries
// the pattern's columns alone, so the rows rebuilt for the filter replace them all
bool endsAtTheFilter(const SeedPattern& pattern, llvm::ArrayRef<Operation*> patternOps, FilterOp filter) {
    for (Operation* const patternOp : patternOps) {
        for (Operation* const user : patternOp->getUsers()) {
            if (user != filter.getOperation() && !pattern._ops.contains(user)) {
                return false;
            }
        }
    }

    return llvm::all_of(filter.getColumnsToFilter(), [&pattern](Value column) {
        Operation* const def = column.getDefiningOp();
        return def && pattern._ops.contains(def);
    });
}

const PatternStep* findStep(const SeedPattern& pattern, Operation* op) {
    for (const PatternStep& step : pattern._steps) {
        if (step._op == op) {
            return &step;
        }
    }

    return nullptr;
}

bool matchListedEdgesPattern(FilterOp filter, ListedEdgesPattern& match) {
    const Value mask = filter.getMask();
    const Value column = disjunctionColumn(mask);
    if (!column) {
        return false;
    }

    WalkEndEdge walkStart;
    const bool listsHopEdges = isEdgeColumn(column);
    const bool listsWalkStarts = !listsHopEdges && matchWalkEndEdge(column, walkStart) && walkStart._first;
    if (!listsHopEdges && !listsWalkStarts) {
        return false;
    }

    Operation* const root = findPatternRoot(listsHopEdges ? column : walkStart._walk.getSrcids());
    if (!root || root->getBlock() != filter->getBlock()) {
        return false;
    }

    SeedPattern& pattern = match._pattern;
    collectPatternOps(root, filter, pattern);

    for (Operation& op : llvm::make_range(Block::iterator(root), Block::iterator(filter.getOperation()))) {
        if (pattern._ops.contains(&op)) {
            match._patternOps.push_back(&op);
        }
    }

    if (!endsAtTheFilter(pattern, match._patternOps, filter) || !analyzePattern(pattern, match._patternOps)) {
        return false;
    }

    const PatternStep* const step = listsHopEdges ? findEdgeStep(pattern, patternOrigin(column, pattern)) : findStep(pattern, walkStart._walk.getOperation());
    if (!step || !collectIDDisjunction(mask, column, match._edgeIDs)) {
        return false;
    }

    const PatternStep* const fetchedStep = listsHopEdges ? step : nullptr;
    const Value seeded = listsHopEdges ? step->_op->getResult(EDGE_IDS_RESULT) : step->_from;
    const llvm::SmallPtrSet<Operation*, 1> noneApplied;
    if (!isWalkableFrom(pattern, seeded, fetchedStep) || !planWalk(pattern, seeded, fetchedStep, noneApplied, false, match._events)) {
        return false;
    }

    if (listsWalkStarts) {
        match._events.push_back(WalkEvent {._kind = WalkEvent::Kind::Filter, ._filter = filter});
    }

    match._filter = filter;
    match._listedStep = step;

    return true;
}

// Every row rebuilt from a hop's listed edges holds the disjunction, so the filter testing it
// goes. A walk rebuilt from their ends tests it again.
void rerootAtListedEdges(ListedEdgesPattern& match, mlir::RewriterBase& rewriter) {
    FilterOp filter = match._filter;
    const Location loc = filter.getLoc();
    const PatternStep& step = *match._listedStep;

    rewriter.setInsertionPoint(filter);
    PatternReplay replay(match._pattern, rewriter, loc);

    if (ExplorePaths walk = dyn_cast<ExplorePaths>(step._op)) {
        const Value starts = scanListedEdgeEnds(match._edgeIDs, walk.getStepDirection(0), false, loc, rewriter);
        RemoveDuplicates seeds = rewriter.create<RemoveDuplicates>(loc, TypeRange {starts.getType()}, ValueRange {starts}, Value());
        replay.bind(step._from, seeds.getResult(0));
    } else {
        Operation* const hop = step._op;
        ConstScanEdges listed = scanListedEdges(match._edgeIDs, isa<GetEdges>(hop), loc, rewriter);

        replay.bind(step._from, listed->getResult(hopInputResult(hop)));
        replay.bind(step._to, listed->getResult(hopReachedResult(hop)));
        replay.bind(hop->getResult(EDGE_IDS_RESULT), listed.getEids());
        replay.bind(hop->getResult(EDGE_TYPES_RESULT), listed.getEtypes());
    }

    replay.walk(match._events);

    const ResultRange filtered = filter.getFilteredColumns();
    const Operation::operand_range carried = filter.getColumnsToFilter();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(replay.materialize(carried[index]));
    }

    const MaskCone cone = collectMaskCone(filter.getMask());
    rewriter.eraseOp(filter);

    for (Operation* const op : llvm::reverse(match._patternOps)) {
        op->dropAllUses();
        rewriter.eraseOp(op);
    }

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        if (!match._pattern._ops.contains(coneOp)) {
            eraseIfUnused(coneOp, rewriter);
        }
    }
}

struct RerootPatternAtEdgeIDs : public impl::RerootPatternAtEdgeIDsBase<RerootPatternAtEdgeIDs> {
    void runOnOperation() override {
        runFilterWorklist<ListedEdgesPattern>(getOperation(), matchListedEdgesPattern, rerootAtListedEdges);
    }
};

// The estimate counts rows and edges read rather than time, and a walk off the written root
// reads the nodes after its first hop out of their order, so a pattern moves off the root
// the query wrote only when another start costs this much less
constexpr double REROOT_GAIN_FACTOR = 1.3;

// A pattern rooted at a scan up to the first op reading it that is not rebuilt with it, and
// the walk out of the node making the fewest rows, scanned by the labels its filters ask for.
// A walk crossing the other side of its junction in partway rebuilds the junction there.
struct CheapestRoot {
    SeedPattern _pattern;
    llvm::SmallVector<Operation*> _orderedOps;
    Operation* _end {nullptr};
    Value _start;
    ArrayAttr _labels;
    llvm::SmallVector<WalkEvent> _events;
    JunctionJoins _joins;
    bool _crossesJunction {false};
};

// False when a graph write stands before the end, which the rebuilt pattern would read past
bool collectRootedPattern(Operation* scan, CheapestRoot& match) {
    SeedPattern& pattern = match._pattern;
    pattern._root = scan;
    pattern._ops.insert(scan);
    match._orderedOps.push_back(scan);

    const auto definedByThePattern = [&pattern](Value value) {
        Operation* const def = value.getDefiningOp();
        return def && pattern._ops.contains(def);
    };

    for (Operation& op : llvm::make_range(std::next(Block::iterator(scan)), scan->getBlock()->end())) {
        bool writesTheGraph = false;
        bool readsThePattern = false;
        op.walk([&](Operation* nested) {
            writesTheGraph = writesTheGraph || isGraphWrite(nested);
            readsThePattern = readsThePattern || llvm::any_of(nested->getOperands(), definedByThePattern);
        });

        if (writesTheGraph) {
            return false;
        } else if (!readsThePattern) {
            continue;
        }

        ExplorePaths exploration = dyn_cast<ExplorePaths>(op);
        const bool explores = exploration && walksFreely(exploration);

        const bool buildsThePattern = isa<FilterOp>(op) || isHop(&op) || explores || computesPerRow(&op);
        if (!buildsThePattern) {
            match._end = &op;
            return true;
        }

        FilterOp filter = dyn_cast<FilterOp>(op);
        const bool carriesRows = filter || isHop(&op) || explores;
        const OperandRange carried = filter ? filter.getColumnsToFilter() : op.getOperands();
        if (carriesRows && !llvm::all_of(carried, definedByThePattern)) {
            return false;
        }

        pattern._ops.insert(&op);
        match._orderedOps.push_back(&op);
    }

    return false;
}

void collectWrittenWalk(const CheapestRoot& match, llvm::SmallVectorImpl<WalkEvent>& events) {
    const llvm::SmallVector<PatternStep>& steps = match._pattern._steps;

    size_t stepIndex = 0;
    for (Operation* const op : match._orderedOps) {
        if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            events.push_back(WalkEvent {._kind = WalkEvent::Kind::Filter, ._filter = filter});
        } else if (isHop(op) || isa<ExplorePaths>(op)) {
            events.push_back(WalkEvent {._kind = WalkEvent::Kind::Forward, ._step = &steps[stepIndex++]});
        }
    }
}

bool isComputedAhead(Value value, Operation* op) {
    Operation* const ancestor = value.getParentBlock()->findAncestorOpInBlock(*op);
    if (!ancestor) {
        return false;
    }

    Operation* const def = value.getDefiningOp();

    return !def || def->isBeforeInBlock(ancestor);
}

// The keys fuse_hash_join joins on: each side of the equality computed from one column of a
// side of the junction of its own
void readJoinKeys(Value conjunct, const PatternJunction& junction, bool usesHashJoin, JoinConjunct& join) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!equality) {
        return;
    }

    bool keysThePattern = false;
    bool keysTheOtherSide = false;
    bool hashesKeys = true;
    for (const Value key : {equality.getLhs(), equality.getRhs()}) {
        JoinKeySide side;
        side._key = key;
        collectRowCone({key}, equality->getBlock(), side._cone);

        if (side._cone._inputs.size() != 1) {
            return;
        }

        const Value column = climbFilters(side._cone._inputs.front());
        keysThePattern = keysThePattern || llvm::is_contained(junction._patternResults, column);
        keysTheOtherSide = keysTheOtherSide || llvm::is_contained(junction._otherResults, column);
        hashesKeys = hashesKeys && isHashableKey(side);
    }

    join._equatesKeys = keysThePattern && keysTheOtherSide;
    join._hashes = join._equatesKeys && hashesKeys && usesHashJoin && isa<CrossProduct>(junction._op);
}

// A conjunct reading both sides of the junction. Its cone is cloned where the junction
// stands, so it reads nothing computed after the junction.
bool matchJoinConjunct(Value conjunct,
                       FilterOp filter,
                       const PatternJunction& junction,
                       const SeedPattern& pattern,
                       bool usesHashJoin,
                       JoinConjunct& join) {
    join._conjunct = conjunct;
    join._filter = filter;
    collectRowCone({conjunct}, filter->getBlock(), join._cone);

    bool readsTheOtherSide = false;
    for (const Value input : join._cone._inputs) {
        const Value climbed = climbFilters(input);

        if (llvm::is_contained(junction._otherResults, climbed)) {
            readsTheOtherSide = true;
        } else if (const Value patternColumn = junctionSource(junction, climbed)) {
            collectPatternInputs(patternColumn, pattern, join._patternInputs);
        } else if (!isComputedAhead(input, junction._op)) {
            return false;
        }
    }

    readJoinKeys(conjunct, junction, usesHashJoin, join);

    return readsTheOtherSide && !join._patternInputs.empty();
}

bool matchJunctionJoins(const CheapestRoot& match,
                        const RowEstimator& estimator,
                        const DBPassContext& context,
                        JunctionJoins& joins) {
    const SeedPattern& pattern = match._pattern;
    PatternJunction& junction = joins._junction;

    Yield yield = dyn_cast<Yield>(match._end);
    CrossProduct product = yield ? dyn_cast<CrossProduct>(yield->getParentOp()) : CrossProduct();
    Unwind unwind = dyn_cast<Unwind>(match._end);

    if (product) {
        const bool holdsThePatternAlone = llvm::all_of(yield->getBlock()->without_terminator(), [&pattern](Operation& op) {
            return pattern._ops.contains(&op) || computesFromTheRow(&op);
        });

        if (!holdsThePatternAlone) {
            return false;
        }

        readProductJunction(product, yield->getParentRegion() == &product.getLeftFactor(), junction);
        joins._crossedRows = estimateFactorRows(*junction._otherFactor, estimator);
    } else if (unwind && !unwind.getColumnsToFilter().empty()) {
        readUnwindJunction(unwind, junction);
        if (!unwindsOverThePatternAlone(junction, pattern, match._orderedOps)) {
            return false;
        }

        joins._crossedRows = estimateUnwoundRows(unwind);
    } else {
        return false;
    }

    for (const Value output : junction._patternOutputs) {
        Operation* const def = output.getDefiningOp();
        if (!def || !pattern._ops.contains(def)) {
            return false;
        }
    }

    llvm::SmallVector<std::pair<Value, FilterOp>, 4> conjuncts;
    FilterStack stack(junction._op);
    for (FilterOp filter = stack.getReader(); filter && stack.isExclusiveTo(filter); filter = stack.getReader()) {
        stack.climbTo(filter);

        llvm::SmallVector<Value, 4> filterConjuncts;
        collectConjuncts(filter.getMask(), filterConjuncts);
        for (const Value conjunct : filterConjuncts) {
            conjuncts.emplace_back(conjunct, filter);
        }
    }

    // Equalities go first, so the filter right over a rebuilt junction is one fuse_hash_join
    // can fuse with it into a hash join
    std::stable_partition(conjuncts.begin(), conjuncts.end(), [](const std::pair<Value, FilterOp>& conjunct) {
        return static_cast<bool>(conjunct.first.getDefiningOp<EqOp>());
    });

    for (const auto& [conjunct, filter] : conjuncts) {
        JoinConjunct& join = joins._joins.emplace_back();
        if (!matchJoinConjunct(conjunct, filter, junction, pattern, context._usesHashJoin, join)) {
            joins._joins.pop_back();
        }
    }

    return !joins._joins.empty();
}

bool isStepEvent(const WalkEvent& event) {
    return event._kind == WalkEvent::Kind::Forward || event._kind == WalkEvent::Kind::Backward;
}

// The walk crossing the junction's other side in ahead of its event @param position, after
// which each join conjunct runs before the next step once the steps have bound what it reads
void crossWalkAt(const JunctionJoins& joins,
                 Value start,
                 llvm::ArrayRef<WalkEvent> walk,
                 size_t position,
                 llvm::SmallVectorImpl<WalkEvent>& events) {
    llvm::DenseSet<Value> bound {start};
    llvm::SmallBitVector joined(joins._joins.size());

    const auto joinBound = [&joins, &bound, &joined, &events]() {
        for (size_t index = 0; index < joins._joins.size(); index++) {
            const JoinConjunct& join = joins._joins[index];
            const bool readsBound = llvm::all_of(join._patternInputs, [&bound](void* input) {
                return bound.contains(Value::getFromOpaquePointer(input));
            });

            if (!joined.test(index) && readsBound) {
                events.push_back(WalkEvent {._kind = WalkEvent::Kind::Join, ._join = &join});
                joined.set(index);
            }
        }
    };

    bool crossed = false;
    for (size_t index = 0; index < walk.size(); index++) {
        const WalkEvent& event = walk[index];
        const bool isStep = isStepEvent(event);

        if (index == position) {
            events.push_back(WalkEvent {._kind = WalkEvent::Kind::Cross});
            crossed = true;
            joinBound();
        } else if (crossed && isStep) {
            joinBound();
        }

        events.push_back(event);

        if (isStep) {
            bindStep(*event._step, bound);
        }
    }

    if (!crossed) {
        events.push_back(WalkEvent {._kind = WalkEvent::Kind::Cross});
    }

    joinBound();
}

void collectScanLabelFilters(const SeedPattern& pattern,
                             Value node,
                             llvm::SmallPtrSetImpl<Operation*>& absorbed,
                             llvm::SmallVectorImpl<Attribute>& labels) {
    for (FilterOp filter : pattern._filters) {
        CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
        const ArrayAttr conjunction = check ? check.getConjunction() : ArrayAttr();
        if (!conjunction) {
            continue;
        }

        GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
        if (!labelSet || patternOrigin(labelSet.getInputNodes(), pattern) != node) {
            continue;
        }

        absorbed.insert(filter.getOperation());
        for (const Attribute label : conjunction) {
            if (!llvm::is_contained(labels, label)) {
                labels.push_back(label);
            }
        }
    }
}

bool fusesIntoScanByPropertyValue(FilterOp filter, Value start, const SeedPattern& pattern) {
    const auto readsTheStart = [&pattern, start](Value column) {
        return patternOrigin(column, pattern) == start;
    };

    const MaskCone cone = collectMaskCone(filter.getMask());
    const bool readsTheStartAlone = llvm::all_of(cone._inputs, [&readsTheStart](Value input) {
        return readsTheStart(input) || ::db::isRunInvariant(input);
    });

    if (!readsTheStartAlone) {
        return false;
    }

    llvm::SmallVector<Value> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    return llvm::any_of(conjuncts, [&readsTheStart](Value conjunct) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        PropertyEquality property;
        return equality && matchPropertyEquality(equality, property) && readsTheStart(property._read.getInputNodes());
    });
}

// The types each hop of the pattern keeps by a filter of its own type column, which a later
// pass fuses into the hop
void collectHopTypeChecks(const SeedPattern& pattern, llvm::DenseMap<Operation*, ArrayAttr>& hopTypes) {
    for (FilterOp filter : pattern._filters) {
        CheckEdgeTypeConstraint check = filter.getMask().getDefiningOp<CheckEdgeTypeConstraint>();
        const OpResult edgeTypes = check ? dyn_cast<OpResult>(check.getEdgeTypeIds()) : OpResult();
        if (!edgeTypes) {
            continue;
        }

        Operation* const hop = edgeTypes.getOwner();
        if (isEdgeHop(hop) && edgeTypes.getResultNumber() == EDGE_TYPES_RESULT) {
            hopTypes[hop] = check.getEdgeTypes();
        }
    }
}

// The work a walk does: the rows each op it emits builds, the scan of its first node, then
// each filter and each hop, and the edges each hop reads. A property equality in the first
// filter makes that scan and filter one scan by property value, which emits only the rows
// the filter keeps, and a hop emits only the edges of the types its own filter keeps, as
// the by-type hop it becomes, though it reads every edge of the nodes it leaves. Crossing a
// junction's other side in makes a row per pair, unless it is the hash join fuse_hash_join
// makes of the product and the equality right after it, which reads each side once. An
// equality of a key of each side keeps one pair in as many as the distinct keys of the side
// holding more, as PostgreSQL estimates a join of columns it holds no statistics of.
double estimateWalkCost(const SeedPattern& pattern,
                        Value start,
                        ArrayAttr startLabels,
                        llvm::ArrayRef<WalkEvent> events,
                        const JunctionJoins* joins,
                        const RowEstimator& estimator) {
    const auto nodeOf = [&pattern, joins](Value column) {
        const Value patternColumn = joins ? junctionSource(joins->_junction, climbFilters(column)) : Value();
        return patternOrigin(patternColumn ? patternColumn : column, pattern);
    };

    llvm::DenseMap<Operation*, ArrayAttr> hopTypes;
    collectHopTypeChecks(pattern, hopTypes);

    WalkKnowledge knowledge;
    double rows = estimator.getNodeCount();
    if (startLabels) {
        rows *= estimator.keepLabelled(ArrayAttr::get(startLabels.getContext(), {startLabels}), start, knowledge);
    }

    const bool startsWithFilter = !events.empty() && events.front()._kind == WalkEvent::Kind::Filter;
    const bool scansByPropertyValue = startsWithFilter && fusesIntoScanByPropertyValue(events.front()._filter, start, pattern);

    double cost = scansByPropertyValue ? 0 : rows;
    double crossedPatternRows = 0;
    for (size_t index = 0; index < events.size(); index++) {
        const WalkEvent& event = events[index];
        bool buildsRows = true;

        switch (event._kind) {
            case WalkEvent::Kind::RootLabels: {
                const ArrayAttr rootLabels = cast<ScanNodesByLabel>(pattern._root).getLabels();
                rows *= estimator.keepLabelled(ArrayAttr::get(rootLabels.getContext(), {rootLabels}), pattern._root->getResult(0), knowledge);
            }
            break;
            case WalkEvent::Kind::Filter: {
                FilterOp filter = event._filter;
                rows *= estimator.getSelectivity(filter.getMask(), nodeOf, knowledge);
            }
            break;
            case WalkEvent::Kind::Forward: {
                const PatternStep& step = *event._step;
                const HopEstimate hop = estimator.walkHop(step._op, false, step._from, step._to, hopTypes.lookup(step._op), knowledge);
                cost += rows * hop._edgesRead;
                rows *= hop._rows;
            }
            break;
            case WalkEvent::Kind::Backward: {
                const PatternStep& step = *event._step;
                const HopEstimate hop = estimator.walkHop(step._op, true, step._to, step._from, hopTypes.lookup(step._op), knowledge);
                cost += rows * hop._edgesRead;
                rows *= hop._rows;
            }
            break;
            case WalkEvent::Kind::Cross: {
                const bool joinsNext = index + 1 < events.size() && events[index + 1]._kind == WalkEvent::Kind::Join;
                const bool hashes = joinsNext && events[index + 1]._join->_hashes;
                if (hashes) {
                    cost += rows + joins->_crossedRows;
                    buildsRows = false;
                }

                crossedPatternRows = rows;
                rows *= joins->_crossedRows;
            }
            break;
            case WalkEvent::Kind::Join: {
                const JoinConjunct& join = *event._join;
                if (join._equatesKeys) {
                    const double patternKeys = std::min(DEFAULT_DISTINCT_VALUES, crossedPatternRows);
                    const double otherKeys = std::min(DEFAULT_DISTINCT_VALUES, joins->_crossedRows);
                    rows /= std::max({patternKeys, otherKeys, 1.0});
                } else {
                    rows *= estimator.getSelectivity(join._conjunct, nodeOf, knowledge);
                }
            }
            break;
        }

        if (buildsRows) {
            cost += rows;
        }
    }

    return cost;
}

// A walk crossing the other side in once nothing but join conjuncts is left leaves the
// junction and the filters over it as they stand
void settleCross(CheapestRoot& match) {
    llvm::SmallVector<WalkEvent>& events = match._events;
    const auto cross = llvm::find_if(events, [](const WalkEvent& event) {
        return event._kind == WalkEvent::Kind::Cross;
    });

    const bool joinsAlone = std::all_of(std::next(cross), events.end(), [](const WalkEvent& event) {
        return event._kind == WalkEvent::Kind::Join;
    });

    if (joinsAlone) {
        events.erase(cross, events.end());
    } else {
        match._crossesJunction = true;
    }
}

bool matchCheapestRoot(Operation* scan, const RowEstimator& estimator, const DBPassContext& context, CheapestRoot& match) {
    SeedPattern& pattern = match._pattern;
    if (!collectRootedPattern(scan, match) || !analyzePattern(pattern, match._orderedOps) || pattern._steps.empty()) {
        return false;
    }

    const JunctionJoins* const joins = matchJunctionJoins(match, estimator, context, match._joins) ? &match._joins : nullptr;

    const Value root = scan->getResult(0);
    ScanNodesByLabel rootScan = dyn_cast<ScanNodesByLabel>(scan);
    const ArrayAttr rootLabels = rootScan ? rootScan.getLabels() : ArrayAttr();

    llvm::SmallVector<WalkEvent> written;
    collectWrittenWalk(match, written);

    llvm::SmallVector<WalkEvent> crossedWritten;
    if (joins) {
        crossWalkAt(*joins, root, written, written.size(), crossedWritten);
    }

    const llvm::ArrayRef<WalkEvent> writtenWalk = joins ? crossedWritten : written;

    double cheapest = estimateWalkCost(pattern, root, rootLabels, writtenWalk, joins, estimator) / REROOT_GAIN_FACTOR;
    bool foundCheaper = false;

    const auto considerWalk = [&](Value candidate, ArrayAttr startLabels, llvm::ArrayRef<WalkEvent> events) {
        const double cost = estimateWalkCost(pattern, candidate, startLabels, events, joins, estimator);
        if (cost < cheapest) {
            cheapest = cost;
            foundCheaper = true;

            match._start = candidate;
            match._labels = startLabels;
            match._events.assign(events.begin(), events.end());
        }
    };

    llvm::SmallVector<Value> candidates {root};
    for (const PatternStep& step : pattern._steps) {
        candidates.push_back(step._to);
    }

    for (const Value candidate : candidates) {
        if (!isWalkableFrom(pattern, candidate, nullptr)) {
            continue;
        }

        const bool isTheRoot = candidate == root;

        llvm::SmallVector<Attribute> labels;
        if (isTheRoot && rootLabels) {
            labels.append(rootLabels.begin(), rootLabels.end());
        }

        llvm::SmallPtrSet<Operation*, 4> absorbed;
        collectScanLabelFilters(pattern, candidate, absorbed, labels);
        const ArrayAttr startLabels = labels.empty() ? ArrayAttr() : ArrayAttr::get(scan->getContext(), labels);

        llvm::SmallVector<WalkEvent> events;
        if (!planWalk(pattern, candidate, nullptr, absorbed, isTheRoot || !rootLabels, events)) {
            continue;
        }

        if (joins) {
            // Crossed in only ahead of a step or at the end, the filters a scan or a hop is
            // followed by stay beside it, where the passes fusing them into it look for them
            for (size_t position = 0; position <= events.size(); position++) {
                const bool beforeAStepOrTheEnd = position == events.size() || isStepEvent(events[position]);
                if (!beforeAStepOrTheEnd) {
                    continue;
                }

                llvm::SmallVector<WalkEvent> crossedEvents;
                crossWalkAt(*joins, candidate, events, position, crossedEvents);
                considerWalk(candidate, startLabels, crossedEvents);
            }
        } else {
            considerWalk(candidate, startLabels, events);
        }
    }

    if (foundCheaper && joins) {
        settleCross(match);
    }

    return foundCheaper;
}

void rerootAtCheapestNode(CheapestRoot& match, mlir::RewriterBase& rewriter) {
    const SeedPattern& pattern = match._pattern;
    Operation* const rootOp = pattern._root;
    const Location loc = rootOp->getLoc();
    const Type nodesType = rootOp->getResult(0).getType();

    rewriter.setInsertionPoint(match._end);

    Value scanned;
    if (match._labels) {
        scanned = rewriter.create<ScanNodesByLabel>(loc, nodesType, match._labels).getResult();
    } else {
        scanned = rewriter.create<ScanNodes>(loc, nodesType).getResult();
    }

    PatternReplay replay(pattern, rewriter, loc);
    if (match._crossesJunction) {
        replay.setJunction(&match._joins._junction);
    }

    replay.bind(match._start, scanned);
    replay.walk(match._events);

    if (match._crossesJunction) {
        const JunctionJoins& joins = match._joins;
        Operation* const junctionOp = joins._junction._op;

        for (const Value result : junctionOp->getResults()) {
            rewriter.replaceAllUsesWith(result, replay.materializeJunctionResult(result));
        }

        for (const JoinConjunct& join : joins._joins) {
            dropConjunct(join._filter, join._conjunct, rewriter);
        }

        rewriter.eraseOp(junctionOp);
    } else {
        const auto readOutsideThePattern = [&pattern](OpOperand& use) {
            return !pattern._ops.contains(use.getOwner());
        };

        for (Operation* const op : match._orderedOps) {
            for (Value result : op->getResults()) {
                if (llvm::any_of(result.getUses(), readOutsideThePattern)) {
                    result.replaceUsesWithIf(replay.materialize(result), readOutsideThePattern);
                }
            }
        }
    }

    for (Operation* const op : llvm::reverse(match._orderedOps)) {
        op->dropAllUses();
        rewriter.eraseOp(op);
    }
}

struct RerootPatternAtCheapestNode : public impl::RerootPatternAtCheapestNodeBase<RerootPatternAtCheapestNode> {
    RerootPatternAtCheapestNode() {}

    RerootPatternAtCheapestNode(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        if (!_context->_view) {
            return;
        }

        const RowEstimator estimator(*_context->_view);

        const DBPassContext& context = *_context;
        runWorklist<ScanNodes, ScanNodesByLabel>(getOperation(), [&estimator, &context](Operation* op, mlir::RewriterBase& rewriter) {
            CheapestRoot match;
            if (!matchCheapestRoot(op, estimator, context, match)) {
                return false;
            }

            rerootAtCheapestNode(match, rewriter);
            return true;
        });
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

// The node scans a column's rows come off, in factor order. A cross product's rows are its
// two factors' together, and a factor is free to be a product of its own, so each is walked
// through the column it yields first. Anything else a column can come off - a filter, a hop,
// a property read - has a row count only the engine can reach, so the match fails.
bool collectScans(Value column, llvm::SmallVectorImpl<Operation*>& scans) {
    llvm::SmallVector<Value> pending {column};
    while (!pending.empty()) {
        Operation* const def = pending.pop_back_val().getDefiningOp();
        if (!def) {
            return false;
        }

        if (isa<ScanNodes, ScanNodesByLabel>(def)) {
            scans.push_back(def);
        } else if (CrossProduct product = dyn_cast<CrossProduct>(def)) {
            const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
            const Operation::operand_range rightColumns = factorYieldColumns(product.getRightFactor());
            if (leftColumns.empty() || rightColumns.empty()) {
                return false;
            }

            pending.push_back(rightColumns.front());
            pending.push_back(leftColumns.front());
        } else {
            return false;
        }
    }

    return true;
}

// The one scan a column carries the nodes of. Unlike collectScans this follows the result a
// product was read at: its results are the left factor's yielded columns then the right's,
// so an index past the left's count names a column of the right factor.
Operation* scanBehind(Value column) {
    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return nullptr;
        }

        if (isa<ScanNodes, ScanNodesByLabel>(def)) {
            return def;
        }

        CrossProduct product = dyn_cast<CrossProduct>(def);
        if (!product) {
            return nullptr;
        }

        const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
        const Operation::operand_range rightColumns = factorYieldColumns(product.getRightFactor());
        const size_t resultIndex = cast<OpResult>(column).getResultNumber();
        if (resultIndex >= leftColumns.size() + rightColumns.size()) {
            return nullptr;
        }

        column = resultIndex < leftColumns.size() ? leftColumns[resultIndex]
                                                  : rightColumns[resultIndex - leftColumns.size()];
    }
}

// What a count reads: the scans it tallies, the property whose holders one of them is
// narrowed to, if any, and which scan that is.
struct ScanTally {
    llvm::SmallVector<Operation*> scans;
    StringAttr property;
    size_t propertyScan {0};
};

// The label conjunction db.count_scan_rows spells for one scan: its own label list, or an
// empty one for an unlabelled scan.
Attribute scanLabels(Operation* scan, mlir::OpBuilder& builder) {
    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        return scanByLabel.getLabelsAttr();
    }

    return builder.getStrArrayAttr({});
}

// Whether this count tallies nothing but whole node scans, and what it reads off them.
// count(DISTINCT x) is a different tally over the same rows, so it is left alone.
bool countsWholeScans(Count count, ScanTally& tally) {
    if (count.getDistinct()) {
        return false;
    }

    Value rows = count.getInput();

    // A property read keeps every row and nulls the ones without the value, so count(*)
    // reads through it to the nodes underneath while a plain count tallies the holders.
    if (GetNodeProperties fetch = rows.getDefiningOp<GetNodeProperties>()) {
        rows = fetch.getInputNodes();
        if (!count.getRows()) {
            tally.property = fetch.getPropertyAttr();
        }
    } else {
        // A plain count over anything else charges the non-null rows, which is the row
        // count only on a column that holds no null - the node IDs a scan emits.
        const ColumnType rowsType = dyn_cast<ColumnType>(rows.getType());
        if (!count.getRows() && !(rowsType && isa<storage::NodeIDType>(rowsType.getType()))) {
            return false;
        }
    }

    if (!collectScans(rows, tally.scans)) {
        return false;
    }

    if (!tally.property) {
        return true;
    }

    // The property was read of one of the scans' columns; that scan is the one its holders
    // narrow, and the others contribute their whole node count.
    Operation* const propertyScan = scanBehind(rows);
    const auto scanIt = std::ranges::find(tally.scans, propertyScan);
    if (scanIt == tally.scans.end()) {
        return false;
    }

    tally.propertyScan = static_cast<size_t>(scanIt - tally.scans.begin());

    return true;
}

void countFromMetadata(Count count, const ScanTally& tally, mlir::OpBuilder& builder) {
    llvm::SmallVector<Attribute> labels;
    for (Operation* const scan : tally.scans) {
        labels.push_back(scanLabels(scan, builder));
    }

    // The first scan is where a property is read from unless the op says otherwise, so the
    // common single-scan form carries no index.
    const mlir::Type indexType = builder.getIntegerType(64, /*isSigned=*/false);
    const IntegerAttr propertyScan = tally.propertyScan > 0 ? IntegerAttr::get(indexType, tally.propertyScan)
                                                            : IntegerAttr();

    builder.setInsertionPoint(count);
    CountScanRows scanRows = builder.create<CountScanRows>(count.getLoc(),
                                                           count.getResult().getType(),
                                                           builder.getArrayAttr(labels),
                                                           tally.property,
                                                           propertyScan);

    Operation* const countOp = count.getOperation();
    const Value countedRows = count.getInput();

    countOp->getResult(0).replaceAllUsesWith(scanRows.getResult());
    countOp->erase();

    // Drop the now-dead chain, consumer to producer, each only if unused; a cross product
    // takes the scans in its factor regions down with it.
    Operation* const countedOp = countedRows.getDefiningOp();
    GetNodeProperties fetch = dyn_cast<GetNodeProperties>(countedOp);
    const Value scanned = fetch ? fetch.getInputNodes() : Value();

    eraseIfUnused(countedOp);
    if (scanned) {
        eraseIfUnused(scanned.getDefiningOp());
    }
}

// Whether the query writes to the graph. The graph's counts leave out the query's own
// writes, so none of its counts can be read off them.
bool writesTheGraph(Operation* root) {
    const WalkResult walked = root->walk([](Operation* op) {
        if (isGraphWrite(op)) {
            return WalkResult::interrupt();
        }

        return WalkResult::advance();
    });

    return walked.wasInterrupted();
}

struct CountFromMetadata : public impl::CountFromMetadataBase<CountFromMetadata> {
    CountFromMetadata() {}

    CountFromMetadata(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        if (_context->_hasPendingWrites || writesTheGraph(getOperation())) {
            return;
        }

        // Collect the counts first, since rewriting erases ops and would invalidate the walk.
        llvm::SmallVector<Count> counts;
        getOperation()->walk([&](Count count) {
            counts.push_back(count);
        });

        mlir::OpBuilder builder(&getContext());
        for (const Count count : counts) {
            ScanTally tally;
            if (!countsWholeScans(count, tally)) {
                continue;
            }

            countFromMetadata(count, tally, builder);
        }
    }

private:
    const DBPassContext* _context {&defaultPassContext};
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

std::unique_ptr<Pass> createFuseScanEdgesByEndpointLabel(const DBPassContext* context) {
    return std::make_unique<FuseScanEdgesByEndpointLabel>(context);
}

std::unique_ptr<Pass> createFuseHashJoin(const DBPassContext* context) {
    return std::make_unique<FuseHashJoin>(context);
}

std::unique_ptr<Pass> createCountFromMetadata(const DBPassContext* context) {
    return std::make_unique<CountFromMetadata>(context);
}

std::unique_ptr<Pass> createRerootPatternAtCheapestNode(const DBPassContext* context) {
    return std::make_unique<RerootPatternAtCheapestNode>(context);
}

std::unique_ptr<Pass> createStageFilterConjuncts(const DBPassContext* context) {
    return std::make_unique<StageFilterConjuncts>(context);
}

}
