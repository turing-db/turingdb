#include "DBPasses.h"

#include <algorithm>
#include <limits>
#include <optional>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "CardinalityEstimation.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"

#include "IRConstantColumn.h"
#include "PropertyScanLiteral.h"
#include "DBOps.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSESCANBYLABEL
#define GEN_PASS_DEF_PUSHDOWNFILTERS
#define GEN_PASS_DEF_FUSEUNWINDEQUALITY
#define GEN_PASS_DEF_FUSESCANBYNODEIDS
#define GEN_PASS_DEF_FUSESCANBYPROPERTYVALUE
#define GEN_PASS_DEF_FUSESCANEDGES
#define GEN_PASS_DEF_FUSEEDGESBYTYPE
#define GEN_PASS_DEF_FUSESCANEDGESBYTYPE
#define GEN_PASS_DEF_REUSEPROPERTYREADS
#define GEN_PASS_DEF_FUSEHASHJOIN
#define GEN_PASS_DEF_TRIMUNREADCOLUMNS
#include "DBPasses.h.inc"

namespace {

struct LabelScanChain {
    ScanNodes scan;
    GetNodeLabelSet labelSet;
    CheckLabelConstraint check;
};

bool matchLabelScanChain(FilterOp filter, LabelScanChain& chain) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return false;
    }

    const Value scanColumn = columns[0];

    chain.check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!chain.check) {
        return false;
    }

    chain.labelSet = chain.check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!chain.labelSet || chain.labelSet.getInputNodes() != scanColumn) {
        return false;
    }

    chain.scan = scanColumn.getDefiningOp<ScanNodes>();
    if (!chain.scan) {
        return false;
    }

    return true;
}

void eraseIfUnused(Operation* op) {
    if (op && op->use_empty()) {
        op->erase();
    }
}

// The driver every filter pass shares: collect the filters first, since rewriting erases
// ops and would invalidate the walk, then rewrite each one that matches.
template <typename Match>
void runFilterPass(Operation* root,
                   bool (*matchFilter)(FilterOp, Match&),
                   void (*rewriteFilter)(FilterOp, const Match&, mlir::OpBuilder&),
                   mlir::OpBuilder& builder) {
    llvm::SmallVector<FilterOp> filters;
    root->walk([&](FilterOp filter) {
        filters.push_back(filter);
    });

    for (FilterOp filter : filters) {
        Match match;
        if (!matchFilter(filter, match)) {
            continue;
        }

        rewriteFilter(filter, match, builder);
    }
}

void fuseScanByLabel(FilterOp filter, const LabelScanChain& chain, mlir::OpBuilder& builder) {
    ScanNodes scan = chain.scan;
    GetNodeLabelSet labelSet = chain.labelSet;
    CheckLabelConstraint check = chain.check;

    builder.setInsertionPoint(filter);
    ScanNodesByLabel scanByLabel = builder.create<ScanNodesByLabel>(filter.getLoc(),
                                                                    scan.getResult().getType(),
                                                                    check.getLabels());

    Operation* const filterOp = filter.getOperation();
    filterOp->getResult(0).replaceAllUsesWith(scanByLabel.getResult());
    filterOp->erase();

    // Drop the now-dead chain, consumer to producer, each only if unused.
    eraseIfUnused(check);
    eraseIfUnused(labelSet);
    eraseIfUnused(scan);
}

struct FuseScanByLabel : public impl::FuseScanByLabelBase<FuseScanByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<LabelScanChain>(getOperation(), matchLabelScanChain, fuseScanByLabel, builder);
    }
};

bool isNodeSource(Operation* op) {
    return isa<ScanNodes, ScanNodesByLabel, ConstScanNodes, ScanNodesByPropertyValue>(op);
}

bool isEdgeHop(Operation* op) {
    return isa<GetOutEdges, GetInEdges, GetEdges, GetOutEdgesByType, GetInEdgesByType>(op);
}

bool isReverseHop(Operation* op) {
    return isa<GetInEdges, GetInEdgesByType>(op);
}

constexpr size_t hopFixedResultCount = 4;

// The columns a cross_product factor yields, in result order.
Operation::operand_range factorYieldColumns(mlir::Region& factor) {
    mlir::Block& factorBlock = factor.front();
    Yield yield = cast<Yield>(factorBlock.getTerminator());

    return yield.getColumns();
}

// Walk op chain until we reach a node/edge source or something we can't push down to
// The column its variable was bound at, following every op that passes the column through.
// Sets crossedProducer when one of those builds the rows - a hop or a cross product - as
// opposed to a filter, which only drops them.
using ColumnEquality = std::pair<Value, Value>;

void collectConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts);

// Every equality of two node columns the filter requires: it keeps only the rows where they
// agree, so below it anything reading one of them reads the other just as well. A filter
// whose mask is a conjunction requires each of its conjuncts, equalities among them.
void collectColumnEqualities(FilterOp filter, llvm::SmallVectorImpl<ColumnEquality>& equalities) {
    const auto isNodeColumn = [](Value column) {
        const auto type = dyn_cast<ColumnType>(column.getType());
        return type && isa<storage::NodeIDType>(type.getType());
    };

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        if (!equality || !isNodeColumn(equality.getLhs()) || !isNodeColumn(equality.getRhs())) {
            continue;
        }

        equalities.push_back(ColumnEquality {equality.getLhs(), equality.getRhs()});
    }
}

Value climbToLineageAnchor(Value column, bool& crossedProducer, llvm::SmallVectorImpl<ColumnEquality>* equalities = nullptr) {
    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return {};
        }

        if (isNodeSource(def)) {
            return column;
        }

        if (FilterOp filter = dyn_cast<FilterOp>(def)) {
            const size_t resultIndex = cast<OpResult>(column).getResultNumber();

            if (equalities) {
                collectColumnEqualities(filter, *equalities);
            }

            column = filter.getColumnsToFilter()[resultIndex];

            continue;
        }

        if (CrossProduct product = dyn_cast<CrossProduct>(def)) {
            // Descend into the xprod factor which holds def
            const unsigned resultIndex = cast<OpResult>(column).getResultNumber();

            mlir::Region& leftFactor = product.getLeftFactor();
            const Operation::operand_range leftColumns = factorYieldColumns(leftFactor);
            const unsigned leftCount = leftColumns.size();

            if (resultIndex < leftCount) {
                column = leftColumns[resultIndex];
            } else {
                mlir::Region& rightFactor = product.getRightFactor();
                const Operation::operand_range rightColumns = factorYieldColumns(rightFactor);
                column = rightColumns[resultIndex - leftCount];
            }

            crossedProducer = true;

            continue;
        }

        if (isEdgeHop(def)) {
            const size_t resultIndex = cast<OpResult>(column).getResultNumber();
            constexpr size_t srcResultIndex = 0;
            constexpr size_t tgtResultIndex = 3;

            // The input node re-surfaces as srcids (forward) or tgtids (reverse) and each
            // carried column passes through: those continue a variable that existed before
            // the hop, so keep climbing. The opposite node end and the eids/etypes are
            // bound here, so the variable is born at this hop - its earliest filter point.
            const size_t inputResultIndex = isReverseHop(def) ? tgtResultIndex : srcResultIndex;

            if (resultIndex == inputResultIndex) {
                column = def->getOperand(0);
                crossedProducer = true;
            } else if (resultIndex >= hopFixedResultCount) {
                // Carried columns follow input_nodes (operand 0) in operand order.
                column = def->getOperand(1 + (resultIndex - hopFixedResultCount));
                crossedProducer = true;
            } else {
                return column;
            }
        } else {
            return {};
        }
    }
}

bool isMaskComputeOp(Operation* op) {
    return isa<EqOp, NeqOp, GtOp, LtOp, GteOp, LteOp,
               StartsWithOp, EndsWithOp, ContainsOp,
               AndOp, OrOp, XorOp, NotOp,
               AddOp, SubOp, MulOp, DivOp, ModOp, PowOp, ConcatOp,
               ConstantOp,
               GetNodeProperties, GetEdgeProperties>(op);
}

struct MaskCone {
    llvm::SmallVector<Operation*> _ops;
    llvm::SmallSetVector<Value, 4> _inputs;
};

// One suspended operand loop of the cone walk: the op whose operands are being
// visited, and how many of them are done.
struct ConeFrame {
    Operation* _op {nullptr};
    unsigned _nextOperand {0};
};

// Gathers the mask's compute cone and the external columns feeding it. Valid
// even when the cone spans blocks.
void collectConePostOrder(Value mask, llvm::SmallPtrSet<Operation*, 8>& visited, MaskCone& cone) {
    Operation* const maskDef = mask.getDefiningOp();

    if (!maskDef || !isMaskComputeOp(maskDef)) {
        cone._inputs.insert(mask);
        return;
    }

    // A caller walking several conjuncts into one cone shares the set across the
    // calls, so a root already collected by an earlier conjunct is dropped here.
    if (!visited.insert(maskDef).second) {
        return;
    }

    llvm::SmallVector<ConeFrame> stack {ConeFrame {maskDef, 0}};
    while (!stack.empty()) {
        ConeFrame& frame = stack.back();

        if (frame._nextOperand == frame._op->getNumOperands()) {
            cone._ops.push_back(frame._op);
            stack.pop_back();
            continue;
        }

        const Value operand = frame._op->getOperand(frame._nextOperand);
        frame._nextOperand++;

        Operation* const operandDef = operand.getDefiningOp();
        if (!operandDef || !isMaskComputeOp(operandDef)) {
            cone._inputs.insert(operand);
            continue;
        }

        // A second path can only reach an op through a value that something else
        // also reads, so a singly-used result needs no membership check. An OR
        // chain is singly-used throughout and never touches the set at all.
        const bool reachableOnce = operandDef->getNumResults() == 1 && operand.hasOneUse();
        if (reachableOnce || visited.insert(operandDef).second) {
            stack.push_back(ConeFrame {operandDef, 0});
        }
    }
}

MaskCone collectMaskCone(Value mask) {
    MaskCone cone;

    llvm::SmallPtrSet<Operation*, 8> visited;
    collectConePostOrder(mask, visited, cone);

    return cone;
}

// A single-variable property-predicate filter and where its mask should be rebuilt:
// the lineage anchor of the one column its mask reads.
struct PushablePredicate {
    Value _anchor;
    MaskCone _cone;
};

// The anchor a predicate can be rebuilt on instead of @param anchor: the equalities the climb
// crossed hold whole classes of columns to the same node, and where one of the class was
// scanned for, the predicate reads that node off the scan. Applying it there is what spares
// the walk between the two. The class is closed over the equalities rather than read a pair
// at a time, so a column held to the scan through an intermediate is still found.
Value scannedAnchorEqualTo(Value anchor, llvm::ArrayRef<ColumnEquality> equalities) {
    if (!anchor || isNodeSource(anchor.getDefiningOp())) {
        return {};
    }

    llvm::SmallVector<Value, 4> anchorClass {anchor};
    llvm::SmallPtrSet<void*, 4> seen {anchor.getAsOpaquePointer()};

    const auto anchorOf = [](Value column) {
        bool crossed = false;
        return climbToLineageAnchor(column, crossed);
    };

    for (size_t index = 0; index < anchorClass.size(); index++) {
        for (const ColumnEquality& equality : equalities) {
            const Value lhs = anchorOf(equality.first);
            const Value rhs = anchorOf(equality.second);
            if (!lhs || !rhs) {
                continue;
            }

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

bool matchPushablePredicate(FilterOp filter, PushablePredicate& pushable) {
    Operation* const maskDef = filter.getMask().getDefiningOp();
    if (!maskDef || !isMaskComputeOp(maskDef)) {
        return false;
    }

    pushable._cone = collectMaskCone(filter.getMask());
    if (pushable._cone._inputs.empty()) {
        return false;
    }

    bool crossedProducer = false;
    llvm::SmallVector<ColumnEquality, 4> equalities;
    for (const Value input : pushable._cone._inputs) {
        const Value inputAnchor = climbToLineageAnchor(input, crossedProducer, &equalities);
        if (!inputAnchor) {
            return false;
        }

        if (!pushable._anchor) {
            pushable._anchor = inputAnchor;
        } else if (pushable._anchor != inputAnchor) {
            // More than one lineage feeds the mask: not a single-variable predicate.
            return false;
        }
    }

    const Value equatedAnchor = scannedAnchorEqualTo(pushable._anchor, equalities);
    if (equatedAnchor) {
        pushable._anchor = equatedAnchor;
        crossedProducer = true;
    }

    // Crossing only filters leaves the predicate over the rows the anchor produced already:
    // moving it would swap two filters over the same rows, or step between a constraint
    // filter and the op that is about to absorb it.
    return crossedProducer;
}

void pushDownPredicate(FilterOp filter, const PushablePredicate& pushable, mlir::OpBuilder& builder) {
    Value anchor = pushable._anchor;
    const MaskCone& cone = pushable._cone;
    const mlir::Location loc = filter.getLoc();

    Operation* const anchorProducer = anchor.getDefiningOp();

    llvm::SmallVector<mlir::Value> liveColumns;
    for (const mlir::Value result : anchorProducer->getResults()) {
        if (!result.use_empty()) {
            liveColumns.push_back(result);
        }
    }

    // Rebuild the mask over the anchor version of the column, right after it is bound.
    mlir::IRMapping mapping;
    for (const Value input : cone._inputs) {
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
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    const Operation::operand_range carried = filter.getColumnsToFilter();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(carried[index]);
    }
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
}

struct PushDownFilters : public impl::PushDownFiltersBase<PushDownFilters> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<PushablePredicate>(getOperation(), matchPushablePredicate, pushDownPredicate, builder);
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

Value filterByLabels(Value nodes, ArrayAttr labels, mlir::Location loc, mlir::OpBuilder& builder) {
    MLIRContext* const context = builder.getContext();
    const Type nodeColumnType = nodes.getType();
    const Type labelSetType = ColumnType::get(context, storage::LabelSetIDType::get(context));
    const Type boolType = ColumnType::get(context, storage::BoolType::get(context));

    GetNodeLabelSet labelSet = builder.create<GetNodeLabelSet>(loc, labelSetType, nodes);
    CheckLabelConstraint check = builder.create<CheckLabelConstraint>(loc, boolType, labelSet.getResult(), labels);

    const llvm::SmallVector<Type> resultTypes {nodeColumnType};
    const llvm::SmallVector<Value> columns {nodes};
    FilterOp labelFilter = builder.create<FilterOp>(loc, resultTypes, check.getResult(), columns);

    return labelFilter.getResult(0);
}

void replaceFilterWithSource(FilterOp filter, Value fused, Operation* source, const MaskCone& cone) {
    for (Value filtered : filter.getFilteredColumns()) {
        filtered.replaceAllUsesWith(fused);
    }
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
    eraseIfUnused(source);
}

bool collectNodeIDDisjunction(Value mask, Value scanColumn, llvm::SmallVectorImpl<int64_t>& nodeIDs) {
    if (OrOp disjunction = mask.getDefiningOp<OrOp>()) {
        return collectNodeIDDisjunction(disjunction.getLhs(), scanColumn, nodeIDs)
            && collectNodeIDDisjunction(disjunction.getRhs(), scanColumn, nodeIDs);
    }

    EqOp equality = mask.getDefiningOp<EqOp>();
    if (!equality) {
        return false;
    }

    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    Value constantSide;
    if (lhs == scanColumn) {
        constantSide = rhs;
    } else if (rhs == scanColumn) {
        constantSide = lhs;
    } else {
        return false;
    }

    ConstantOp constant = constantSide.getDefiningOp<ConstantOp>();
    if (!constant) {
        return false;
    }

    const IntegerAttr literal = dyn_cast<IntegerAttr>(constant.getValue());
    if (!literal || !literal.getType().isSignlessInteger(64)) {
        return false;
    }

    const int64_t nodeID = literal.getInt();
    if (nodeID < 0) {
        return false;
    }

    nodeIDs.push_back(nodeID);
    return true;
}

// The unwind factor of a cross product whose elements reach nothing but one equality
// against a column of the other factor, and the ops that equality is rebuilt from.
struct UnwindEqualityCross {
    CrossProduct _product {nullptr};
    UnwindConst _unwind {nullptr};
    Region* _relationFactor {nullptr};
    bool _unwindOnTheLeft {false};
    Value _unwoundColumn;
    Value _comparedColumn;
    EqOp _equality {nullptr};
    FilterOp _filter {nullptr};
};

// The constant unwind a factor is nothing but, null for any other factor. Anything else
// in the region would be dropped along with it, so the unwind has to be all there is
// besides the yield of its one column.
UnwindConst matchUnwindFactor(Region& factor) {
    Block& block = factor.front();
    if (block.getOperations().size() != 2) {
        return nullptr;
    }

    Yield yield = dyn_cast<Yield>(block.getTerminator());
    if (!yield || yield.getColumns().size() != 1) {
        return nullptr;
    }

    return yield.getColumns().front().getDefiningOp<UnwindConst>();
}

// Whether one comparison per element stands in for what the unwound column is compared to.
// A heterogeneous list rides the type-erased column whose cells are compared by the tag
// each carries, which no set of typed comparisons reproduces, and a repeated element emits
// the rows it matches once per copy, which a membership test does not express.
bool elementsCompareDistinctly(UnwindConst unwind) {
    const ColumnType column = cast<ColumnType>(unwind.getResult().getType());
    if (isa<storage::ListElementType>(column.getType())) {
        return false;
    }

    llvm::SmallDenseSet<Attribute, 8> seen;
    for (const Attribute element : unwind.getElements()) {
        if (!isa<IntegerAttr, FloatAttr, StringAttr>(element)) {
            return false;
        }

        if (!seen.insert(element).second) {
            return false;
        }
    }

    return true;
}

// Whether the rows can go on reading the elements through the column they were compared
// to. On every row the filter keeps the two hold the same value, so a property column
// stands in for the elements it matched; an entity column does not, since a node equals a
// node ID by the number it carries and a projection would print the node instead.
bool standsInForTheElements(Value comparedColumn) {
    const ColumnType column = cast<ColumnType>(comparedColumn.getType());

    return !isa<storage::NodeIDType, storage::EdgeIDType>(column.getType());
}

// The one equality among the users of @param unwoundColumn, provided nothing else reads it
// but the filter that equality masks - so dropping the unwind leaves no other reader behind.
EqOp matchSoleEquality(Value unwoundColumn, FilterOp& filter) {
    EqOp equality;
    for (Operation* const user : unwoundColumn.getUsers()) {
        const EqOp candidate = dyn_cast<EqOp>(user);
        if (!candidate) {
            continue;
        }

        if (equality) {
            return nullptr;
        }

        equality = candidate;
    }

    if (!equality || !equality.getResult().hasOneUse()) {
        return nullptr;
    }

    filter = dyn_cast<FilterOp>(*equality.getResult().getUsers().begin());
    if (!filter || filter.getMask() != equality.getResult()) {
        return nullptr;
    }

    for (Operation* const user : unwoundColumn.getUsers()) {
        const bool testsTheColumn = user == equality.getOperation();
        const bool carriesTheColumn = user == filter.getOperation();

        if (!testsTheColumn && !carriesTheColumn) {
            return nullptr;
        }
    }

    return equality;
}

bool matchUnwindEqualityCross(CrossProduct product, UnwindEqualityCross& match) {
    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();

    const UnwindConst leftUnwind = matchUnwindFactor(leftFactor);
    const UnwindConst rightUnwind = matchUnwindFactor(rightFactor);

    // Two unwind factors have no relation between them to fold either into.
    if (static_cast<bool>(leftUnwind) == static_cast<bool>(rightUnwind)) {
        return false;
    }

    match._product = product;
    match._unwind = leftUnwind ? leftUnwind : rightUnwind;
    match._relationFactor = leftUnwind ? &rightFactor : &leftFactor;
    match._unwindOnTheLeft = static_cast<bool>(leftUnwind);

    if (!elementsCompareDistinctly(match._unwind)) {
        return false;
    }

    // The results are the left factor's yielded columns followed by the right factor's, so
    // an unwind on the left contributes the first and one on the right the last.
    const size_t unwoundIndex = match._unwindOnTheLeft ? 0 : product.getResults().size() - 1;
    match._unwoundColumn = product.getResult(unwoundIndex);

    match._equality = matchSoleEquality(match._unwoundColumn, match._filter);
    if (!match._equality) {
        return false;
    }

    const Value lhs = match._equality.getLhs();
    match._comparedColumn = lhs == match._unwoundColumn ? match._equality.getRhs() : lhs;

    // The compared column has to be one the rows still carry once the product is gone:
    // a column the relation yields, or a value computed from one. A column defined inside
    // a factor is dropped with it.
    Operation* const comparedDef = match._comparedColumn.getDefiningOp();
    const bool survivesTheProduct = comparedDef
                                    && comparedDef->getParentRegion() == product->getParentRegion();

    if (!survivesTheProduct) {
        return false;
    }

    // Codegen carries every column in flight, so the filter holding the elements does not
    // mean anything reads them: an unused result is dropped rather than stood in for, and
    // only a column something does read has to be one the compared column can replace.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();

    size_t survivingColumns = 0;
    for (size_t index = 0; index < carried.size(); index++) {
        const bool holdsTheElements = carried[index] == match._unwoundColumn;

        if (holdsTheElements && filtered[index].use_empty()) {
            continue;
        }

        if (holdsTheElements && !standsInForTheElements(match._comparedColumn)) {
            return false;
        }

        survivingColumns++;
    }

    // A filter of nothing cuts nothing, so the elements were all it carried and the rows
    // it kept are read from the relation directly.
    if (survivingColumns == 0) {
        return false;
    }

    return true;
}

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
        ConstantOp constant = builder.create<ConstantOp>(loc, element);
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
    const Value lhs = match._equality.getLhs();
    match._comparedColumn = lhs == match._unwoundColumn ? match._equality.getRhs() : lhs;

    const Value mask = buildElementDisjunction(match, builder);

    // The elements the rows carried are gone. On every row the filter keeps they were the
    // compared value, so that column takes their place where something still reads them,
    // and the slot goes away where nothing does.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();

    llvm::SmallVector<Value> columns;
    llvm::SmallVector<Type> resultTypes;
    llvm::SmallVector<size_t> keptColumns;
    for (size_t index = 0; index < carried.size(); index++) {
        const bool holdsTheElements = carried[index] == match._unwoundColumn;
        if (holdsTheElements && filtered[index].use_empty()) {
            continue;
        }

        const Value kept = holdsTheElements ? match._comparedColumn : carried[index];

        columns.push_back(kept);
        resultTypes.push_back(kept.getType());
        keptColumns.push_back(index);
    }

    builder.setInsertionPoint(match._filter);
    FilterOp fused = builder.create<FilterOp>(match._filter.getLoc(), resultTypes, mask, columns);

    for (size_t index = 0; index < keptColumns.size(); index++) {
        filtered[keptColumns[index]].replaceAllUsesWith(fused.getResult(index));
    }

    match._filter.erase();
    match._equality.erase();
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

struct NodeIDScanChain {
    ScanSource _source;
    llvm::SmallVector<int64_t> _nodeIDs;
};

bool matchNodeIDScanChain(FilterOp filter, NodeIDScanChain& chain) {
    if (!matchSoleScanSource(filter, chain._source)) {
        return false;
    }

    if (!collectNodeIDDisjunction(filter.getMask(), chain._source._column, chain._nodeIDs)) {
        return false;
    }

    // A scan yields each node once in ID order, so the filter did too.
    llvm::sort(chain._nodeIDs);
    chain._nodeIDs.erase(std::unique(chain._nodeIDs.begin(), chain._nodeIDs.end()), chain._nodeIDs.end());

    return true;
}

void fuseScanByNodeIDs(FilterOp filter, const NodeIDScanChain& chain, mlir::OpBuilder& builder) {
    const mlir::Location loc = filter.getLoc();
    const Type nodeColumnType = chain._source._column.getType();

    builder.setInsertionPoint(filter);
    ConstScanNodes constScan = builder.create<ConstScanNodes>(loc, nodeColumnType, chain._nodeIDs);

    Value fused = constScan.getResult();
    if (chain._source._labels) {
        fused = filterByLabels(fused, chain._source._labels, loc, builder);
    }

    replaceFilterWithSource(filter, fused, chain._source._op, collectMaskCone(filter.getMask()));
}

struct FuseScanByNodeIDs : public impl::FuseScanByNodeIDsBase<FuseScanByNodeIDs> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<NodeIDScanChain>(getOperation(), matchNodeIDScanChain, fuseScanByNodeIDs, builder);
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

// A directed hop whose rows are then cut down to one edge type: the by-type hop spelled the
// long way, since the walk itself can keep the edges of that type and never build the rows
// the filter goes on to drop.
struct TypedHop {
    Operation* _hop {nullptr};
    CheckEdgeTypeConstraint _check;
    StringAttr _edgeType;
};

bool matchTypedHop(FilterOp filter, TypedHop& typedHop) {
    CheckEdgeTypeConstraint check = filter.getMask().getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check) {
        return false;
    }

    // An edge carries exactly one type, so a single required type is an equality a hop can
    // walk; several of them are a set match no one hop expresses.
    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.size() != 1) {
        return false;
    }

    StringAttr edgeType = dyn_cast<StringAttr>(edgeTypes[0]);
    if (!edgeType) {
        return false;
    }

    const Value edgeTypeIds = check.getEdgeTypeIds();
    Operation* const hop = edgeTypeIds.getDefiningOp();
    if (!hop || !isa<GetOutEdges, GetInEdges>(hop)) {
        return false;
    }

    constexpr size_t etypesResultIndex = 2;
    if (edgeTypeIds != hop->getResult(etypesResultIndex)) {
        return false;
    }

    // Every column the filter cuts has to be one the hop bound, or the fused hop has nothing
    // of its own to hand back in its place.
    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != hop) {
            return false;
        }
    }

    // And nothing outside the pair may read the hop, or that reader would go on seeing the
    // rows the type turns away.
    for (const Value result : hop->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsThePair = user == filter.getOperation() || user == check.getOperation();
            if (!readsThePair) {
                return false;
            }
        }
    }

    typedHop = TypedHop {._hop = hop, ._check = check, ._edgeType = edgeType};

    return true;
}

template <typename ByTypeOp>
Operation* createByTypeHop(Operation* hop, StringAttr edgeType, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();

    ByTypeOp byTypeHop = builder.create<ByTypeOp>(hop->getLoc(),
                                                  results[0].getType(),
                                                  results[1].getType(),
                                                  results[2].getType(),
                                                  results[3].getType(),
                                                  results.drop_front(hopFixedResultCount).getTypes(),
                                                  hop->getOperand(0),
                                                  edgeType,
                                                  hop->getOperands().drop_front());

    return byTypeHop.getOperation();
}

void fuseEdgesByType(FilterOp filter, const TypedHop& typedHop, mlir::OpBuilder& builder) {
    Operation* const hop = typedHop._hop;

    builder.setInsertionPoint(hop);

    // A by-type hop declares the same four fixed results and the same carry set behind them,
    // so the plain hop's results map onto it one for one.
    Operation* const byTypeHop = isa<GetOutEdges>(hop)
                                     ? createByTypeHop<GetOutEdgesByType>(hop, typedHop._edgeType, builder)
                                     : createByTypeHop<GetInEdgesByType>(hop, typedHop._edgeType, builder);

    hop->replaceAllUsesWith(byTypeHop);

    // The hop now yields the rows the filter used to leave, so each column the filter handed
    // on is the one it was given.
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(typedHop._check);
    hop->erase();
}

struct FuseEdgesByType : public impl::FuseEdgesByTypeBase<FuseEdgesByType> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first: fusing erases the filter, its check and its hop, which would
        // invalidate the walk.
        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            TypedHop typedHop;
            if (matchTypedHop(filter, typedHop)) {
                filters.push_back(filter);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            TypedHop typedHop;
            if (!matchTypedHop(filter, typedHop)) {
                continue;
            }

            fuseEdgesByType(filter, typedHop, builder);
        }
    }
};

// An edge scan whose rows are then cut down to one edge type: the by-type scan spelled the
// long way, since the scan itself can keep the edges of that type and never build the rows
// the filter goes on to drop.
struct TypedEdgeScan {
    ScanEdges _scan;
    CheckEdgeTypeConstraint _check;
    StringAttr _edgeType;
};

bool matchTypedEdgeScan(FilterOp filter, TypedEdgeScan& typedScan) {
    CheckEdgeTypeConstraint check = filter.getMask().getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.size() != 1) {
        return false;
    }

    StringAttr edgeType = dyn_cast<StringAttr>(edgeTypes[0]);
    if (!edgeType) {
        return false;
    }

    const Value edgeTypeIds = check.getEdgeTypeIds();
    ScanEdges scan = edgeTypeIds.getDefiningOp<ScanEdges>();
    if (!scan) {
        return false;
    }

    if (edgeTypeIds != scan.getEtypes()) {
        return false;
    }

    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != scan.getOperation()) {
            return false;
        }
    }

    Operation* const filterOp = filter.getOperation();
    Operation* const checkOp = check.getOperation();
    for (const Value result : scan->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsThePair = user == filterOp || user == checkOp;
            if (!readsThePair) {
                return false;
            }
        }
    }

    typedScan = TypedEdgeScan {._scan = scan, ._check = check, ._edgeType = edgeType};

    return true;
}

void fuseScanEdgesByType(FilterOp filter, const TypedEdgeScan& typedScan, mlir::OpBuilder& builder) {
    ScanEdges scan = typedScan._scan;
    Operation* const scanOp = scan.getOperation();

    builder.setInsertionPoint(scanOp);

    // A by-type scan declares the same four results in the same order, so the plain scan's
    // map onto it one for one.
    ScanEdgesByType byTypeScan = builder.create<ScanEdgesByType>(scan.getLoc(),
                                                                 scan.getSrcids().getType(),
                                                                 scan.getEids().getType(),
                                                                 scan.getEtypes().getType(),
                                                                 scan.getTgtids().getType(),
                                                                 typedScan._edgeType);

    scanOp->replaceAllUsesWith(byTypeScan.getOperation());

    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(typedScan._check);
    scanOp->erase();
}

struct FuseScanEdgesByType : public impl::FuseScanEdgesByTypeBase<FuseScanEdgesByType> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first: fusing erases the filter, its check and its scan, which would
        // invalidate the walk.
        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            TypedEdgeScan typedScan;
            if (matchTypedEdgeScan(filter, typedScan)) {
                filters.push_back(filter);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            TypedEdgeScan typedScan;
            if (!matchTypedEdgeScan(filter, typedScan)) {
                continue;
            }

            fuseScanEdgesByType(filter, typedScan, builder);
        }
    }
};

// Where an op's carry set sits: the carried operands start at _operandOffset and each comes
// back as the result at the same position from _resultOffset.
struct CarrySetLayout {
    size_t _operandOffset {0};
    size_t _resultOffset {0};
};

bool matchCarrySetLayout(Operation* op, CarrySetLayout& layout) {
    if (isEdgeHop(op)) {
        layout = CarrySetLayout {._operandOffset = 1, ._resultOffset = hopFixedResultCount};
        return true;
    } else if (isa<FilterOp>(op)) {
        layout = CarrySetLayout {._operandOffset = 1, ._resultOffset = 0};
        return true;
    } else if (isa<Unwind>(op)) {
        layout = CarrySetLayout {._operandOffset = 1, ._resultOffset = 1};
        return true;
    } else if (isa<Limit, Skip, Sort, GroupAggregate, Collect>(op)) {
        layout = CarrySetLayout {._operandOffset = 0, ._resultOffset = 0};
        return true;
    } else if (CallProcedure call = dyn_cast<CallProcedure>(op)) {
        const size_t inputCount = call.getInputs().size();
        const size_t yieldCount = call.getYields().size();
        layout = CarrySetLayout {._operandOffset = inputCount, ._resultOffset = yieldCount};
        return true;
    }

    return false;
}

bool trimsColumns(Operation* op) {
    CarrySetLayout layout;

    return matchCarrySetLayout(op, layout) || isa<CrossProduct, HashJoin>(op);
}

size_t carriedCount(Operation* op, const CarrySetLayout& layout) {
    return op->getNumOperands() - layout._operandOffset;
}

size_t collectValueCount(Collect collect) {
    const size_t aggregateCount = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {}).size();

    return collect.getColumns().size() - collect.getKeyCount() - aggregateCount;
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

void selectKeptCarriedColumns(Operation* op, const CarrySetLayout& layout, llvm::SmallVectorImpl<size_t>& kept) {
    const size_t count = carriedCount(op, layout);

    llvm::SmallBitVector keep(count);
    for (size_t carriedIndex = 0; carriedIndex < count; carriedIndex++) {
        if (!op->getResult(layout._resultOffset + carriedIndex).use_empty()) {
            keep.set(carriedIndex);
        }
    }

    keepRequiredColumns(op, keep);

    const bool standsForItsRows = layout._resultOffset == 0;
    if (standsForItsRows) {
        keepRowCarryingColumn(op->getOperands().drop_front(layout._operandOffset), 0, keep);
    }

    for (size_t carriedIndex = 0; carriedIndex < count; carriedIndex++) {
        if (keep[carriedIndex]) {
            kept.push_back(carriedIndex);
        }
    }
}

void setOrEraseIndices(OperationState& state, StringAttr name, llvm::ArrayRef<int64_t> indices, mlir::OpBuilder& builder) {
    if (indices.empty()) {
        state.attributes.erase(name);
    } else {
        state.attributes.set(name, builder.getDenseI64ArrayAttr(indices));
    }
}

void renumberSortKeys(Sort sort, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    llvm::SmallVector<int64_t> keyColumns;
    for (const int64_t keyColumn : sort.getKeyColumns()) {
        const auto keptIt = llvm::find(kept, static_cast<size_t>(keyColumn));
        bioassert(keptIt != kept.end(), "Sort key column {} is not in the trimmed carry set", keyColumn);

        keyColumns.push_back(static_cast<int64_t>(keptIt - kept.begin()));
    }

    state.attributes.set(sort.getKeyColumnsAttrName(), builder.getDenseI64ArrayAttr(keyColumns));
}

void trimGroupAggregateKinds(GroupAggregate groupAggregate, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    const size_t keyCount = groupAggregate.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = groupAggregate.getKinds();

    llvm::SmallVector<int64_t> keptKinds;
    for (const size_t columnIndex : kept) {
        if (columnIndex >= keyCount) {
            keptKinds.push_back(kinds[columnIndex - keyCount]);
        }
    }

    state.attributes.set(groupAggregate.getKindsAttrName(), builder.getDenseI64ArrayAttr(keptKinds));
}

void trimCollectAttributes(Collect collect, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    const size_t keyCount = collect.getKeyCount();
    const size_t valueEnd = keyCount + collectValueCount(collect);
    const llvm::ArrayRef<int64_t> kinds = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {});

    llvm::SmallVector<int64_t> keptKinds;
    llvm::SmallVector<int64_t> keptValues;
    for (const size_t columnIndex : kept) {
        if (columnIndex >= valueEnd) {
            keptKinds.push_back(kinds[columnIndex - valueEnd]);
        } else if (columnIndex >= keyCount) {
            keptValues.push_back(static_cast<int64_t>(columnIndex - keyCount));
        }
    }

    llvm::SmallVector<int64_t> distinctValues;
    for (const int64_t valueIndex : collect.getDistinctValues().value_or(llvm::ArrayRef<int64_t> {})) {
        const auto keptIt = llvm::find(keptValues, valueIndex);
        if (keptIt != keptValues.end()) {
            distinctValues.push_back(static_cast<int64_t>(keptIt - keptValues.begin()));
        }
    }

    setOrEraseIndices(state, collect.getKindsAttrName(), keptKinds, builder);
    setOrEraseIndices(state, collect.getDistinctValuesAttrName(), distinctValues, builder);
}

void trimAttributes(Operation* op, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    if (Sort sort = dyn_cast<Sort>(op)) {
        renumberSortKeys(sort, kept, state, builder);
    } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
        trimGroupAggregateKinds(groupAggregate, kept, state, builder);
    } else if (Collect collect = dyn_cast<Collect>(op)) {
        trimCollectAttributes(collect, kept, state, builder);
    } else if (CallProcedure call = dyn_cast<CallProcedure>(op)) {
        const int32_t inputCount = static_cast<int32_t>(call.getInputs().size());
        const int32_t keptCount = static_cast<int32_t>(kept.size());
        state.attributes.set(call.getOperandSegmentSizesAttrName(), builder.getDenseI32ArrayAttr({inputCount, keptCount}));
    }
}

void trimCarrySet(Operation* op, const CarrySetLayout& layout, llvm::ArrayRef<size_t> kept, mlir::OpBuilder& builder) {
    const Operation::operand_range operands = op->getOperands();
    const Operation::result_range results = op->getResults();

    llvm::SmallVector<Value> trimmedOperands;
    llvm::append_range(trimmedOperands, operands.take_front(layout._operandOffset));

    llvm::SmallVector<Type> trimmedTypes;
    llvm::append_range(trimmedTypes, results.take_front(layout._resultOffset).getTypes());

    for (const size_t carriedIndex : kept) {
        trimmedOperands.push_back(operands[layout._operandOffset + carriedIndex]);
        trimmedTypes.push_back(results[layout._resultOffset + carriedIndex].getType());
    }

    OperationState state(op->getLoc(), op->getName());
    state.addOperands(trimmedOperands);
    state.addTypes(trimmedTypes);
    state.addAttributes(op->getAttrs());
    trimAttributes(op, kept, state, builder);

    builder.setInsertionPoint(op);
    Operation* const trimmed = builder.create(state);

    for (size_t resultIndex = 0; resultIndex < layout._resultOffset; resultIndex++) {
        results[resultIndex].replaceAllUsesWith(trimmed->getResult(resultIndex));
    }

    for (size_t keptIndex = 0; keptIndex < kept.size(); keptIndex++) {
        Value carried = results[layout._resultOffset + kept[keptIndex]];
        carried.replaceAllUsesWith(trimmed->getResult(layout._resultOffset + keptIndex));
    }

    op->erase();
}

void eraseUnkeptYields(Yield yield, const llvm::SmallBitVector& keep, size_t firstIndex) {
    llvm::BitVector erased(yield.getNumOperands());
    for (size_t columnIndex = 0; columnIndex < erased.size(); columnIndex++) {
        if (!keep[firstIndex + columnIndex]) {
            erased.set(columnIndex);
        }
    }

    yield->eraseOperands(erased);
}

// The db.yield ending a factor region of a two-factor op - db.cross_product, db.hash_join.
Yield factorYield(Operation* op, unsigned factorIndex) {
    return cast<Yield>(op->getRegion(factorIndex).front().getTerminator());
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

void keptResultTypes(Operation* op, const llvm::SmallBitVector& keep, llvm::SmallVectorImpl<Type>& types) {
    const Operation::result_range results = op->getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (keep[resultIndex]) {
            types.push_back(results[resultIndex].getType());
        }
    }
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

// Hands the two factors to the op built in its place, drops the columns their yields no
// longer name, and rewires every kept result. Shared by the cross product and the hash
// join, which differ only in the op the caller built.
void replaceWithTrimmedFactors(Operation* op,
                               Operation* trimmed,
                               const llvm::SmallBitVector& keep,
                               size_t leftCount) {
    Yield leftYield = factorYield(op, 0);
    Yield rightYield = factorYield(op, 1);

    trimmed->getRegion(0).takeBody(op->getRegion(0));
    trimmed->getRegion(1).takeBody(op->getRegion(1));

    eraseUnkeptYields(leftYield, keep, 0);
    eraseUnkeptYields(rightYield, keep, leftCount);

    const Operation::result_range results = op->getResults();
    size_t trimmedIndex = 0;
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (keep[resultIndex]) {
            results[resultIndex].replaceAllUsesWith(trimmed->getResult(trimmedIndex++));
        }
    }

    op->erase();
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
}

struct TrimUnreadColumns : public impl::TrimUnreadColumnsBase<TrimUnreadColumns> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        llvm::SmallVector<Operation*> carriers;
        root->walk([&](Operation* op) {
            if (trimsColumns(op)) {
                carriers.push_back(op);
            }
        });

        // The walk lists a producer before its readers, so sweeping it backwards trims a
        // reader before the op feeding it and a chain of hops settles in one pass.
        mlir::OpBuilder builder(&getContext());
        for (Operation* const op : llvm::reverse(carriers)) {
            if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
                trimCrossProduct(product, builder);
                continue;
            } else if (HashJoin join = dyn_cast<HashJoin>(op)) {
                trimHashJoin(join, builder);
                continue;
            }

            CarrySetLayout layout;
            const bool carries = matchCarrySetLayout(op, layout);
            bioassert(carries, "A trimming op that is neither a cross product nor a join has a carry set");

            llvm::SmallVector<size_t> kept;
            selectKeptCarriedColumns(op, layout, kept);

            if (kept.size() == carriedCount(op, layout)) {
                continue;
            }

            trimCarrySet(op, layout, kept, builder);
        }
    }
};

// A property equality fuses into the scan and every other conjunct of the mask is rebuilt
// over the fused rows, so the whole cone is cloned or dropped: it must read nothing but
// the scanned column, and nothing outside it may read a part of it.
struct PropertyValueScanChain {
    ScanSource _source;
    MaskCone _cone;
    StringAttr _property;
    TypedAttr _value;
    llvm::SmallVector<Value> _residual;
};

// eq(get_node_properties(scan, property), constant), with the constant on either side
bool matchPropertyEquality(EqOp equality, Value scanColumn, PropertyValueScanChain& chain) {
    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    GetNodeProperties read = lhs.getDefiningOp<GetNodeProperties>();
    Value constantSide = rhs;
    if (!read) {
        read = rhs.getDefiningOp<GetNodeProperties>();
        constantSide = lhs;
    }

    if (!read || read.getInputNodes() != scanColumn) {
        return false;
    }

    ConstantOp constant = constantSide.getDefiningOp<ConstantOp>();
    if (!constant) {
        return false;
    }

    const TypedAttr literal = dyn_cast<TypedAttr>(constant.getValue());
    if (!literal || !storage::isPropertyScanLiteral(literal)) {
        return false;
    }

    chain._property = read.getPropertyAttr();
    chain._value = literal;
    return true;
}

// The conjuncts of a mask, as an `and` tree spells them: the predicates that all have to
// hold, one of which can become the fused scan while the others stay a filter.
void collectConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts) {
    if (AndOp conjunction = mask.getDefiningOp<AndOp>()) {
        collectConjuncts(conjunction.getLhs(), conjuncts);
        collectConjuncts(conjunction.getRhs(), conjuncts);
        return;
    }

    conjuncts.push_back(mask);
}

bool maskConeIsPrivateTo(const MaskCone& cone, FilterOp filter) {
    llvm::SmallPtrSet<Operation*, 8> coneOps;
    for (Operation* const coneOp : cone._ops) {
        coneOps.insert(coneOp);
    }

    Operation* const filterOp = filter.getOperation();
    for (Operation* const coneOp : cone._ops) {
        for (Operation* const user : coneOp->getUsers()) {
            if (user != filterOp && !coneOps.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

bool matchPropertyValueScanChain(FilterOp filter, PropertyValueScanChain& chain) {
    if (!matchSoleScanSource(filter, chain._source)) {
        return false;
    }

    chain._cone = collectMaskCone(filter.getMask());

    const MaskCone& cone = chain._cone;
    const bool readsTheScannedColumnAlone = cone._inputs.size() == 1 && cone._inputs.front() == chain._source._column;
    if (!readsTheScannedColumnAlone || !maskConeIsPrivateTo(cone, filter)) {
        return false;
    }

    llvm::SmallVector<Value> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    // The first property equality in the mask wins, not the most selective one: there are
    // no column statistics to choose with, so `n.gender = 'M' AND n.ssn = '...'` fuses the
    // unselective conjunct and leaves the selective one a residual filter.
    for (size_t candidate = 0; candidate < conjuncts.size(); candidate++) {
        EqOp equality = conjuncts[candidate].getDefiningOp<EqOp>();
        if (!equality || !matchPropertyEquality(equality, chain._source._column, chain)) {
            continue;
        }

        for (size_t other = 0; other < conjuncts.size(); other++) {
            if (other == candidate) {
                continue;
            }

            // Only a conjunct the cone built can be rebuilt over the fused rows.
            Operation* const conjunctDef = conjuncts[other].getDefiningOp();
            if (!conjunctDef || !isMaskComputeOp(conjunctDef)) {
                return false;
            }

            chain._residual.push_back(conjuncts[other]);
        }

        return true;
    }

    return false;
}

// Rebuilds the conjuncts the fused scan does not carry over its rows, as the mask of the
// filter that stays. The clone reads the fused column wherever the original read the scan.
Value cloneResidualMask(llvm::ArrayRef<Value> residual,
                        Value scanColumn,
                        Value fused,
                        Type maskType,
                        mlir::Location loc,
                        mlir::OpBuilder& builder) {
    MaskCone cone;
    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value conjunct : residual) {
        collectConePostOrder(conjunct, visited, cone);
    }

    mlir::IRMapping mapping;
    mapping.map(scanColumn, fused);
    for (Operation* const coneOp : cone._ops) {
        builder.clone(*coneOp, mapping);
    }

    Value mask = mapping.lookup(residual.front());
    for (const Value conjunct : residual.drop_front()) {
        AndOp conjunction = builder.create<AndOp>(loc, maskType, mask, mapping.lookup(conjunct));
        mask = conjunction.getResult();
    }

    return mask;
}

void fuseScanByPropertyValue(FilterOp filter, const PropertyValueScanChain& chain, mlir::OpBuilder& builder) {
    const mlir::Location loc = filter.getLoc();
    const Type nodeColumnType = chain._source._column.getType();

    builder.setInsertionPoint(filter);
    ScanNodesByPropertyValue scan = builder.create<ScanNodesByPropertyValue>(loc,
                                                                             nodeColumnType,
                                                                             chain._property,
                                                                             chain._value,
                                                                             chain._source._labels);

    Value fused = scan.getResult();
    if (!chain._residual.empty()) {
        const Value mask = cloneResidualMask(chain._residual,
                                             chain._source._column,
                                             fused,
                                             filter.getMask().getType(),
                                             loc,
                                             builder);

        const llvm::SmallVector<Type> resultTypes {nodeColumnType};
        const llvm::SmallVector<Value> columns {fused};
        FilterOp residualFilter = builder.create<FilterOp>(loc, resultTypes, mask, columns);
        fused = residualFilter.getResult(0);
    }

    replaceFilterWithSource(filter, fused, chain._source._op, chain._cone);
}

struct FuseScanByPropertyValue : public impl::FuseScanByPropertyValueBase<FuseScanByPropertyValue> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<PropertyValueScanChain>(getOperation(),
                                              matchPropertyValueScanChain,
                                              fuseScanByPropertyValue,
                                              builder);
    }
};

// The ops whose results hold the rows of their operands selected, reordered or repeated as
// a whole, values untouched, so a property read of an operand and carried through is the
// column a read of the result would have produced. group_aggregate and collect are not
// among them - a group's rows are not its input's - and neither is remove_duplicates,
// whose dedup key is every column it carries, so a wider carry set drops different rows.
bool mapsRowsThrough(Operation* op) {
    return isEdgeHop(op) || isa<FilterOp, Unwind, Limit, Skip, Sort>(op);
}

// The operand whose rows a result's rows are drawn from, if any: each carried column comes
// back at its own position, and a hop's input re-surfaces as srcids, or tgtids when it
// walks backwards.
bool matchRowSourceOperand(Operation* op, const CarrySetLayout& layout, size_t resultIndex, size_t& operandIndex) {
    if (resultIndex >= layout._resultOffset) {
        operandIndex = layout._operandOffset + (resultIndex - layout._resultOffset);
        return true;
    } else if (isEdgeHop(op)) {
        constexpr size_t srcResultIndex = 0;
        constexpr size_t tgtResultIndex = 3;
        const size_t inputResultIndex = isReverseHop(op) ? tgtResultIndex : srcResultIndex;

        if (resultIndex == inputResultIndex) {
            operandIndex = 0;
            return true;
        }
    }

    return false;
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

// Widens an op's carry set by one column and hands back the result it comes out as. A carry
// set is the trailing operands and the trailing results of the op holding it, so the column
// appends to both; an op's arity is fixed once built, hence the rebuild.
Value appendCarriedColumn(Operation* op, Value column, mlir::OpBuilder& builder) {
    llvm::SmallVector<Value> operands;
    llvm::append_range(operands, op->getOperands());
    operands.push_back(column);

    llvm::SmallVector<Type> types;
    llvm::append_range(types, op->getResultTypes());
    types.push_back(column.getType());

    OperationState state(op->getLoc(), op->getName());
    state.addOperands(operands);
    state.addTypes(types);
    state.addAttributes(op->getAttrs());

    builder.setInsertionPoint(op);
    Operation* const widened = builder.create(state);

    op->replaceAllUsesWith(widened->getResults().drop_back());
    op->erase();

    return widened->getResults().back();
}

// The column holding `property` for the rows of `column`, taken from a read already
// standing before `useSite` and carried down through the ops in between when that read was
// taken further up the chain. Null when there is none: this adds no read of its own.
Value propertyColumnOf(Value column, StringAttr property, bool nodeProperty, Operation* useSite, mlir::OpBuilder& builder) {
    for (Operation* const user : column.getUsers()) {
        StringAttr userProperty;
        bool userReadsNodes = false;
        if (user == useSite || !matchPropertyRead(user, userProperty, userReadsNodes)) {
            continue;
        }

        const bool readsTheSameProperty = userReadsNodes == nodeProperty && userProperty == property;
        const bool standsBeforeTheUse = user->getBlock() == useSite->getBlock() && user->isBeforeInBlock(useSite);

        if (readsTheSameProperty && standsBeforeTheUse) {
            return user->getResult(0);
        }
    }

    Operation* const def = column.getDefiningOp();
    if (!def || !mapsRowsThrough(def)) {
        return {};
    }

    CarrySetLayout layout;
    const bool carries = matchCarrySetLayout(def, layout);
    bioassert(carries, "A row-mapping op has a carry set");

    size_t sourceOperandIndex = 0;
    const size_t resultIndex = cast<OpResult>(column).getResultNumber();
    if (!matchRowSourceOperand(def, layout, resultIndex, sourceOperandIndex)) {
        return {};
    }

    const Value source = def->getOperand(sourceOperandIndex);
    const Value sourceProperty = propertyColumnOf(source, property, nodeProperty, def, builder);
    if (!sourceProperty) {
        return {};
    }

    const size_t carriedColumnCount = carriedCount(def, layout);
    for (size_t carriedIndex = 0; carriedIndex < carriedColumnCount; carriedIndex++) {
        if (def->getOperand(layout._operandOffset + carriedIndex) == sourceProperty) {
            return def->getResult(layout._resultOffset + carriedIndex);
        }
    }

    return appendCarriedColumn(def, sourceProperty, builder);
}

struct ReusePropertyReads : public impl::ReusePropertyReadsBase<ReusePropertyReads> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        llvm::SmallVector<Operation*> reads;
        llvm::DenseSet<Attribute> writtenNodeProperties;
        llvm::DenseSet<Attribute> writtenEdgeProperties;
        root->walk([&](Operation* op) {
            StringAttr writtenProperty;
            bool writesNodes = false;

            if (isa<GetNodeProperties, GetEdgeProperties>(op)) {
                reads.push_back(op);
            } else if (matchPropertyWrite(op, writtenProperty, writesNodes)) {
                if (writesNodes) {
                    writtenNodeProperties.insert(writtenProperty);
                } else {
                    writtenEdgeProperties.insert(writtenProperty);
                }
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (Operation* const read : reads) {
            StringAttr property;
            bool nodeProperty = false;
            const bool isPropertyRead = matchPropertyRead(read, property, nodeProperty);
            bioassert(isPropertyRead, "A collected op is a property read");

            // A read standing before this one saw what the property held before the write,
            // and a projection behind a SET reads what the statement wrote
            const llvm::DenseSet<Attribute>& written = nodeProperty ? writtenNodeProperties : writtenEdgeProperties;
            if (written.contains(property)) {
                continue;
            }

            const Value reused = propertyColumnOf(read->getOperand(0), property, nodeProperty, read, builder);
            if (!reused) {
                continue;
            }

            read->getResult(0).replaceAllUsesWith(reused);
            read->erase();
        }
    }
};

// One side of a join: the factor supplying its key, the product column the key is read
// from, the ops computing one from the other, and - once sunk - where the key sits in the
// factor's yield.
struct JoinKeySide {
    Region* _factor {nullptr};
    Value _key;
    Value _column;
    MaskCone _cone;
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

// The property a one-op key cone reads, null for any other cone. A property column's
// value type is resolved from the name during lowering, so two keys read from one name
// are two columns of one type.
StringAttr conePropertyName(const MaskCone& cone) {
    if (cone._ops.size() != 1) {
        return nullptr;
    }

    Operation* const read = cone._ops.front();
    if (GetNodeProperties nodeRead = dyn_cast<GetNodeProperties>(read)) {
        return nodeRead.getPropertyAttr();
    } else if (GetEdgeProperties edgeRead = dyn_cast<GetEdgeProperties>(read)) {
        return edgeRead.getPropertyAttr();
    }

    return nullptr;
}

// Whether the two sides' keys are provably columns of one type. The join matches a build
// key against a probe key by the bytes each serializes to, so two columns that could
// serialize differently - an integer property against a string one, a node against a
// number - would answer a comparison the equality did not. A db column type is concrete
// only for the entity and metadata columns; a column typed none carries no type at all
// until lowering resolves it, so two of those are only provably alike when both are read
// from one property name.
bool keysShareAColumnType(const JoinKeySide& left, const JoinKeySide& right) {
    if (left._cone._ops.empty() && right._cone._ops.empty()) {
        const Type leftType = left._key.getType();
        const bool typeIsResolved = !isa<mlir::NoneType>(cast<ColumnType>(leftType).getType());

        return typeIsResolved && leftType == right._key.getType();
    }

    const StringAttr leftProperty = conePropertyName(left._cone);

    return leftProperty && leftProperty == conePropertyName(right._cone);
}

// Reads into side the one product column a key is computed over, along with the ops
// computing one from the other. False when the key reads no column, more than one, or one
// a different cross product made - product names the one already matched, null for the
// first side, and is set to the one this side found.
bool matchKeySide(Value key, JoinKeySide& side, CrossProduct& product) {
    side._key = key;
    side._cone = collectMaskCone(key);

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
// the factor and dropped from here, so a reader left behind would lose its operand.
bool keyConeIsPrivateTo(const MaskCone& cone, EqOp equality) {
    llvm::SmallPtrSet<Operation*, 8> coneOps;
    for (Operation* const coneOp : cone._ops) {
        coneOps.insert(coneOp);
    }

    Operation* const equalityOp = equality.getOperation();
    for (Operation* const coneOp : cone._ops) {
        for (Operation* const user : coneOp->getUsers()) {
            if (user != equalityOp && !coneOps.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

// Whether the product's rows reach nothing but the equality and the filter it masks. The
// join emits the rows the filter kept, so a reader of the product's own rows would go
// from seeing every pair to seeing only the matching ones.
bool productRowsReachOnly(CrossProduct product, const EqualityCross& match) {
    EqOp equality = match._equality;
    FilterOp filter = match._filter;

    llvm::SmallPtrSet<Operation*, 8> readers;
    readers.insert(equality.getOperation());
    readers.insert(filter.getOperation());

    const JoinKeySide* const sides[] = {&match._left, &match._right};
    for (const JoinKeySide* const side : sides) {
        for (Operation* const coneOp : side->_cone._ops) {
            readers.insert(coneOp);
        }
    }

    for (const Value column : product.getResults()) {
        for (Operation* const user : column.getUsers()) {
            if (!readers.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

// Whether every column the filter carries is a column of the product. The join hands each
// of them back as a result of its own, which it can only do for a column the product made:
// one computed from them outside was computed over the rows the join no longer produces.
bool carriesProductColumnsOnly(FilterOp filter, CrossProduct product) {
    for (const Value carried : filter.getColumnsToFilter()) {
        if (carried.getDefiningOp<CrossProduct>() != product) {
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
        if (isa<CrossProduct, HashJoin>(op)) {
            return WalkResult::interrupt();
        }

        return WalkResult::advance();
    });

    return walked.wasInterrupted();
}

// The label set a scan's label names stand for, resolved against the schema.
void collectScanLabels(ArrayAttr labelNames, const ::db::GraphMetadata& metadata, ::db::LabelSet& labels) {
    const ::db::LabelMap& labelMap = metadata.labels();
    for (const Attribute labelAttr : labelNames) {
        const llvm::StringRef name = cast<StringAttr>(labelAttr).getValue();
        const std::optional<::db::LabelID> label = labelMap.get(std::string_view(name.data(), name.size()));

        if (label) {
            labels.set(*label);
        }
    }
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

// The row budget a limit downstream of the cut puts on the rows the join would emit, and
// zero when none governs them. A product stops as soon as the budget is met, where the
// join reads its whole build side before it can emit a row, which is what tips a small
// budget back towards the product.
uint64_t limitOverTheCut(FilterOp filter) {
    llvm::SmallVector<Operation*, 8> pending;
    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value column : filter.getFilteredColumns()) {
        for (Operation* const user : column.getUsers()) {
            if (visited.insert(user).second) {
                pending.push_back(user);
            }
        }
    }

    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val();
        if (Limit limit = dyn_cast<Limit>(op)) {
            return limit.getCount();
        }

        for (const Value result : op->getResults()) {
            for (Operation* const user : result.getUsers()) {
                if (visited.insert(user).second) {
                    pending.push_back(user);
                }
            }
        }
    }

    return 0;
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

    return estimation.shouldPreferCartesian(leftLabels, rightLabels, limitOverTheCut(match._filter));
}

// What the db level puts on rows it cannot count - the figure the v2 planner reads a
// yielded item at.
constexpr size_t uncountableRows = 10;

// The rows an op seeds a factor with, and nothing at all when it seeds none - a fetch, a
// filter or a hop reads a column rather than making one. A listed set of IDs is its own
// count, a literal list one row per element, a by-label scan what the graph holds under
// those labels, and a property scan the whole node count: selectivity is not something
// the db level knows.
std::optional<size_t> estimateSourceRows(Operation* op,
                                         const ::db::GraphMetadata& metadata,
                                         const ::db::CardinalityEstimation& estimation) {
    if (ConstScanNodes constScan = dyn_cast<ConstScanNodes>(op)) {
        return constScan.getNodeIDs().size();
    } else if (UnwindConst unwind = dyn_cast<UnwindConst>(op)) {
        return unwind.getElements().size();
    } else if (ScanNodesByLabel byLabel = dyn_cast<ScanNodesByLabel>(op)) {
        ::db::LabelSet labels;
        collectScanLabels(byLabel.getLabels(), metadata, labels);

        return estimation.estimateNodeCount(labels);
    } else if (isa<ScanNodes, ScanNodesByPropertyValue>(op)) {
        return estimation.estimateNodeCount(::db::LabelSet {});
    } else if (isa<ScanEdges, ScanEdgesByType>(op)) {
        return estimation.estimateEdgeCount();
    }

    return std::nullopt;
}

// The ratio an op multiplies the rows it reads by, and nothing when it hands them on as
// they come. A hop makes the graph's average degree of rows per row it reads - all the db
// level can say of a hop out of a node it cannot name - and an undirected one walks both
// directions, so twice that. It is a ratio rather than a count because a graph with fewer
// edges than nodes has a hop shrink its input rather than grow it. An unwind makes a row
// per element of each row's list, whose length is a property of the rows themselves: no
// statistic of the graph measures it, so it reads at the figure an uncountable source
// does - where a literal list, whose elements are in the IR, is counted exactly instead.
struct RowMultiplier {
    size_t _numerator {1};
    size_t _denominator {1};
};

std::optional<RowMultiplier> estimateRowMultiplier(Operation* op, const ::db::CardinalityEstimation& estimation) {
    if (isa<Unwind>(op)) {
        return RowMultiplier {uncountableRows, 1};
    }

    const bool walksOneDirection = isa<GetOutEdges, GetInEdges, GetOutEdgesByType, GetInEdgesByType>(op);
    const bool walksBoth = isa<GetEdges>(op);
    if (!walksOneDirection && !walksBoth) {
        return std::nullopt;
    }

    const size_t nodeCount = estimation.estimateNodeCount(::db::LabelSet {});
    if (nodeCount == 0) {
        return std::nullopt;
    }

    const size_t edgeCount = estimation.estimateEdgeCount();

    return RowMultiplier {walksBoth ? 2 * edgeCount : edgeCount, nodeCount};
}

size_t multiplySaturating(size_t rows, size_t factor) {
    constexpr size_t rowCeiling = std::numeric_limits<size_t>::max();
    if (factor != 0 && rows > rowCeiling / factor) {
        return rowCeiling;
    }

    return rows * factor;
}

// The rows a factor makes: the product of what its sources seed it with, a factor crossing
// two of them holding a row per pair, grown or shrunk by every hop and unwind reading from
// one. A factor seeded from somewhere the db level cannot count - a procedure, a load -
// counts as an uncountable source's rows.
size_t estimateFactorRows(Region& factor,
                          const ::db::GraphMetadata& metadata,
                          const ::db::CardinalityEstimation& estimation) {
    bool countedASource = false;
    size_t rows = 1;

    factor.walk([&](Operation* op) {
        const std::optional<size_t> seeded = estimateSourceRows(op, metadata, estimation);
        if (seeded) {
            countedASource = true;
            rows = multiplySaturating(rows, *seeded);
            return;
        }

        const std::optional<RowMultiplier> multiplier = estimateRowMultiplier(op, estimation);
        if (multiplier) {
            rows = multiplySaturating(rows, multiplier->_numerator) / multiplier->_denominator;
        }
    });

    return countedASource ? rows : uncountableRows;
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

    const ::db::GraphView& view = *context._view;
    const ::db::GraphMetadata& metadata = view.metadata();
    const ::db::CardinalityEstimation estimation(view);

    return estimateFactorRows(leftFactor, metadata, estimation)
         < estimateFactorRows(rightFactor, metadata, estimation);
}

bool matchEqualityCross(FilterOp filter, EqualityCross& match) {
    EqOp equality = filter.getMask().getDefiningOp<EqOp>();
    if (!equality || !equality.getResult().hasOneUse()) {
        return false;
    }

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

    if (!keysShareAColumnType(match._left, match._right)) {
        return false;
    }

    const bool conesArePrivate = keyConeIsPrivateTo(match._left._cone, equality)
                                 && keyConeIsPrivateTo(match._right._cone, equality);
    if (!conesArePrivate) {
        return false;
    }

    return carriesProductColumnsOnly(filter, product) && productRowsReachOnly(product, match);
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

void fuseHashJoin(EqualityCross& match, mlir::OpBuilder& builder) {
    CrossProduct product = match._product;

    Yield leftYield = cast<Yield>(product.getLeftFactor().front().getTerminator());
    Yield rightYield = cast<Yield>(product.getRightFactor().front().getTerminator());
    const size_t leftCount = leftYield.getNumOperands();
    const size_t rightCount = rightYield.getNumOperands();

    const size_t leftKey = sinkKeyIntoFactor(match._left, 0, builder);
    const size_t rightKey = sinkKeyIntoFactor(match._right, leftCount, builder);

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

    builder.setInsertionPoint(product);
    HashJoin join = builder.create<HashJoin>(product.getLoc(), resultTypes, probeKey, buildKey);
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

    // The join keeps only the rows the equality held on, so what the filter handed
    // downstream now comes from the join directly.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();
    for (size_t columnIndex = 0; columnIndex < carried.size(); columnIndex++) {
        const unsigned productIndex = cast<OpResult>(carried[columnIndex]).getResultNumber();
        filtered[columnIndex].replaceAllUsesWith(joinColumns[productIndex]);
    }

    for (size_t columnIndex = 0; columnIndex < joinColumns.size(); columnIndex++) {
        product.getResult(columnIndex).replaceAllUsesWith(joinColumns[columnIndex]);
    }

    match._filter.erase();
    match._equality.erase();

    const JoinKeySide* const sides[] = {&match._left, &match._right};
    for (const JoinKeySide* const side : sides) {
        for (Operation* const coneOp : llvm::reverse(side->_cone._ops)) {
            eraseIfUnused(coneOp);
        }
    }

    product.erase();
}

struct FuseHashJoin : public impl::FuseHashJoinBase<FuseHashJoin> {
    FuseHashJoin() {}

    FuseHashJoin(const DBPassContext& context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect the matches first: fusing erases ops, which would invalidate the walk.
        const DBPassContext& context = _context;
        llvm::SmallVector<EqualityCross, 2> matches;
        root->walk([&matches, &context](FilterOp filter) {
            EqualityCross match;
            if (!matchEqualityCross(filter, match) || prefersTheProduct(match, context)) {
                return;
            }

            match._buildsTheLeftFactor = buildsTheLeftFactor(match, context);
            matches.push_back(match);
        });

        mlir::OpBuilder builder(&getContext());
        for (EqualityCross& match : matches) {
            fuseHashJoin(match, builder);
        }
    }

private:
    DBPassContext _context;
};

}

std::unique_ptr<Pass> createFuseHashJoin(const DBPassContext& context) {
    return std::make_unique<FuseHashJoin>(context);
}

}
