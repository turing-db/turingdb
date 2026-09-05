#include "DBPasses.h"

#include <algorithm>

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
Value climbToLineageAnchor(Value column) {
    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return {};
        }

        if (isNodeSource(def)) {
            return column;
        }

        if (isa<FilterOp>(def)) {
            // A filter passes each column through unchanged, so its result is the same
            // variable one step on - a valid boundary to sit a further filter right after.
            return column;
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
            } else if (resultIndex >= hopFixedResultCount) {
                // Carried columns follow input_nodes (operand 0) in operand order.
                column = def->getOperand(1 + (resultIndex - hopFixedResultCount));
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
               AddOp, SubOp, MulOp, DivOp, ModOp, PowOp,
               ConstantOp,
               GetNodeProperties, GetEdgeProperties>(op);
}

struct MaskCone {
    llvm::SmallVector<Operation*> _ops;
    llvm::SmallSetVector<Value, 4> _inputs;
};

// Gathers the mask's compute cone and the external columns feeding it. Valid
// even when the cone spans blocks.
void collectConePostOrder(Value value, llvm::SmallPtrSet<Operation*, 8>& visited, MaskCone& cone) {
    Operation* const def = value.getDefiningOp();

    if (!def || !isMaskComputeOp(def)) {
        cone._inputs.insert(value);
        return;
    }

    if (!visited.insert(def).second) {
        return;
    }

    for (const Value operand : def->getOperands()) {
        collectConePostOrder(operand, visited, cone);
    }

    cone._ops.push_back(def);
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

bool matchPushablePredicate(FilterOp filter, PushablePredicate& pushable) {
    Operation* const maskDef = filter.getMask().getDefiningOp();
    if (!maskDef || !isMaskComputeOp(maskDef)) {
        return false;
    }

    pushable._cone = collectMaskCone(filter.getMask());
    if (pushable._cone._inputs.empty()) {
        return false;
    }

    bool reachedAnchor = true;
    for (const Value input : pushable._cone._inputs) {
        const Value inputAnchor = climbToLineageAnchor(input);
        if (!inputAnchor) {
            return false;
        }

        if (!pushable._anchor) {
            pushable._anchor = inputAnchor;
        } else if (pushable._anchor != inputAnchor) {
            // More than one lineage feeds the mask: not a single-variable predicate.
            return false;
        }

        if (input != inputAnchor) {
            reachedAnchor = false;
        }
    }

    // Filter already maximally pushed down
    return !reachedAnchor;
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
    }

    return false;
}

bool trimsColumns(Operation* op) {
    CarrySetLayout layout;

    return matchCarrySetLayout(op, layout) || isa<CrossProduct>(op);
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

// A product's results are the columns its two factors yield; a result nobody reads leaves
// the yield, and each factor keeps a row-carrying column to be sized by.
void trimCrossProduct(CrossProduct product, mlir::OpBuilder& builder) {
    Yield leftYield = cast<Yield>(product.getLeftFactor().front().getTerminator());
    Yield rightYield = cast<Yield>(product.getRightFactor().front().getTerminator());
    const size_t leftCount = leftYield.getNumOperands();

    const Operation::result_range results = product.getResults();

    llvm::SmallBitVector keep(results.size());
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (!results[resultIndex].use_empty()) {
            keep.set(resultIndex);
        }
    }

    keepRowCarryingColumn(leftYield.getColumns(), 0, keep);
    keepRowCarryingColumn(rightYield.getColumns(), leftCount, keep);

    if (keep.all()) {
        return;
    }

    llvm::SmallVector<Type> trimmedTypes;
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (keep[resultIndex]) {
            trimmedTypes.push_back(results[resultIndex].getType());
        }
    }

    builder.setInsertionPoint(product);
    CrossProduct trimmed = builder.create<CrossProduct>(product.getLoc(), trimmedTypes);
    trimmed.getLeftFactor().takeBody(product.getLeftFactor());
    trimmed.getRightFactor().takeBody(product.getRightFactor());

    eraseUnkeptYields(leftYield, keep, 0);
    eraseUnkeptYields(rightYield, keep, leftCount);

    size_t trimmedIndex = 0;
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (keep[resultIndex]) {
            results[resultIndex].replaceAllUsesWith(trimmed.getResult(trimmedIndex++));
        }
    }

    product.erase();
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
            }

            CarrySetLayout layout;
            const bool carries = matchCarrySetLayout(op, layout);
            bioassert(carries, "A trimming op that is not a cross product has a carry set");

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

}

}
