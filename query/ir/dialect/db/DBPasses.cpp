#include "DBPasses.h"

#include <algorithm>
#include <optional>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "DBOps.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSESCANBYLABEL
#define GEN_PASS_DEF_PUSHDOWNFILTERS
#define GEN_PASS_DEF_FUSESCANBYNODEIDS
#define GEN_PASS_DEF_TRIMUNREADCOLUMNS
#include "DBPasses.h.inc"

namespace {

struct LabelScanChain {
    ScanNodes scan;
    GetNodeLabelSet labelSet;
    CheckLabelConstraint check;
};

std::optional<LabelScanChain> matchLabelScanChain(FilterOp filter) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return std::nullopt;
    }

    const Value scanColumn = columns[0];

    auto check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return std::nullopt;
    }

    auto labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet || labelSet.getInputNodes() != scanColumn) {
        return std::nullopt;
    }

    auto scan = scanColumn.getDefiningOp<ScanNodes>();
    if (!scan) {
        return std::nullopt;
    }

    return LabelScanChain {.scan = scan, .labelSet = labelSet, .check = check};
}

void eraseIfUnused(Operation* op) {
    if (op && op->use_empty()) {
        op->erase();
    }
}

struct FuseScanByLabel : public impl::FuseScanByLabelBase<FuseScanByLabel> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect matches first: fusing erases ops, which would invalidate the walk.
        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            if (matchLabelScanChain(filter)) {
                filters.push_back(filter);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            std::optional<LabelScanChain> chain = matchLabelScanChain(filter);
            if (!chain) {
                continue;
            }

            builder.setInsertionPoint(filter);
            auto scanByLabel = builder.create<ScanNodesByLabel>(filter.getLoc(),
                                                                chain->scan.getResult().getType(),
                                                                chain->check.getLabels());

            Operation* const filterOp = filter.getOperation();
            filterOp->getResult(0).replaceAllUsesWith(scanByLabel.getResult());
            filterOp->erase();

            // Drop the now-dead chain, consumer to producer, each only if unused.
            eraseIfUnused(chain->check);
            eraseIfUnused(chain->labelSet);
            eraseIfUnused(chain->scan);
        }
    }
};

bool isNodeSource(Operation* op) {
    return isa<ScanNodes, ScanNodesByLabel, ConstScanNodes>(op);
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

std::optional<PushablePredicate> matchPushablePredicate(FilterOp filter) {
    Operation* const maskDef = filter.getMask().getDefiningOp();
    if (!maskDef || !isMaskComputeOp(maskDef)) {
        return std::nullopt;
    }

    MaskCone cone = collectMaskCone(filter.getMask());
    if (cone._inputs.empty()) {
        return std::nullopt;
    }

    Value anchor;
    bool reachedAnchor = true;
    for (const Value input : cone._inputs) {
        const Value inputAnchor = climbToLineageAnchor(input);
        if (!inputAnchor) {
            return std::nullopt;
        }

        if (!anchor) {
            anchor = inputAnchor;
        } else if (anchor != inputAnchor) {
            // More than one lineage feeds the mask: not a single-variable predicate.
            return std::nullopt;
        }

        if (input != inputAnchor) {
            reachedAnchor = false;
        }
    }

    // Filter already maximally pushed down
    if (reachedAnchor) {
        return std::nullopt;
    }

    return PushablePredicate {._anchor = anchor, ._cone = std::move(cone)};
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
        Operation* const root = getOperation();

        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            filters.push_back(filter);
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            std::optional<PushablePredicate> pushable = matchPushablePredicate(filter);
            if (!pushable) {
                continue;
            }

            pushDownPredicate(filter, *pushable, builder);
        }
    }
};

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

struct NodeIDScanChain {
    Operation* _source {nullptr};
    ArrayAttr _labels;
    llvm::SmallVector<int64_t> _nodeIDs;
};

bool matchNodeIDScanChain(FilterOp filter, NodeIDScanChain& chain) {
    // The filter is replaced by a source, which yields the listed nodes and nothing else.
    // Any other column it carries (a property read before the WHERE, say) has no filtered
    // counterpart in that source to be rewired to, so such a filter has to stay.
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return false;
    }

    const Value scanColumn = columns.front();
    chain._source = scanColumn.getDefiningOp();
    if (!chain._source || !isa<ScanNodes, ScanNodesByLabel>(chain._source)) {
        return false;
    }

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(chain._source)) {
        chain._labels = scanByLabel.getLabels();
    }

    if (!collectNodeIDDisjunction(filter.getMask(), scanColumn, chain._nodeIDs)) {
        return false;
    }

    // A scan yields each node once in ID order, so the filter did too.
    llvm::sort(chain._nodeIDs);
    chain._nodeIDs.erase(std::unique(chain._nodeIDs.begin(), chain._nodeIDs.end()), chain._nodeIDs.end());

    return true;
}

void fuseScanByNodeIDs(FilterOp filter, const NodeIDScanChain& chain, mlir::OpBuilder& builder) {
    const mlir::Location loc = filter.getLoc();
    const Type nodeColumnType = chain._source->getResult(0).getType();

    builder.setInsertionPoint(filter);
    ConstScanNodes constScan = builder.create<ConstScanNodes>(loc, nodeColumnType, chain._nodeIDs);
    Value fused = constScan.getResult();

    if (chain._labels) {
        MLIRContext* const context = builder.getContext();
        const Type labelSetType = ColumnType::get(context, storage::LabelSetIDType::get(context));
        const Type boolType = ColumnType::get(context, storage::BoolType::get(context));

        GetNodeLabelSet labelSet = builder.create<GetNodeLabelSet>(loc, labelSetType, fused);
        CheckLabelConstraint check = builder.create<CheckLabelConstraint>(loc, boolType, labelSet.getResult(), chain._labels);

        const llvm::SmallVector<Type> resultTypes {nodeColumnType};
        const llvm::SmallVector<Value> columns {fused};
        FilterOp labelFilter = builder.create<FilterOp>(loc, resultTypes, check.getResult(), columns);
        fused = labelFilter.getResult(0);
    }

    const MaskCone cone = collectMaskCone(filter.getMask());

    for (Value filtered : filter.getFilteredColumns()) {
        filtered.replaceAllUsesWith(fused);
    }
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
    eraseIfUnused(chain._source);
}

struct FuseScanByNodeIDs : public impl::FuseScanByNodeIDsBase<FuseScanByNodeIDs> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            filters.push_back(filter);
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            NodeIDScanChain chain;
            if (!matchNodeIDScanChain(filter, chain)) {
                continue;
            }

            fuseScanByNodeIDs(filter, chain, builder);
        }
    }
};

// Where an op's carry set sits: the carried operands start at _operandOffset and each comes
// back as the result at the same position from _resultOffset.
struct CarrySetLayout {
    size_t _operandOffset {0};
    size_t _resultOffset {0};
};

std::optional<CarrySetLayout> carrySetLayout(Operation* op) {
    if (isEdgeHop(op)) {
        return CarrySetLayout {._operandOffset = 1, ._resultOffset = hopFixedResultCount};
    } else if (isa<FilterOp>(op)) {
        return CarrySetLayout {._operandOffset = 1, ._resultOffset = 0};
    } else if (isa<Limit, Skip, Sort, GroupAggregate, Collect>(op)) {
        return CarrySetLayout {._operandOffset = 0, ._resultOffset = 0};
    }

    return std::nullopt;
}

bool trimsColumns(Operation* op) {
    return carrySetLayout(op).has_value() || isa<CrossProduct>(op);
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

            const CarrySetLayout layout = *carrySetLayout(op);

            llvm::SmallVector<size_t> kept;
            selectKeptCarriedColumns(op, layout, kept);

            if (kept.size() == carriedCount(op, layout)) {
                continue;
            }

            trimCarrySet(op, layout, kept, builder);
        }
    }
};

}

}
