#include "DBPassUtils.h"

#include "mlir/Transforms/RegionUtils.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallPtrSet.h"

#include "IRConstantColumn.h"
#include "DBColumnElement.h"
#include "BoundEntityIDs.h"

using namespace mlir;
using namespace mlir::db;

namespace {

template <typename HopOp, typename ByTypeOp, typename ByLabelOp, typename ByTypeAndLabelOp>
Operation* createHop(Location loc,
                     TypeRange types,
                     Value from,
                     ArrayAttr edgeTypes,
                     ArrayAttr labels,
                     ValueRange carried,
                     mlir::OpBuilder& builder) {
    const bool asksLabels = labels && !labels.empty();

    if (edgeTypes && asksLabels) {
        return builder.create<ByTypeAndLabelOp>(loc, types, from, edgeTypes, labels, carried).getOperation();
    } else if (edgeTypes) {
        return builder.create<ByTypeOp>(loc, types, from, edgeTypes, carried).getOperation();
    } else if (asksLabels) {
        return builder.create<ByLabelOp>(loc, types, from, labels, carried).getOperation();
    } else {
        return builder.create<HopOp>(loc, types, from, carried).getOperation();
    }
}

}

const DBPassContext mlir::db::defaultPassContext;

void mlir::db::eraseIfUnused(Operation* op) {
    if (op && op->use_empty()) {
        op->erase();
    }
}

bool mlir::db::isNodeSource(Operation* op) {
    return isa<ScanNodes, ScanNodesByLabel, ConstScanNodes, ScanNodesByPropertyValue>(op);
}

bool mlir::db::isEdgeHop(Operation* op) {
    return isa<GetOutEdges,
               GetInEdges,
               GetEdges,
               GetOutEdgesByType,
               GetInEdgesByType,
               GetOutEdgesByLabel,
               GetInEdgesByLabel,
               GetOutEdgesByTypeAndLabel,
               GetInEdgesByTypeAndLabel>(op);
}

bool mlir::db::isEdgeScan(Operation* op) {
    return isa<ScanEdges,
               ScanEdgesByType,
               ScanOutEdgesByLabelSrc,
               ScanInEdgesByLabelTgt,
               ScanOutEdgesByLabelTgt,
               ScanInEdgesByLabelSrc,
               ScanOutEdgesByLabelSrcTgt,
               ScanInEdgesByLabelSrcTgt>(op);
}

bool mlir::db::isReverseHop(Operation* op) {
    return isa<GetInEdges, GetInEdgesByType, GetInEdgesByLabel, GetInEdgesByTypeAndLabel>(op);
}

size_t mlir::db::hopInputResult(Operation* hop) {
    return isReverseHop(hop) ? 3 : 0;
}

size_t mlir::db::hopReachedResult(Operation* hop) {
    return isReverseHop(hop) ? 0 : 3;
}

bool mlir::db::isNodeColumn(Value column) {
    const ColumnType type = dyn_cast<ColumnType>(column.getType());
    return type && isa<storage::NodeIDType>(type.getType());
}

bool mlir::db::isEdgeColumn(Value column) {
    const ColumnType type = dyn_cast<ColumnType>(column.getType());
    return type && isa<storage::EdgeIDType>(type.getType());
}

Type mlir::db::entityElementOf(Value column) {
    const ColumnType type = dyn_cast<ColumnType>(column.getType());
    if (!type) {
        return Type();
    } else if (storage::isEntityIDType(type.getType())) {
        return type.getType();
    }

    Operation* const read = column.getDefiningOp();
    if (!isa_and_nonnull<ListIndex, Head, Last>(read)) {
        return Type();
    }

    const storage::ListType list = dyn_cast<storage::ListType>(::db::declaredColumnElement(read->getOperand(0)));
    if (!list || !storage::isEntityIDType(list.getElementType())) {
        return Type();
    }

    return list.getElementType();
}

bool mlir::db::isEntityColumn(Value column) {
    return static_cast<bool>(entityElementOf(column));
}

MaskCone mlir::db::collectMaskCone(Value mask) {
    MaskCone cone;

    const auto movesOntoOtherRows = [](Operation* op) {
        return computesFromTheRow(op) && !raisesOnSomeRows(op);
    };

    llvm::SmallPtrSet<Operation*, 8> visited;
    collectConePostOrder(mask, movesOntoOtherRows, visited, cone);

    return cone;
}

Value mlir::db::checkLabels(Value nodes, ArrayAttr labels, Location loc, mlir::OpBuilder& builder) {
    MLIRContext* const context = builder.getContext();
    const Type labelSetType = ColumnType::get(context, storage::LabelSetIDType::get(context));
    const Type boolType = ColumnType::get(context, storage::BoolType::get(context));

    GetNodeLabelSet labelSet = builder.create<GetNodeLabelSet>(loc, labelSetType, nodes);
    CheckLabelConstraint check = builder.create<CheckLabelConstraint>(loc,
                                                                      boolType,
                                                                      labelSet.getResult(),
                                                                      builder.getArrayAttr({labels}));

    return check.getResult();
}

void mlir::db::keepLabelledRows(llvm::SmallVectorImpl<Value>& columns, ArrayAttr labels, Location loc, mlir::OpBuilder& builder) {
    const Value mask = checkLabels(columns.front(), labels, loc, builder);

    const ValueRange columnRange(columns);
    FilterOp labelFilter = builder.create<FilterOp>(loc, columnRange.getTypes(), mask, columnRange);

    columns.assign(labelFilter.getResults().begin(), labelFilter.getResults().end());
}

bool mlir::db::computesAValuePerRow(Operation* op) {
    if (op->hasTrait<OpTrait::ConstantLike>()) {
        return false;
    }

    return op->hasTrait<OpTrait::ConstantThroughOperands>()
        || computesPerRow(op)
        || isa<Range, MakeList, MakeMap, ListSlice, DynamicMapKey, MakePath, ExpandPath, PathLength,
               CollectSubquery, CountSubquery, ExistsSubquery, PatternComprehension>(op);
}

void mlir::db::cloneRowCone(llvm::ArrayRef<Operation*> ops, mlir::IRMapping& mapping, mlir::RewriterBase& rewriter) {
    for (Operation* const op : ops) {
        rewriter.clone(*op, mapping);
    }
}

void mlir::db::eraseUnusedRowCone(llvm::ArrayRef<Operation*> ops, mlir::RewriterBase& rewriter) {
    for (Operation* const op : llvm::reverse(ops)) {
        eraseIfUnused(op, rewriter);
    }
}

void mlir::db::hoistDefinitionsAhead(llvm::ArrayRef<Value> values, Operation* anchor, mlir::RewriterBase& rewriter) {
    Block* const block = anchor->getBlock();

    llvm::SmallVector<Value> pending(values.begin(), values.end());
    llvm::SmallPtrSet<Operation*, 8> visited;
    llvm::SmallVector<Operation*> hoisted;
    while (!pending.empty()) {
        const Value value = pending.pop_back_val();
        Operation* const def = value.getDefiningOp();
        const bool computedAfter = def && def->getBlock() == block && anchor->isBeforeInBlock(def);
        if (!computedAfter || !visited.insert(def).second) {
            continue;
        }

        hoisted.push_back(def);
        llvm::append_range(pending, def->getOperands());

        llvm::SetVector<Value> readFromAbove;
        mlir::getUsedValuesDefinedAbove(def->getRegions(), readFromAbove);
        llvm::append_range(pending, readFromAbove);
    }

    llvm::sort(hoisted, [](Operation* lhs, Operation* rhs) {
        return lhs->isBeforeInBlock(rhs);
    });

    for (Operation* const op : hoisted) {
        rewriter.moveOpBefore(op, anchor);
    }
}

void mlir::db::eraseUnusedRowConeOf(Operation* root, mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Operation*> dead {root};
    llvm::SmallPtrSet<Operation*, 8> erased;
    while (!dead.empty()) {
        Operation* const op = dead.pop_back_val();
        if (erased.contains(op) || !op->use_empty()) {
            continue;
        }

        llvm::SmallVector<Operation*> operandDefs;
        for (const Value operand : op->getOperands()) {
            Operation* const def = operand.getDefiningOp();
            if (def && computesFromTheRow(def)) {
                operandDefs.push_back(def);
            }
        }

        erased.insert(op);
        rewriter.eraseOp(op);
        dead.append(operandDefs.begin(), operandDefs.end());
    }
}

void mlir::db::eraseIfUnused(Operation* op, mlir::RewriterBase& rewriter) {
    if (op && op->use_empty()) {
        rewriter.eraseOp(op);
    }
}

void mlir::db::hopTypes(Operation* hop, llvm::SmallVectorImpl<Type>& types) {
    for (size_t index = 0; index < hopFixedResultCount; index++) {
        types.push_back(hop->getResult(index).getType());
    }
}

Operation* mlir::db::createReversedHop(Operation* hop, Value from, ArrayAttr labels, ValueRange carried, mlir::OpBuilder& builder) {
    const Location loc = hop->getLoc();
    const ArrayAttr edgeTypes = hop->getAttrOfType<ArrayAttr>("edge_types");

    llvm::SmallVector<Type> types;
    hopTypes(hop, types);
    llvm::append_range(types, carried.getTypes());

    if (isa<GetEdges>(hop)) {
        return builder.create<GetEdges>(loc, types, from, carried).getOperation();
    } else if (isReverseHop(hop)) {
        return createHop<GetOutEdges, GetOutEdgesByType, GetOutEdgesByLabel, GetOutEdgesByTypeAndLabel>(loc,
                                                                                                         types,
                                                                                                         from,
                                                                                                         edgeTypes,
                                                                                                         labels,
                                                                                                         carried,
                                                                                                         builder);
    } else {
        return createHop<GetInEdges, GetInEdgesByType, GetInEdgesByLabel, GetInEdgesByTypeAndLabel>(loc,
                                                                                                     types,
                                                                                                     from,
                                                                                                     edgeTypes,
                                                                                                     labels,
                                                                                                     carried,
                                                                                                     builder);
    }
}

Type mlir::db::boolColumnType(mlir::MLIRContext* context) {
    return ColumnType::get(context, storage::BoolType::get(context));
}

IntegerAttr mlir::db::unsignedAttribute(uint64_t value, mlir::OpBuilder& builder) {
    const IntegerType type = IntegerType::get(builder.getContext(), 64, IntegerType::Unsigned);

    return IntegerAttr::get(type, value);
}

void mlir::db::takeHopRegions(ExplorePaths from, ExplorePaths to) {
    MutableArrayRef<Region> fromHops = from.getHops();
    MutableArrayRef<Region> toHops = to.getHops();

    for (size_t step = 0; step < fromHops.size(); step++) {
        toHops[step].takeBody(fromHops[step]);
    }
}

Value mlir::db::climbFilters(Value column) {
    while (FilterOp filter = column.getDefiningOp<FilterOp>()) {
        column = filter.getColumnsToFilter()[cast<OpResult>(column).getResultNumber()];
    }

    return column;
}

Value mlir::db::seedColumnOf(ExplorePaths exploration, Value column) {
    const Value climbed = climbFilters(column);
    if (climbed.getDefiningOp() != exploration.getOperation()) {
        return {};
    }

    const size_t resultIndex = cast<OpResult>(climbed).getResultNumber();
    constexpr size_t TARGET_RESULT_INDEX = 1;
    const bool holdsTheSeed = resultIndex == 0 || (resultIndex == TARGET_RESULT_INDEX && exploration.getEndsOnSeed());

    if (holdsTheSeed) {
        return exploration.getInputNodes();
    } else if (resultIndex >= pathFixedResultCount) {
        return exploration.getColumnsToFilter()[resultIndex - pathFixedResultCount];
    } else {
        return {};
    }
}

bool mlir::db::isHop(Operation* op) {
    return isa<GetOutEdges, GetInEdges, GetEdges>(op);
}
