#include "DBCarriedColumns.h"

#include <stdint.h>
#include <optional>

#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/STLExtras.h"

#include "DBCrossProducts.h"
#include "DBPassUtils.h"

#include "BioAssert.h"

using namespace mlir;
using namespace mlir::db;

namespace {

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

void renumberEndColumn(ExplorePaths exploration, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    const std::optional<uint64_t> endColumn = exploration.getEndColumn();
    if (!endColumn) {
        return;
    }

    const auto keptIt = llvm::find(kept, static_cast<size_t>(*endColumn));
    bioassert(keptIt != kept.end(), "End column {} is not in the trimmed carry set", *endColumn);

    state.attributes.set(exploration.getEndColumnAttrName(), unsignedAttribute(static_cast<uint64_t>(keptIt - kept.begin()), builder));
}

void trimAttributes(Operation* op, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    if (Sort sort = dyn_cast<Sort>(op)) {
        renumberSortKeys(sort, kept, state, builder);
    } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
        renumberEndColumn(exploration, kept, state, builder);
    } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
        trimGroupAggregateKinds(groupAggregate, kept, state, builder);
    } else if (Collect collect = dyn_cast<Collect>(op)) {
        trimCollectAttributes(collect, kept, state, builder);
    }
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

}

OptionalYield mlir::db::patternYield(OptionalMatch optional) {
    return cast<OptionalYield>(optional.getPattern().front().getTerminator());
}

OptionalMatch mlir::db::rebuildOptionalMatch(OptionalMatch optional, llvm::ArrayRef<size_t> keptResults, mlir::OpBuilder& builder) {
    OptionalYield yield = patternYield(optional);

    llvm::SmallVector<Type> resultTypes;
    for (const Value column : yield.getColumns()) {
        resultTypes.push_back(column.getType());
    }

    builder.setInsertionPoint(optional);
    OptionalMatch rebuilt = builder.create<OptionalMatch>(optional.getLoc(), resultTypes, optional.getInputColumns());
    rebuilt->setAttrs(optional->getAttrDictionary());
    rebuilt.getPattern().takeBody(optional.getPattern());

    const ResultRange results = optional->getResults();
    for (size_t index = 0; index < keptResults.size(); index++) {
        results[keptResults[index]].replaceAllUsesWith(rebuilt->getResult(index));
    }

    optional.erase();

    return rebuilt;
}

size_t mlir::db::collectValueCount(Collect collect) {
    const size_t aggregateCount = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {}).size();

    return collect.getColumns().size() - collect.getKeyCount() - aggregateCount;
}

bool mlir::db::matchCarrySet(Operation* op, CarrySet& carrySet) {
    ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(op);

    return carrier && carrier.getCarrySet(carrySet);
}

MutableOperandRange mlir::db::trimmedOperandsOf(Operation* op) {
    if (isa<GroupAggregate, Collect>(op)) {
        return MutableOperandRange(op);
    }

    return cast<ColumnCarrierOpInterface>(op).getCarrySetMutable();
}

Operation* mlir::db::rebuildOp(Operation* op, OperationState& state, mlir::OpBuilder& builder) {
    const unsigned regionCount = op->getNumRegions();
    for (unsigned regionIndex = 0; regionIndex < regionCount; regionIndex++) {
        state.addRegion();
    }

    builder.setInsertionPoint(op);
    Operation* const rebuilt = builder.create(state);

    for (unsigned regionIndex = 0; regionIndex < regionCount; regionIndex++) {
        rebuilt->getRegion(regionIndex).takeBody(op->getRegion(regionIndex));
    }

    return rebuilt;
}

void mlir::db::trimCarrySet(Operation* op, const CarrySet& carrySet, llvm::ArrayRef<size_t> kept, mlir::OpBuilder& builder) {
    const Operation::operand_range operands = op->getOperands();
    const Operation::result_range results = op->getResults();

    llvm::SmallVector<Value> keptOperands;
    llvm::SmallVector<Type> trimmedTypes;
    llvm::append_range(trimmedTypes, results.take_front(carrySet._resultOffset).getTypes());

    for (const size_t carriedIndex : kept) {
        keptOperands.push_back(operands[carrySet._operandOffset + carriedIndex]);
        trimmedTypes.push_back(results[carrySet._resultOffset + carriedIndex].getType());
    }

    OperationState state(op->getLoc(), op->getName());
    state.addOperands(operands);
    state.addTypes(trimmedTypes);
    state.addAttributes(op->getAttrs());
    trimAttributes(op, kept, state, builder);

    Operation* const trimmed = rebuildOp(op, state, builder);
    trimmedOperandsOf(trimmed).assign(keptOperands);

    for (size_t resultIndex = 0; resultIndex < carrySet._resultOffset; resultIndex++) {
        results[resultIndex].replaceAllUsesWith(trimmed->getResult(resultIndex));
    }

    for (size_t keptIndex = 0; keptIndex < kept.size(); keptIndex++) {
        Value carried = results[carrySet._resultOffset + kept[keptIndex]];
        carried.replaceAllUsesWith(trimmed->getResult(carrySet._resultOffset + keptIndex));
    }
}

void mlir::db::keptResultTypes(Operation* op, const llvm::SmallBitVector& keep, llvm::SmallVectorImpl<Type>& types) {
    const Operation::result_range results = op->getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (keep[resultIndex]) {
            types.push_back(results[resultIndex].getType());
        }
    }
}

void mlir::db::replaceWithTrimmedFactors(Operation* op,
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
}

bool mlir::db::matchRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    ColumnCarrierOpInterface carrier = dyn_cast_or_null<ColumnCarrierOpInterface>(region.getParentOp());

    return carrier && carrier.getRegionCarrySet(region, carrySet);
}

Value mlir::db::carriedColumnOf(BlockArgument argument) {
    Region& region = *argument.getParentRegion();

    RegionCarrySet carrySet;
    if (!matchRegionCarrySet(region, carrySet)) {
        return {};
    }

    const size_t argumentIndex = argument.getArgNumber();
    const bool isCarried = argumentIndex >= carrySet._argumentOffset
                        && argumentIndex < carrySet._argumentOffset + carrySet._count;
    if (!isCarried) {
        return {};
    }

    return region.getParentOp()->getOperand(carrySet._operandOffset + argumentIndex - carrySet._argumentOffset);
}

BlockArgument mlir::db::carriedArgument(Region& region, size_t carriedIndex) {
    RegionCarrySet carrySet;
    const bool carries = matchRegionCarrySet(region, carrySet);
    bioassert(carries, "A region a column is carried into belongs to an op with a carry set");

    return region.front().getArgument(static_cast<unsigned>(carrySet._argumentOffset + carriedIndex));
}

size_t mlir::db::carryIntoRegions(Operation* op, Value column, mlir::RewriterBase& rewriter, bool& appended) {
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);
    Region& firstRegion = op->getRegion(0);

    RegionCarrySet carrySet;
    const bool carries = carrier.getRegionCarrySet(firstRegion, carrySet);
    bioassert(carries, "An op a column is carried into carries columns into its regions");

    const OperandRange carried = op->getOperands().slice(carrySet._operandOffset, carrySet._count);
    const auto carriedIt = llvm::find(carried, column);

    appended = carriedIt == carried.end();
    if (!appended) {
        return static_cast<size_t>(carriedIt - carried.begin());
    }

    llvm::SmallVector<std::pair<Block*, unsigned>, 2> argumentPositions;
    for (Region& region : op->getRegions()) {
        RegionCarrySet regionCarrySet;
        const bool carriesIntoRegion = carrier.getRegionCarrySet(region, regionCarrySet);
        bioassert(carriesIntoRegion, "An op carrying columns into a region carries them into all of them");

        if (!region.empty()) {
            argumentPositions.emplace_back(&region.front(), static_cast<unsigned>(regionCarrySet._argumentOffset + regionCarrySet._count));
        }
    }

    rewriter.modifyOpInPlace(op, [&carrier, column, &argumentPositions] {
        carrier.getRegionCarrySetMutable().append(column);

        for (const auto& [body, position] : argumentPositions) {
            body->insertArgument(position, column.getType(), column.getLoc());
        }
    });

    return carrySet._count;
}

BlockArgument mlir::db::carryIntoRegion(Region& region, Value column, mlir::RewriterBase& rewriter) {
    bool appended = false;
    const size_t carriedIndex = carryIntoRegions(region.getParentOp(), column, rewriter, appended);

    return carriedArgument(region, carriedIndex);
}

Value mlir::db::appendCarriedColumn(Operation* op, Value column, mlir::RewriterBase& rewriter) {
    CarrySet carrySet;
    const bool carries = matchCarrySet(op, carrySet);
    bioassert(carries, "A column is carried through an op with a carry set");

    const size_t resultIndex = carrySet._resultOffset + carrySet._count;

    llvm::SmallVector<Type> types;
    llvm::append_range(types, op->getResultTypes());
    types.insert(types.begin() + resultIndex, column.getType());

    OperationState state(op->getLoc(), op->getName());
    state.addOperands(op->getOperands());
    state.addTypes(types);
    state.addAttributes(op->getAttrs());

    Operation* const widened = rebuildOp(op, state, rewriter);
    rewriter.modifyOpInPlace(widened, [widened, column] {
        cast<ColumnCarrierOpInterface>(widened).getCarrySetMutable().append(column);
    });

    const size_t resultCount = op->getNumResults();

    llvm::SmallVector<Value> replacements;
    for (size_t index = 0; index < resultCount; index++) {
        const size_t widenedIndex = index < resultIndex ? index : index + 1;
        replacements.push_back(widened->getResult(static_cast<unsigned>(widenedIndex)));
    }

    rewriter.replaceOp(op, replacements);

    return widened->getResult(static_cast<unsigned>(resultIndex));
}
