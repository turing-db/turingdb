#include "NLTranslator.h"

#include <vector>
#include <span>

#include "NLOps.h"
#include "StorageEnums.h"

#include "mlir/IR/Block.h"
#include "llvm/ADT/DenseMap.h"

#include "metadata/PropertyType.h"

#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// Whether the grouped reduction charges each of a group's values once, through a tally
bool chargesDistinctValues(GroupAggregateKind kind) {
    switch (kind) {
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::SumDistinct:
        case GroupAggregateKind::AvgDistinct:
        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            return true;
        break;

        case GroupAggregateKind::Count:
        case GroupAggregateKind::Sum:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::Avg:
        case GroupAggregateKind::CountRows:
        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::First:
            return false;
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

// The runtime grouped reduction the interpreter dispatches on, from the MLIR one
// the op carries. Like toRuntimeAggregateKind, this is the one place the two enums
// meet, keeping the runtime free of the MLIR dialect headers.
GroupAggregateKind toRuntimeGroupAggregateKind(storage::GroupAggregateKind kind) {
    switch (kind) {
        case storage::GroupAggregateKind::Count:
            return GroupAggregateKind::Count;
        break;

        case storage::GroupAggregateKind::Sum:
            return GroupAggregateKind::Sum;
        break;

        case storage::GroupAggregateKind::Min:
            return GroupAggregateKind::Min;
        break;

        case storage::GroupAggregateKind::Max:
            return GroupAggregateKind::Max;
        break;

        case storage::GroupAggregateKind::Avg:
            return GroupAggregateKind::Avg;
        break;

        case storage::GroupAggregateKind::CountDistinct:
            return GroupAggregateKind::CountDistinct;
        break;

        case storage::GroupAggregateKind::SumDistinct:
            return GroupAggregateKind::SumDistinct;
        break;

        case storage::GroupAggregateKind::AvgDistinct:
            return GroupAggregateKind::AvgDistinct;
        break;

        case storage::GroupAggregateKind::CountRows:
            return GroupAggregateKind::CountRows;
        break;

        case storage::GroupAggregateKind::StDev:
            return GroupAggregateKind::StDev;
        break;

        case storage::GroupAggregateKind::StDevP:
            return GroupAggregateKind::StDevP;
        break;

        case storage::GroupAggregateKind::StDevDistinct:
            return GroupAggregateKind::StDevDistinct;
        break;

        case storage::GroupAggregateKind::StDevPDistinct:
            return GroupAggregateKind::StDevPDistinct;
        break;

        case storage::GroupAggregateKind::First:
            return GroupAggregateKind::First;
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

}

void NLTranslator::translateGroupAggregateBuffer(nl::GroupAggregateBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the update and
    // the emit loop that name the handle share the same group table and per-group
    // state. The key buffers and accumulators themselves are allocated by the
    // update, which knows their types - the grouped sibling of translateSortBuffer.
    NLGroupAggregateState* state = _program->allocGroupAggregateState();
    _groupAggregateStates[buffer.getState()] = state;

    // The reset empties the group table and per-group state each time the block
    // holding this nl.group_aggregate_buffer runs: once at function scope for a
    // top-level grouped RETURN.
    NLGroupAggregateResetData* resetData = _program->allocFunctionData<NLGroupAggregateResetData>(state);
    body->addStmt(NLFunctionDescriptor {&NLExecutor::runGroupAggregateReset, resetData});
}

void NLTranslator::buildGroupAggregate(mlir::storage::GroupAggregateKind mlirKind,
                                       mlir::Value column,
                                       NLGroupAggregates::Aggregate& aggregate) {
    const GroupAggregateKind kind = toRuntimeGroupAggregateKind(mlirKind);
    const mlir::Type chunkType = column.getType();

    aggregate._input = getColumn(column);

    if (chargesDistinctValues(kind)) {
        aggregate._distinct = allocGroupDistinctTally(chunkType);
    }

    // A switch (not an if/else) over every kind so a new one is a compile error
    // here rather than silently taking the value-reduction path.
    switch (kind) {
        case GroupAggregateKind::Count: {
            // count tallies rows, so it keeps no reduced value, only the per-group
            // tally. count(*) over an ID chunk charges every row; count(x) over a
            // nullable value chunk charges only the present values.
            NLExecutor::selectGroupAggregateState(kind, ValueType::Int64, aggregate._stateType);

            const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
            const mlir::Type countElementType = chunk.getElementType();
            const auto nullable = mlir::dyn_cast<storage::NullableType>(countElementType);
            const mlir::Type countValueType = nullable ? nullable.getValueType() : mlir::Type {};
            if (isNullableList(countElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountOptListFold();
            } else if (nullable) {
                aggregate._fold = NLExecutor::selectGroupCountOptFold(chunkKindFromNullableValue(countValueType));
            } else if (mlir::isa<storage::AnyType>(countElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountCellFold();
            } else if (llvm::isa<storage::ListType>(countElementType)) {
                // No row of a list chunk is null, so a group's tally is its whole row
                // count, as it is for count(*).
                aggregate._fold = NLExecutor::selectGroupCountAllFold();
            } else if (isEntityIDElement(countElementType)) {
                // An invalid ID is the null an OPTIONAL MATCH leaves, so a group charges
                // its valid rows alone.
                const NLChunkKind countKind = chunkKindFromElementType(countElementType);
                aggregate._fold = NLExecutor::selectGroupCountValidIDFold(countKind);
            } else if (mlir::isa<storage::PathType>(countElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountPresentPathFold();
            } else {
                // Every other non-nullable chunk holds no null to skip - a column a CALL
                // yielded - so every row of the group is charged.
                aggregate._fold = NLExecutor::selectGroupCountAllFold();
            }
        }
        break;

        case GroupAggregateKind::CountRows: {
            // count(*) charges every row of the group and reads no value, so the
            // chunk it is anchored on needs no type dispatch at all
            NLExecutor::selectGroupAggregateState(kind, ValueType::Int64, aggregate._stateType);
            aggregate._fold = NLExecutor::selectGroupCountAllFold();
        }
        break;

        case GroupAggregateKind::CountDistinct: {
            // count(DISTINCT x) keeps the same per-group tally as count, charged
            // once per distinct value instead of once per row, so it shares count's
            // state and differs only in the fold. count(DISTINCT n) over an
            // ID chunk keys on the ID; count(DISTINCT x) over a nullable value chunk
            // keys on the value and skips the nulls.
            NLExecutor::selectGroupAggregateState(kind, ValueType::Int64, aggregate._stateType);

            const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
            const mlir::Type distinctElementType = chunk.getElementType();
            const auto nullable = mlir::dyn_cast<storage::NullableType>(distinctElementType);
            const mlir::Type distinctValueType = nullable ? nullable.getValueType() : mlir::Type {};
            if (isNullableList(distinctElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountDistinctOptListFold();
            } else if (nullable) {
                aggregate._fold = NLExecutor::selectGroupCountDistinctOptFold(chunkKindFromNullableValue(distinctValueType));
            } else if (mlir::isa<storage::AnyType>(distinctElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountDistinctCellFold();
            } else {
                aggregate._fold = NLExecutor::selectGroupCountDistinctChunkFold(getChunkKind(chunkType));
            }
        }
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
        case GroupAggregateKind::First: {
            // sum/min/max/avg/stdev/first reduce the values themselves, so the input must be
            // a nullable value chunk; stdev and avg of numbers accumulate as f64, the rest in
            // the input's own type. nullableChunkValueType rejects an ID chunk here. The
            // distinct kinds keep the shape of the kind they mirror and differ only in the
            // fold, which charges each of a group's values once.
            //
            // A type-erased input carries a type per cell, so stdev reduces it by tag into an
            // f64, sum and avg keep a tagged sum - an integer until a float joins it, or a
            // duration - and min/max/first keep a cell.
            const mlir::Type reducedElement = mlir::cast<nl::ChunkType>(chunkType).getElementType();
            const bool reducesTaggedCells = mlir::isa<storage::AnyType>(reducedElement);

            const bool keepsAValue = (kind == GroupAggregateKind::Min)
                                  || (kind == GroupAggregateKind::Max)
                                  || (kind == GroupAggregateKind::First);
            const bool sums = (kind == GroupAggregateKind::Sum) || (kind == GroupAggregateKind::SumDistinct);
            const bool averages = (kind == GroupAggregateKind::Avg) || (kind == GroupAggregateKind::AvgDistinct);
            const bool keepsTaggedCells = reducesTaggedCells && (keepsAValue || sums || averages);

            const auto nullableReduced = mlir::dyn_cast<storage::NullableType>(reducedElement);
            const mlir::Type reducedValue = nullableReduced ? nullableReduced.getValueType() : reducedElement;
            const bool accumulatesAsDouble = storage::reducesToADouble(mlirKind, reducedValue);

            const ValueType inputType = reducesTaggedCells ? ValueType::Double
                                                           : nullableChunkValueType(chunkType);
            const ValueType accumulatorType = accumulatesAsDouble ? ValueType::Double : inputType;

            if (keepsTaggedCells) {
                NLExecutor::selectTaggedGroupAggregateState(kind, aggregate._stateType);
            } else {
                NLExecutor::selectGroupAggregateState(kind, accumulatorType, aggregate._stateType);
            }

            if (reducesTaggedCells) {
                aggregate._fold = NLExecutor::selectTaggedGroupAggregateFold(kind);
            } else {
                aggregate._fold = NLExecutor::selectGroupAggregateFold(kind, inputType);
            }
        }
        break;
    }
}

void NLTranslator::addGroupKeyColumn(NLGroupTable& groupTable, mlir::Value column) {
    const mlir::Type chunkType = column.getType();

    NLGroupHashKey hashKey;
    buildGroupHashKey(chunkType, hashKey);

    NLGroupTable::KeyColumn key;
    key._input = getColumn(column);
    key._buffer = allocColumnForChunkType(chunkType);
    key._gatherAppend = selectGroupKeyGatherForChunkType(chunkType);
    key._emitCopy = selectCopyForChunkType(chunkType);
    key._gather = selectGatherForChunkType(chunkType);

    groupTable.addKeyColumn(hashKey, key);
}

NLGroupDistinctTally* NLTranslator::allocGroupDistinctTally(mlir::Type valueChunkType) {
    NLGroupHashKey valueKey;
    buildGroupHashKey(valueChunkType, valueKey);

    NLGroupDistinctTally* tally = _program->allocGroupDistinctTally();
    tally->setValueKey(valueKey);

    return tally;
}

void NLTranslator::translateGroupAggregateUpdate(nl::GroupAggregateUpdate update, NLStmtContainer* body) {
    const mlir::Value updateState = update.getState();
    NLGroupAggregateState* state = groupAggregateStateFor(updateState);

    // The keyCount / aggregate-kind spec lives on the nl.group_aggregate_buffer that
    // produced the handle.
    nl::GroupAggregateBuffer buffer = updateState.getDefiningOp<nl::GroupAggregateBuffer>();
    if (!buffer) {
        throw IRException("group_aggregate_update state must come from nl.group_aggregate_buffer");
    }

    // The key buffers and aggregates are set up once, by the single update
    // feeding an accumulator. Generated IR has exactly one update per accumulator; a
    // second would fold the same rows twice, so it is rejected here.
    NLGroupTable& groupTable = state->groupTable();
    NLGroupAggregates& aggregates = state->aggregates();
    if (!groupTable.keyColumns().empty() || !aggregates.getAggregates().empty()) {
        throw IRException("an nl.group_aggregate_buffer must be fed by a single nl.group_aggregate_update");
    }

    const mlir::OperandRange columns = update.getColumns();
    const size_t keyCount = buffer.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = buffer.getKinds();

    // The collected columns are the grouping keys followed by the aggregate inputs,
    // one per aggregate; the two counts must partition them exactly.
    if (columns.size() != keyCount + kinds.size()) {
        throw IRException("group_aggregate collects one column per grouping key and aggregate");
    }

    NLGroupAggregateUpdateData* data = _program->allocFunctionData<NLGroupAggregateUpdateData>(state);

    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        addGroupKeyColumn(groupTable, columns[keyIndex]);
    }

    groupTable.hashTable().chooseTable();

    // One state per group per aggregate, with the state type and the fold baked
    // from the kind and the input value type.
    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);

        NLGroupAggregates::Aggregate aggregate;
        buildGroupAggregate(kind, columns[keyCount + aggregateIndex], aggregate);

        aggregates.add(aggregate);
    }

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runGroupAggregateUpdate, data});
}

void NLTranslator::translateGroupAggregateLoop(const IteratorConfig& config,
                                               mlir::Block& loopBody,
                                               NLLimitState* limit,
                                               NLStmtContainer* body) {
    NLGroupAggregateState* state = config._groupAggregateState;
    if (!state) {
        throw IRException("nl.group_aggregate iterator must carry a group accumulator");
    }

    // For::verify binds one loop variable per iterator chunk, and the group
    // iterator's chunks are the grouping-key columns followed by the aggregate
    // results, so the loop must take exactly one variable per output column.
    std::vector<NLGroupTable::KeyColumn>& keyColumns = state->groupTable().keyColumns();
    const size_t keyCount = keyColumns.size();
    const std::span<NLGroupAggregates::Aggregate> aggregates = state->aggregates().getAggregates();
    const size_t aggregateCount = aggregates.size();
    if (loopBody.getNumArguments() != keyCount + aggregateCount) {
        throw IRException("nl.group_aggregate loop must bind one variable per output column");
    }

    NLGroupAggregateLoopData* loopData = _program->allocFunctionData<NLGroupAggregateLoopData>(state,
                                                                                          _program->allocStepBuffers());
    loopData->setLimit(limit);

    // Each loop variable is the emit output of one column: the first keyCount are the
    // grouping keys (filled by slicing their key buffers), the rest the aggregate
    // results (filled from the per-group state). Wire each into its column's slot on
    // the shared state, so runGroupAggregateLoop fills the loop variables directly.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        keyColumns[keyIndex]._output = output;
    }

    for (size_t aggregateIndex = 0; aggregateIndex < aggregateCount; aggregateIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyCount + aggregateIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        aggregates[aggregateIndex]._output = output;
    }

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runGroupAggregateLoop, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

NLGroupAggregateState* NLTranslator::groupAggregateStateFor(mlir::Value handle) const {
    const auto stateIt = _groupAggregateStates.find(handle);
    if (stateIt == _groupAggregateStates.end()) {
        throw IRException("group aggregate handle must be produced by an nl.group_aggregate_buffer");
    }

    return stateIt->second;
}
