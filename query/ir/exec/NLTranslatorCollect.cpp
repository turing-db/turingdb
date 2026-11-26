#include "NLTranslator.h"

#include <optional>
#include <vector>
#include <span>

#include "NLOps.h"
#include "StorageEnums.h"

#include "mlir/IR/Block.h"
#include "llvm/ADT/DenseMap.h"

#include "list/ListView.h"

#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// A drain wires its emit loop's variables into the accumulator's one output slot per
// column, so an accumulator can serve exactly one nl.collect or nl.unwind_collect. A
// second drain would rebind the first's outputs to its own, incompatible column types
// (a per-group list cell against an unwound value), leaving the first to emit through
// the other's columns. Generated IR names each accumulator from a single drain, so a
// second one means malformed IR - the accumulate side rejects a second
// nl.collect_update the same way.
void throwIfAlreadyDrained(const NLCollectState* state) {
    const std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();
    const bool isDrained = !valueColumns.empty() && valueColumns.front()._output;

    if (isDrained) {
        throw IRException("an nl.collect_buffer must be drained by a single nl.collect or nl.unwind_collect");
    }
}

}

void NLTranslator::translateCollectBuffer(nl::CollectBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the update (and
    // later the drain) that name the handle share the same group table and per-group
    // lists. The key buffers and the value buffer are allocated by the update, which
    // knows their types - the collect sibling of translateGroupAggregateBuffer.
    NLCollectState* state = _program->allocCollectState();
    _collectStates[buffer.getState()] = state;

    // The reset empties the group table and per-group lists each time the block
    // holding this nl.collect_buffer runs: once at function scope for a top-level
    // collect.
    NLCollectResetData* resetData = _program->allocFunctionData<NLCollectResetData>(state);
    body->emplaceStmt(&NLExecutor::runCollectReset, resetData);
}

void NLTranslator::translateCollectUpdate(nl::CollectUpdate update, NLStmtContainer* body) {
    const mlir::Value updateState = update.getState();
    NLCollectState* state = collectStateFor(updateState);

    // The keyCount lives on the nl.collect_buffer that produced the handle.
    nl::CollectBuffer buffer = updateState.getDefiningOp<nl::CollectBuffer>();
    if (!buffer) {
        throw IRException("collect_update state must come from nl.collect_buffer");
    }

    // The key buffers and the value buffers are allocated once, by the single update
    // feeding an accumulator. Generated IR has exactly one update per accumulator; a
    // second would append the same rows twice, so it is rejected here.
    NLGroupTable& groupTable = state->groupTable();
    if (!groupTable.keyColumns().empty() || !state->valueColumns().empty()) {
        throw IRException("an nl.collect_buffer must be fed by a single nl.collect_update");
    }

    const mlir::OperandRange columns = update.getColumns();
    const size_t keyCount = buffer.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = buffer.getKinds().value_or(llvm::ArrayRef<int64_t> {});

    // The collected columns are the grouping keys, the value columns, then one input per
    // aggregate reduced over the same groups: what the keys and the aggregates leave is
    // what the collect gathers, and it gathers at least one. Bound keyCount by the column
    // count before summing, as nl.collect_update's verifier does.
    const size_t columnCount = columns.size();
    if (keyCount >= columnCount || columnCount - keyCount <= kinds.size()) {
        throw IRException("collect collects at least one value column after the grouping keys, then one column per aggregate");
    }

    const size_t valueCount = columnCount - keyCount - kinds.size();

    const llvm::ArrayRef<int64_t> distinctValues = buffer.getDistinctValues().value_or(llvm::ArrayRef<int64_t> {});

    NLCollectUpdateData* data = _program->allocFunctionData<NLCollectUpdateData>(state);

    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        addGroupKeyColumn(groupTable, columns[keyIndex]);
    }

    groupTable.hashTable().chooseTable();

    // Each collected column is either a nullable value chunk (a property fetch), whose
    // present values accumulate in a flat buffer of the same primitive with nulls
    // dropped to match Cypher collect, or an entity chunk, whose every row carries an
    // ID. The fold (append) and the drain emit handlers are baked from that type here,
    // so whichever drain the query uses reads a ready handler off the state.
    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const mlir::Value column = columns[keyCount + valueIndex];
        const mlir::Type element = mlir::cast<nl::ChunkType>(column.getType()).getElementType();

        const bool isDistinct = llvm::is_contained(distinctValues, static_cast<int64_t>(valueIndex));

        NLCollectState::ValueColumn value;
        value._input = getColumn(column);

        if (isDistinct) {
            value._distinct = allocGroupDistinctTally(column.getType());
        }

        if (isNullableList(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectOptContainerHandlers<ListView>(isDistinct, fold, listEmit);

            value._buffer = allocListColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (isNullableMap(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectOptContainerHandlers<MapView>(isDistinct, fold, listEmit);

            value._buffer = allocColumnForKind(NLChunkKind::Map);
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::NullableType>(element)) {
            const NLChunkKind kind = getNullableChunkKind(column.getType());

            value._buffer = allocColumnForKind(kind);
            value._fold = isDistinct ? NLExecutor::selectCollectDistinctFold(kind)
                                     : NLExecutor::selectCollectFold(kind);
            value._unwindCollectEmit = NLExecutor::selectUnwindCollectValueEmit(kind);
            value._listEmit = NLExecutor::selectCollectListEmit(kind);
        } else if (mlir::isa<storage::ListType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectContainerHandlers<ListView>(isDistinct, fold, listEmit);

            value._buffer = allocListColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::MapType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectContainerHandlers<MapView>(isDistinct, fold, listEmit);

            value._buffer = allocColumnForKind(NLChunkKind::Map);
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::AnyType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectTaggedHandlers(isDistinct, fold, listEmit);

            value._buffer = allocCellColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::PathType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectPathHandlers(isDistinct, fold, listEmit);

            value._buffer = allocCellColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else {
            const NLChunkKind kind = getChunkKind(column.getType());

            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectEntityHandlers(kind, isDistinct, fold, listEmit);

            value._buffer = allocColumnForKind(kind);
            value._fold = fold;
            value._listEmit = listEmit;
        }

        state->addValueColumn(value);
    }

    // The reductions taken beside the list read the same groups, so their states
    // live on this state and are wired exactly as a grouped aggregation's are.
    NLGroupAggregates& aggregates = state->aggregates();

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const storage::GroupAggregateKind aggregateKind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);

        NLGroupAggregates::Aggregate aggregate;
        buildGroupAggregate(aggregateKind, columns[keyCount + valueCount + aggregateIndex], aggregate);

        aggregates.add(aggregate);
    }

    body->emplaceStmt(&NLExecutor::runCollectUpdate, data);
}

NLCollectState* NLTranslator::collectStateFor(mlir::Value handle) const {
    const auto stateIt = _collectStates.find(handle);
    if (stateIt == _collectStates.end()) {
        throw IRException("collect handle must be produced by an nl.collect_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateUnwindCollectLoop(const IteratorConfig& config,
                                       mlir::Block& loopBody,
                                       NLStmtContainer* body) {
    NLCollectState* state = config._collectState;
    if (!state) {
        throw IRException("nl.unwind_collect iterator must carry a collect accumulator");
    }

    throwIfAlreadyDrained(state);

    // Only a value collect bakes an unwind emit handler; an entity collect leaves it
    // unset, so its list can be returned but not yet unwound element by element.
    NLCollectState::ValueColumn& unwound = state->unwoundColumn();
    if (!unwound._unwindCollectEmit) {
        throw IRException("unwinding a collected entity list is not supported");
    }

    // For::verify binds one loop variable per iterator chunk; the unwind iterator's
    // chunks are the grouping keys then the element value, so the loop takes one
    // variable per grouping key plus one for the value.
    std::vector<NLGroupTable::KeyColumn>& keyColumns = state->groupTable().keyColumns();
    const size_t keyCount = keyColumns.size();
    if (loopBody.getNumArguments() != keyCount + 1) {
        throw IRException("nl.unwind_collect loop must bind one variable per grouping key plus the element");
    }

    NLUnwindCollectLoopData* loopData = _program->allocFunctionData<NLUnwindCollectLoopData>(state);
    loopData->getGroupIndices()->reserve(_program->getChunkSize());
    loopData->getPositions()->reserve(_program->getChunkSize());

    // The first keyCount loop variables are the grouping keys (filled by gathering
    // their key buffers, one row per emitted element); wire each into its key column's
    // output slot on the shared state.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        keyColumns[keyIndex]._output = output;
    }

    // The last loop variable is the unwound element value (a nullable value chunk).
    const mlir::Value valueVariable = loopBody.getArgument(static_cast<unsigned>(keyCount));
    Column* valueOutput = allocColumnForChunkType(valueVariable.getType());
    _valueSlots[valueVariable] = valueOutput;
    unwound._output = valueOutput;

    body->emplaceStmt(&NLExecutor::runUnwindCollectLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateCollectLoop(const IteratorConfig& config,
                                        mlir::Block& loopBody,
                                        NLStmtContainer* body) {
    NLCollectState* state = config._collectState;
    if (!state) {
        throw IRException("nl.collect iterator must carry a collect accumulator");
    }

    throwIfAlreadyDrained(state);

    // The collect iterator's chunks are the grouping keys, one list cell per collected
    // column, then one per aggregate reduced over the same groups.
    std::vector<NLGroupTable::KeyColumn>& keyColumns = state->groupTable().keyColumns();
    const size_t keyCount = keyColumns.size();
    std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();
    const std::span<NLGroupAggregates::Aggregate> aggregates = state->aggregates().getAggregates();
    if (loopBody.getNumArguments() != keyCount + valueColumns.size() + aggregates.size()) {
        throw IRException("nl.collect loop must bind one variable per grouping key, one per list, and one per aggregate");
    }

    NLCollectLoopData* loopData = _program->allocFunctionData<NLCollectLoopData>(state, _program->allocStepBuffers());

    // The first keyCount loop variables are the grouping keys (filled by slicing their
    // key buffers, one row per group); wire each into its key column's output slot.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        keyColumns[keyIndex]._output = output;
    }

    // Then one per-group list cell per collected column (a ColumnVector<ListView>).
    for (size_t valueIndex = 0; valueIndex < valueColumns.size(); valueIndex++) {
        const mlir::Value listVariable = loopBody.getArgument(static_cast<unsigned>(keyCount + valueIndex));

        Column* listOutput = allocColumnForChunkType(listVariable.getType());
        _valueSlots[listVariable] = listOutput;
        valueColumns[valueIndex]._output = listOutput;
    }

    // The remaining variables take the reductions, one per aggregate, filled from the
    // same group window the lists are sliced over.
    for (size_t aggregateIndex = 0; aggregateIndex < aggregates.size(); aggregateIndex++) {
        const unsigned argumentIndex = static_cast<unsigned>(keyCount + valueColumns.size() + aggregateIndex);
        const mlir::Value loopVariable = loopBody.getArgument(argumentIndex);

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        aggregates[aggregateIndex]._output = output;
    }

    body->emplaceStmt(&NLExecutor::runCollectLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}
