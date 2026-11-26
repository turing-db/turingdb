#include "NLTranslator.h"

#include <vector>

#include "NLOps.h"

#include "mlir/IR/Block.h"
#include "llvm/ADT/DenseMap.h"

#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;

void NLTranslator::translateOptionalBuffer(nl::OptionalBuffer buffer, NLStmtContainer* body) {
    NLOptionalState* state = _program->allocOptionalState();
    _optionalStates[buffer.getState()] = state;

    // This step's own chunks: the drain rebuilds the rows the pattern missed out of them,
    // and their row count is how many matched flags the reset clears.
    for (const mlir::Value column : buffer.getInputColumns()) {
        state->addInputColumn(getColumn(column));
    }

    ColumnVector<uint64_t>* tag = static_cast<ColumnVector<uint64_t>*>(allocColumn(buffer.getTag()));

    NLOptionalResetData* resetData = _program->allocFunctionData<NLOptionalResetData>(state, tag);
    body->emplaceStmt(&NLExecutor::runOptionalReset, resetData);
}

void NLTranslator::translateOptionalCollect(nl::OptionalCollect collect, NLStmtContainer* body) {
    NLOptionalState* state = optionalStateFor(collect.getState());

    // The buffers are allocated once, by the single collect feeding an accumulator.
    // Generated IR has exactly one collect per accumulator; a second would append to the
    // same buffers and double the rows, so it is rejected here as a sort's is.
    if (!state->buffers().empty()) {
        throw IRException("an nl.optional_buffer must be fed by a single nl.optional_collect");
    }

    NLOptionalCollectData* data = _program->allocFunctionData<NLOptionalCollectData>(state);

    if (const mlir::Value tag = collect.getTag()) {
        data->setTag(static_cast<const ColumnVector<uint64_t>*>(getColumn(tag)));
    }

    // One growing buffer per column the pattern contributes, row-aligned. The buffer keeps
    // the column's element type; the append copies a chunk's rows onto its tail.
    for (const mlir::Value column : collect.getColumns()) {
        Column* bufferColumn = allocColumnForChunkType(column.getType());
        state->addMatchedBuffer(bufferColumn);

        const NLOwnedAppendFunction appendOwned = NLExecutor::selectOwnedAppendFunction(bufferColumn);
        const NLAppendFunction append = appendOwned ? nullptr : selectAppendForChunkType(column.getType());

        data->addAppend(NLSortCollectData::Append {getColumn(column),
                                                   bufferColumn,
                                                   append,
                                                   appendOwned});
    }

    body->emplaceStmt(&NLExecutor::runOptionalCollect, data);
}

void NLTranslator::translateOptionalDrainLoop(const IteratorConfig& config,
                                              mlir::Block& loopBody,
                                              NLLimitState* limit,
                                              NLStmtContainer* body) {
    NLOptionalState* state = config._optionalState;
    if (!state) {
        throw IRException("nl.optional_drain iterator must carry an optional accumulator");
    }

    // For::verify binds one loop variable per iterator chunk, and the drain iterator's
    // chunk types are the collected column types, so the loop must take one per buffer.
    const size_t bufferCount = state->buffers().size();
    if (loopBody.getNumArguments() != bufferCount) {
        throw IRException("nl.optional_drain loop must bind one variable per collected column");
    }

    NLOptionalDrainLoopData* loopData = _program->allocFunctionData<NLOptionalDrainLoopData>(state);
    loopData->setLimit(limit);

    // Reserve the row-slice scratch so the per-step gather stays allocation-free, the same
    // as the edge loop's indices column.
    loopData->getIndices()->reserve(_program->getChunkSize());

    // The columns the pattern joined onto come first, in the order it yielded them back,
    // so the first inputColumns().size() of them are the ones a missed row is rebuilt from.
    const std::vector<const Column*>& inputColumns = state->inputColumns();

    for (size_t columnIndex = 0; columnIndex < bufferCount; columnIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(columnIndex));
        const mlir::Type chunkType = loopVariable.getType();

        Column* output = allocColumnForChunkType(chunkType);
        _valueSlots[loopVariable] = output;

        const bool joinedOnto = columnIndex < inputColumns.size();

        NLOptionalDrainLoopData::DrainColumn column;
        column._buffer = state->buffers()[columnIndex];
        column._input = joinedOnto ? inputColumns[columnIndex] : nullptr;
        column._output = output;
        column._gather = selectGatherForChunkType(chunkType);
        column._fillNull = joinedOnto ? nullptr : selectFillNullForChunkType(chunkType);

        loopData->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runOptionalDrainLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateExistsBuffer(nl::ExistsBuffer buffer, NLStmtContainer* body) {
    NLExistsState* state = _program->allocExistsState();
    _existsStates[buffer.getState()] = state;

    translateSubqueryExpressionReset(state, buffer.getInputColumns(), buffer.getTag(), body);
}

void NLTranslator::translateExistsMark(nl::ExistsMark mark, NLStmtContainer* body) {
    NLExistsState* state = existsStateFor(mark.getState());

    NLExistsMarkData* data = _program->allocFunctionData<NLExistsMarkData>(state);

    if (const mlir::Value tag = mark.getTag()) {
        data->setTag(static_cast<const ColumnVector<uint64_t>*>(getColumn(tag)));
    }

    if (const mlir::Value row = mark.getRow()) {
        data->setRow(static_cast<const ColumnVector<uint64_t>*>(getColumn(row)));
    }

    for (const mlir::Value column : mark.getColumns()) {
        data->addColumn(getColumn(column));
    }

    body->emplaceStmt(&NLExecutor::runExistsMark, data);
}

void NLTranslator::translateExistsResult(nl::ExistsResult result, NLStmtContainer* body) {
    NLExistsState* state = existsStateFor(result.getState());

    ColumnVector<CustomBool>* answer = static_cast<ColumnVector<CustomBool>*>(allocColumn(result.getResult()));

    NLExistsResultData* data = _program->allocFunctionData<NLExistsResultData>(state, answer);
    body->emplaceStmt(&NLExecutor::runExistsResult, data);
}

NLExistsState* NLTranslator::existsStateFor(mlir::Value handle) const {
    const auto stateIt = _existsStates.find(handle);
    if (stateIt == _existsStates.end()) {
        throw IRException("exists handle must be produced by an nl.exists_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateCountSubqueryBuffer(nl::CountSubqueryBuffer buffer, NLStmtContainer* body) {
    NLCountSubqueryState* state = _program->allocCountSubqueryState();
    _countSubqueryStates[buffer.getState()] = state;

    translateSubqueryExpressionReset(state, buffer.getInputColumns(), buffer.getTag(), body);
}

void NLTranslator::translateSubqueryExpressionReset(NLSubqueryExpressionState* state,
                                                    mlir::OperandRange inputColumns,
                                                    mlir::Value tag,
                                                    NLStmtContainer* body) {
    // This step's own chunks: their row count is how many answers the reset clears, and how
    // many the result lays out.
    for (const mlir::Value column : inputColumns) {
        state->addInputColumn(getColumn(column));
    }

    ColumnVector<uint64_t>* tagColumn = static_cast<ColumnVector<uint64_t>*>(allocColumn(tag));

    NLSubqueryExpressionResetData* resetData = _program->allocFunctionData<NLSubqueryExpressionResetData>(state, tagColumn);
    body->emplaceStmt(&NLExecutor::runSubqueryExpressionReset, resetData);
}

void NLTranslator::translateCountSubqueryTally(nl::CountSubqueryTally tally, NLStmtContainer* body) {
    NLCountSubqueryState* state = countSubqueryStateFor(tally.getState());

    const ColumnVector<uint64_t>* tag = nullptr;
    if (const mlir::Value tagValue = tally.getTag()) {
        tag = static_cast<const ColumnVector<uint64_t>*>(getColumn(tagValue));
    }

    const ColumnVector<uint64_t>* row = nullptr;
    if (const mlir::Value rowValue = tally.getRow()) {
        row = static_cast<const ColumnVector<uint64_t>*>(getColumn(rowValue));
    }

    const Column* rows = nullptr;
    if (const mlir::Value rowsValue = tally.getRows()) {
        rows = getColumn(rowsValue);
    }

    NLCountSubqueryTallyData* data = _program->allocFunctionData<NLCountSubqueryTallyData>(state, tag, row, rows);
    body->emplaceStmt(&NLExecutor::runCountSubqueryTally, data);
}

void NLTranslator::translateCountSubqueryResult(nl::CountSubqueryResult result, NLStmtContainer* body) {
    NLCountSubqueryState* state = countSubqueryStateFor(result.getState());

    ColumnVector<uint64_t>* counts = static_cast<ColumnVector<uint64_t>*>(allocColumn(result.getResult()));

    NLCountSubqueryResultData* data = _program->allocFunctionData<NLCountSubqueryResultData>(state, counts);
    body->emplaceStmt(&NLExecutor::runCountSubqueryResult, data);
}

NLCountSubqueryState* NLTranslator::countSubqueryStateFor(mlir::Value handle) const {
    const auto stateIt = _countSubqueryStates.find(handle);
    if (stateIt == _countSubqueryStates.end()) {
        throw IRException("count subquery handle must be produced by an nl.count_subquery_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateRowListsBuffer(nl::RowListsBuffer buffer, NLStmtContainer* body) {
    NLRowListsState* state = _program->allocRowListsState();
    _rowListsStates[buffer.getState()] = state;

    const mlir::Value cardinality = buffer.getCardinality();
    const Column* const rows = cardinality ? getColumn(cardinality) : nullptr;

    ColumnVector<uint64_t>* tag = static_cast<ColumnVector<uint64_t>*>(allocColumn(buffer.getTag()));

    NLRowListsResetData* resetData =
        _program->allocFunctionData<NLRowListsResetData>(state, rows, tag);
    body->emplaceStmt(&NLExecutor::runRowListsReset, resetData);
}

void NLTranslator::translateRowListsCollect(nl::RowListsCollect collect, NLStmtContainer* body) {
    NLRowListsState* state = rowListsStateFor(collect.getState());

    const mlir::Value value = collect.getValue();
    const mlir::Value tag = collect.getTag();
    const mlir::Value row = collect.getRow();

    const ColumnVector<uint64_t>* tagColumn = nullptr;
    if (tag) {
        tagColumn = static_cast<const ColumnVector<uint64_t>*>(getColumn(tag));
    }

    const ColumnVector<uint64_t>* rowColumn = nullptr;
    if (row) {
        rowColumn = static_cast<const ColumnVector<uint64_t>*>(getColumn(row));
    }

    NLRowListsCollectData* data =
        _program->allocFunctionData<NLRowListsCollectData>(state,
                                                           tagColumn,
                                                           rowColumn,
                                                           getColumn(value),
                                                           selectListItemRead(value.getType()),
                                                           _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runRowListsCollect, data);
}

void NLTranslator::translateRowListsResult(nl::RowListsResult result, NLStmtContainer* body) {
    NLRowListsState* state = rowListsStateFor(result.getState());

    const mlir::Value resultValue = result.getResult();
    Column* const lists = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = lists;

    NLRowListsResultData* data =
        _program->allocFunctionData<NLRowListsResultData>(state,
                                                          static_cast<ColumnVector<ListView>*>(lists),
                                                          _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runRowListsResult, data);
}

NLRowListsState* NLTranslator::rowListsStateFor(mlir::Value handle) const {
    const auto stateIt = _rowListsStates.find(handle);
    if (stateIt == _rowListsStates.end()) {
        throw IRException("row lists handle must be produced by an nl.row_lists_buffer");
    }

    return stateIt->second;
}

NLOptionalState* NLTranslator::optionalStateFor(mlir::Value handle) const {
    const auto stateIt = _optionalStates.find(handle);
    if (stateIt == _optionalStates.end()) {
        throw IRException("optional handle must be produced by an nl.optional_buffer");
    }

    return stateIt->second;
}
