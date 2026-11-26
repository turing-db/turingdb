#include "NLTranslator.h"

#include <optional>
#include <vector>
#include <span>
#include <string_view>

#include "NLOps.h"

#include "IRConstantColumn.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "list/ListView.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// A chunk holding the single row a reduction collapsed the whole relation to, or a
// computation over such rows and constants: like a constant, it holds one value for
// every row of the step that reads it, whichever loop that step belongs to.
bool yieldsReducedRowChunk(mlir::Value column) {
    llvm::DenseMap<mlir::Value, bool> classified;
    llvm::DenseMap<mlir::Value, bool> constants;
    llvm::SmallVector<mlir::Value> worklist {column};

    while (!worklist.empty()) {
        const mlir::Value value = worklist.back();
        if (classified.contains(value)) {
            worklist.pop_back();
            continue;
        }

        mlir::Operation* const definingOp = value.getDefiningOp();
        const bool isReduction = definingOp && mlir::isa<nl::CountResult, nl::CountScanRows, nl::AggregateResult>(definingOp);
        const bool dependsOnOperands = definingOp
                                    && !isReduction
                                    && definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>();

        if (!dependsOnOperands) {
            classified[value] = isReduction;
            worklist.pop_back();
            continue;
        }

        bool operandsPending = false;
        for (const mlir::Value operand : definingOp->getOperands()) {
            if (!classified.contains(operand)) {
                worklist.push_back(operand);
                operandsPending = true;
            }
        }

        if (operandsPending) {
            continue;
        }

        bool readsAReducedRow = false;
        bool everyOperandStandsForEveryRow = true;

        for (const mlir::Value operand : definingOp->getOperands()) {
            const bool operandIsReducedRow = classified.at(operand);
            const bool operandStandsForEveryRow = operandIsReducedRow || yieldsConstantColumn(operand, constants);

            readsAReducedRow = readsAReducedRow || operandIsReducedRow;
            everyOperandStandsForEveryRow = everyOperandStandsForEveryRow && operandStandsForEveryRow;
        }

        classified[value] = readsAReducedRow && everyOperandStandsForEveryRow;
        worklist.pop_back();
    }

    return classified.at(column);
}

}

void NLTranslator::translateFilter(nl::Filter filter, NLStmtContainer* body) {
    const mlir::Value maskValue = filter.getMask();
    const Column* mask = getColumn(maskValue);
    const NLMaskSurvivorFunction filterFunction = NLExecutor::selectMaskSurvivorFunction(truthColumnKindOf(maskValue.getType(), mask));

    NLFilterData* data = _program->allocFunctionData<NLFilterData>(mask, filterFunction);

    // Reserve to avoid execution time allocations
    data->getIndices()->reserve(_program->getChunkSize());

    // New column for each result output, same types
    const mlir::OperandRange columns = filter.getColumns();
    const mlir::ResultRange results = filter.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value colVal = columns[columnIndex];
        const mlir::Type colType = colVal.getType();

        const Column* input = getColumn(colVal);
        Column* output = allocColumnForChunkType(colType);
        _valueSlots[results[columnIndex]] = output;

        const NLGatherFunction gatherType = selectGatherForChunkType(colType);

        const NLFilterData::FilterColumn column {input, output, gatherType};

        data->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runFilter, data);
}

void NLTranslator::translateOutput(nl::Output output, NLStmtContainer* body) {
    // The optional limit handle is a separate operand, so read just the columns;
    // including it in this list would treat the handle as a chunk.
    const mlir::OperandRange columns = output.getColumns();
    if (columns.empty()) {
        throw IRException("nl.output requires at least one column");
    }

    // Output is either the per-step sink inside an nl.for body, or - for an
    // aggregate like COUNT that collapses to one row - a one-shot emit at function
    // scope. Either way its per-row columns must be bound in the output's own block,
    // which the per-column check below enforces: a loop variable of this loop, or a
    // chunk materialized in this block (a property fetch, or nl.count_result at
    // function scope). Two chunks cross that scope: a constant, and the single row a
    // reduction collapsed to. Any other chunk from an outer or sibling loop fails the
    // check.
    mlir::Block* outputBlock = output->getBlock();

    // A constant broadcasts to any step, since reading it ignores the row. A reduced
    // row is one row and nothing here spreads it over more, so it crosses only into a
    // step that is that single row.
    const bool singleRowStep = stepKeepsASingleRow(outputBlock);

    NLOutputData* outputData = _program->allocFunctionData<NLOutputData>();

    outputData->setLimit(limitStateFor(output.getLimit()));
    outputData->setSkip(skipStateFor(output.getSkip()));

    if (const mlir::Value cardinality = output.getCardinality()) {
        outputData->setCardinality(getColumn(cardinality));
    }

    for (const mlir::Value column : columns) {
        const auto columnArgument = mlir::dyn_cast<mlir::BlockArgument>(column);
        const bool isInnermostLoopVariable = columnArgument && columnArgument.getOwner() == outputBlock;

        // A property fetch result is not a loop variable but an op result
        // produced in this same loop body; it is equally available to output.
        mlir::Operation* definingOp = column.getDefiningOp();
        const bool isProducedInThisBlock = definingOp && definingOp->getBlock() == outputBlock;

        const bool isConstant = yieldsConstantColumn(column);
        const bool isReducedRow = singleRowStep && yieldsReducedRowChunk(column);

        if (!isInnermostLoopVariable && !isProducedInThisBlock && !isConstant && !isReducedRow) {
            throw IRException("nl.output columns must be a loop variable of the enclosing "
                              "nl.for, produced in this block, a constant, or a reduced row");
        }

        const bool carriesRows = !isConstant && !isReducedRow;
        const auto chunk = mlir::cast<nl::ChunkType>(column.getType());
        if (mlir::isa<storage::PathRefType>(chunk.getElementType())) {
            const ColumnVector<PathRef>* paths = static_cast<const ColumnVector<PathRef>*>(getColumn(column));
            ColumnVector<ListView>* lists = static_cast<ColumnVector<ListView>*>(allocListColumn());
            outputData->addExpandedPathColumn(paths, lists, carriesRows);
            outputData->setTrie(&_memory->pathTrie());
        } else {
            outputData->addOutputColumn(getColumn(column), carriesRows);
        }
    }

    _program->setOutputData(outputData);

    // The names label the result, not one emission of it, so they go on the program
    // rather than the per-step output data the limit and skip handles sit on.
    if (const mlir::ArrayAttr columnNames = output.getColumnNamesAttr()) {
        llvm::SmallVector<std::string_view> names;
        for (const mlir::Attribute name : columnNames) {
            const llvm::StringRef nameText = mlir::cast<mlir::StringAttr>(name).getValue();
            names.emplace_back(nameText.data(), nameText.size());
        }

        _program->setColumnNames(names);
    }

    body->emplaceStmt(&NLExecutor::runOutput, outputData);
}

bool NLTranslator::stepKeepsASingleRow(mlir::Block* block) const {
    // A cross product pairs what the loop bound with every row of another factor, so the
    // block holds that product rather than the one row the loop stands for.
    const bool crossesAnotherFactor = !block->getOps<nl::CrossProduct>().empty();
    if (crossesAnotherFactor) {
        return false;
    }

    nl::For forLoop = mlir::dyn_cast<nl::For>(block->getParentOp());
    if (!forLoop) {
        return true;
    }

    const auto configIt = _iteratorConfigs.find(forLoop.getIterator());
    if (configIt == _iteratorConfigs.end()) {
        return false;
    }

    const IteratorConfig& config = configIt->second;
    const bool drainsACollect = config._kind == IteratorKind::Collect;

    return drainsACollect && config._collectState->groupTable().keyColumns().empty();
}

void NLTranslator::translateLimit(nl::Limit limit, NLStmtContainer* body) {
    // Allocate the runtime counter and map the handle to it, so the loops, the
    // update and the output that name the handle all find the same counter.
    NLLimitState* state = _program->allocLimitState();
    _limitStates[limit.getState()] = state;

    // The reset runs each time the block holding this nl.limit runs: once at
    // function scope for a top-level LIMIT, per enclosing step for a nested one.
    const size_t count = limit.getCount().value_or(0);
    const mlir::Value boundCount = limit.getBoundCount();
    const Column* boundColumn = boundCount ? getColumn(boundCount) : nullptr;
    NLLimitInitData* initData = _program->allocFunctionData<NLLimitInitData>(state, count, boundColumn);

    body->emplaceStmt(&NLExecutor::runLimitInit, initData);
}

void NLTranslator::translateLimitRemaining(nl::LimitRemaining remaining, NLStmtContainer* body) {
    NLLimitState* state = _program->allocLimitState();
    _limitStates[remaining.getState()] = state;

    const NLLimitState* source = limitStateFor(remaining.getSource());
    NLLimitRemainingData* remainingData = _program->allocFunctionData<NLLimitRemainingData>(state, source);

    body->emplaceStmt(&NLExecutor::runLimitRemaining, remainingData);
}

void NLTranslator::translateLimitUpdate(nl::LimitUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so limitStateFor returns its counter or
    // throws if it was not produced by an nl.limit.
    NLLimitState* state = limitStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLLimitUpdateData* updateData = _program->allocFunctionData<NLLimitUpdateData>(state, representative);

    body->emplaceStmt(&NLExecutor::runLimitUpdate, updateData);
}

void NLTranslator::translateLimitTruncate(nl::LimitTruncate truncate, NLStmtContainer* body) {
    // The handle is a required operand, so limitStateFor returns its counter or
    // throws if it was not produced by an nl.limit.
    NLLimitState* state = limitStateFor(truncate.getState());

    NLLimitTruncateData* data = _program->allocFunctionData<NLLimitTruncateData>(state);

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = truncate.getColumns();
    const mlir::ResultRange results = truncate.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addTruncateColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runLimitTruncate, data);
}

void NLTranslator::addTruncateColumn(mlir::Value inputValue,
                                     mlir::Value resultValue,
                                     NLLimitTruncateData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type; only the row count is cut. The
    // copy is a block-repeat with factor 1 (each input row once, stopping at
    // emitThisStep), so it reuses the block-repeat broadcast families the cross
    // product uses.
    Column* output = nullptr;
    NLBroadcastFunction copyPrefix = nullptr;

    if (isConstantColumn(input)) {
        output = _memory->allocSame(input);
        copyPrefix = NLExecutor::selectConstBlockRepeatFunction();
    } else if (isNullableList(elementType)) {
        output = allocOptListColumn();
        copyPrefix = NLExecutor::selectOptListBlockRepeatFunction();
    } else if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const NLChunkKind kind = chunkKindFromNullableValue(nullableType.getValueType());
        output = allocOptColumnForKind(kind);
        copyPrefix = NLExecutor::selectOptBlockRepeatFunction(kind);
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        output = allocCellColumn();
        copyPrefix = NLExecutor::selectCellBlockRepeatFunction();
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        copyPrefix = NLExecutor::selectBlockRepeatFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLCrossColumn truncateColumn(input, output, copyPrefix);
    data->addColumn(truncateColumn);
}

NLLimitState* NLTranslator::limitStateFor(mlir::Value handle) const {
    if (!handle) {
        return nullptr;
    }

    const auto stateIt = _limitStates.find(handle);
    if (stateIt == _limitStates.end()) {
        throw IRException("limit handle must be produced by an nl.limit");
    }

    return stateIt->second;
}

void NLTranslator::translateSkip(nl::Skip skip, NLStmtContainer* body) {
    // Allocate the runtime counter and map the handle to it, so the update and the
    // truncate that name the handle both find the same counter.
    NLSkipState* state = _program->allocSkipState();
    _skipStates[skip.getState()] = state;

    // The reset runs each time the block holding this nl.skip runs: once at
    // function scope for a top-level SKIP.
    const size_t count = skip.getCount().value_or(0);
    const mlir::Value boundCount = skip.getBoundCount();
    const Column* boundColumn = boundCount ? getColumn(boundCount) : nullptr;
    NLSkipInitData* initData = _program->allocFunctionData<NLSkipInitData>(state, count, boundColumn);

    body->emplaceStmt(&NLExecutor::runSkipInit, initData);
}

void NLTranslator::translateSkipUpdate(nl::SkipUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so skipStateFor returns its counter or
    // throws if it was not produced by an nl.skip.
    NLSkipState* state = skipStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLSkipUpdateData* updateData = _program->allocFunctionData<NLSkipUpdateData>(state, representative);

    body->emplaceStmt(&NLExecutor::runSkipUpdate, updateData);
}

void NLTranslator::translateSkipTruncate(nl::SkipTruncate truncate, NLStmtContainer* body) {
    // The handle is a required operand, so skipStateFor returns its counter or
    // throws if it was not produced by an nl.skip.
    NLSkipState* state = skipStateFor(truncate.getState());

    NLSkipTruncateData* data = _program->allocFunctionData<NLSkipTruncateData>(state);

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = truncate.getColumns();
    const mlir::ResultRange results = truncate.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addSkipColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runSkipTruncate, data);
}

void NLTranslator::addSkipColumn(mlir::Value inputValue,
                                 mlir::Value resultValue,
                                 NLSkipTruncateData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type; only the dropped prefix is
    // removed. The copy is a range copy of the surviving suffix to the front of a
    // fresh chunk, so it uses the copy families.
    Column* output = nullptr;
    NLCopyFunction copySuffix = nullptr;

    if (isConstantColumn(input)) {
        output = _memory->allocSame(input);
        copySuffix = NLExecutor::selectConstCopyFunction();
    } else if (isNullableList(elementType)) {
        output = allocOptListColumn();
        copySuffix = NLExecutor::selectOptListCopyFunction();
    } else if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const NLChunkKind kind = chunkKindFromNullableValue(nullableType.getValueType());
        output = allocOptColumnForKind(kind);
        copySuffix = NLExecutor::selectOptCopyFunction(kind);
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        output = allocCellColumn();
        copySuffix = NLExecutor::selectCellCopyFunction();
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        copySuffix = NLExecutor::selectCopyFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLSkipColumn skipColumn(input, output, copySuffix);
    data->addColumn(skipColumn);
}

NLSkipState* NLTranslator::skipStateFor(mlir::Value handle) const {
    if (!handle) {
        return nullptr;
    }

    const auto stateIt = _skipStates.find(handle);
    if (stateIt == _skipStates.end()) {
        throw IRException("skip handle must be produced by an nl.skip");
    }

    return stateIt->second;
}

void NLTranslator::translateSortBuffer(nl::SortBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the collect
    // and the emit loop that name the handle share the same buffers. The buffer
    // columns themselves are allocated by the collect, which knows their types.
    NLSortState* state = _program->allocSortState();
    _sortStates[buffer.getState()] = state;

    // A fused ORDER BY ... LIMIT carries the bound as the top-K count, so the
    // accumulator keeps only the best k rows. Absent, it keeps every row.
    if (const std::optional<uint64_t> topK = buffer.getTopK()) {
        state->setTopK(*topK);
    }

    // The reset empties the buffers each time the block holding this nl.sort_buffer
    // runs: once at function scope for a top-level ORDER BY.
    const mlir::Value boundTopK = buffer.getBoundTopK();
    const Column* boundColumn = boundTopK ? getColumn(boundTopK) : nullptr;
    NLSortResetData* resetData = _program->allocFunctionData<NLSortResetData>(state, boundColumn);
    body->emplaceStmt(&NLExecutor::runSortReset, resetData);
}

void NLTranslator::translateSortCollect(nl::SortCollect collect, NLStmtContainer* body) {
    const mlir::Value collectState = collect.getState();
    NLSortState* state = sortStateFor(collectState);

    // The sort spec lives on the nl.sort_buffer that produced the handle.
    nl::SortBuffer buffer = collectState.getDefiningOp<nl::SortBuffer>();
    if (!buffer) {
        throw IRException("sort_collect state must come from nl.sort_buffer");
    }

    // The buffers are allocated once, by the single collect feeding a buffer.
    // Generated IR has exactly one collect per accumulator; a second would append
    // to the same buffers and double the rows, so it is rejected here.
    if (!state->buffers().empty()) {
        throw IRException("an nl.sort_buffer must be fed by a single nl.sort_collect");
    }

    NLSortCollectData* data = _program->allocFunctionData<NLSortCollectData>(state);

    // One growing buffer per collected column, row-aligned. The buffer keeps the
    // column's element type; the append copies a chunk's rows onto its tail. A
    // bounded accumulator also gets a compaction scratch column and the gather
    // that fills it, so the trim can drop all but the best k rows.
    const bool bounded = buffer.getTopK().has_value() || static_cast<bool>(buffer.getBoundTopK());
    const mlir::OperandRange columns = collect.getColumns();
    for (const mlir::Value column : columns) {
        Column* bufferColumn = allocColumnForChunkType(column.getType());

        Column* tempColumn = bounded ? allocColumnForChunkType(column.getType()) : nullptr;
        const NLGatherFunction gather = bounded ? selectGatherForChunkType(column.getType()) : nullptr;
        state->addColumnBuffer(bufferColumn, tempColumn, gather);

        const mlir::Type columnType = column.getType();
        const NLOwnedAppendFunction appendOwned = NLExecutor::selectOwnedAppendFunction(bufferColumn);
        const NLAppendFunction append = appendOwned ? nullptr : selectAppendForChunkType(columnType);

        data->addAppend(NLSortCollectData::Append {getColumn(column),
                                                   bufferColumn,
                                                   append,
                                                   appendOwned});
    }

    // Build the comparators from the spec, most significant key first. Each key
    // indexes a collected column, so the index must be in range, and the column's
    // element type must have an order (an embedding key is rejected by the
    // selector). The buffer column is the one the comparator reads at run time.
    const llvm::ArrayRef<int64_t> keyColumns = buffer.getKeyColumns();
    const llvm::ArrayRef<bool> keyAscending = buffer.getKeyAscending();
    for (size_t keyIndex = 0; keyIndex < keyColumns.size(); keyIndex++) {
        const int64_t keyColumn = keyColumns[keyIndex];
        const bool inRange = keyColumn >= 0 && static_cast<size_t>(keyColumn) < columns.size();
        if (!inRange) {
            throw IRException("sort key column index is out of range");
        }

        const mlir::Type keyType = columns[static_cast<size_t>(keyColumn)].getType();
        const NLSortState::Key key {state->buffer(static_cast<size_t>(keyColumn)),
                                    keyAscending[keyIndex],
                                    selectCompareForChunkType(keyType)};
        state->addKey(key);
    }

    body->emplaceStmt(&NLExecutor::runSortCollect, data);
}

void NLTranslator::translateSortLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
                                     NLLimitState* limit,
                                     NLStmtContainer* body) {
    NLSortState* state = config._sortState;
    if (!state) {
        throw IRException("nl.sort iterator must carry a sort accumulator");
    }

    // For::verify binds one loop variable per iterator chunk, and the sort
    // iterator's chunk types are the collected column types, so the loop must
    // take exactly one variable per buffer.
    const size_t bufferCount = state->buffers().size();
    if (loopBody.getNumArguments() != bufferCount) {
        throw IRException("nl.sort loop must bind one variable per collected column");
    }

    NLSortLoopData* loopData = _program->allocFunctionData<NLSortLoopData>(state);
    loopData->setLimit(limit);

    // Reserve the permutation-slice scratch so the per-step gather stays
    // allocation-free, the same as the edge loop's indices column.
    loopData->getIndices()->reserve(_program->getChunkSize());

    // Each loop variable is filled from its buffer by gathering the rows the
    // current emit step covers, in permutation order. The buffer is the read
    // side, the loop variable the write side - the NLCarriedColumn (input,
    // output, gather) shape the edge loops already use.
    for (size_t columnIndex = 0; columnIndex < bufferCount; columnIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(columnIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;

        const NLCarriedColumn column(state->buffer(columnIndex),
                                     output,
                                     selectGatherForChunkType(loopVariable.getType()));
        loopData->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runSortLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateRowBuffer(nl::RowBuffer buffer, NLStmtContainer* body) {
    NLRowState* state = _program->allocRowState();
    _rowStates[buffer.getState()] = state;

    NLRowResetData* resetData = _program->allocFunctionData<NLRowResetData>(state);
    body->emplaceStmt(&NLExecutor::runRowReset, resetData);
}

void NLTranslator::translateRowCollect(nl::RowCollect collect, NLStmtContainer* body) {
    NLRowState* state = rowStateFor(collect.getState());
    const mlir::OperandRange columns = collect.getColumns();

    if (state->buffers().empty()) {
        for (const mlir::Value column : columns) {
            state->addBuffer(allocColumnForChunkType(column.getType()));
        }
    } else if (state->buffers().size() != columns.size()) {
        throw IRException("every nl.row_collect of an accumulator must append the same number of columns");
    }

    NLRowCollectData* data = _program->allocFunctionData<NLRowCollectData>(state);

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value column = columns[columnIndex];
        const mlir::Type columnType = column.getType();

        Column* bufferColumn = state->buffer(columnIndex);

        const NLOwnedAppendFunction appendOwned = NLExecutor::selectOwnedAppendFunction(bufferColumn);
        const NLAppendFunction append = appendOwned ? nullptr : selectAppendForChunkType(columnType);

        data->addAppend(NLSortCollectData::Append {getColumn(column),
                                                   bufferColumn,
                                                   append,
                                                   appendOwned});
    }

    body->emplaceStmt(&NLExecutor::runRowCollect, data);
}

void NLTranslator::translateRowLoop(const IteratorConfig& config,
                                    mlir::Block& loopBody,
                                    NLLimitState* limit,
                                    NLStmtContainer* body) {
    NLRowState* state = config._rowState;

    const size_t bufferCount = state->buffers().size();
    if (loopBody.getNumArguments() != bufferCount) {
        throw IRException("nl.row_drain loop must bind one variable per collected column");
    }

    NLRowLoopData* loopData = _program->allocFunctionData<NLRowLoopData>(state);
    loopData->setLimit(limit);
    loopData->getIndices()->reserve(_program->getChunkSize());

    for (size_t columnIndex = 0; columnIndex < bufferCount; columnIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(columnIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;

        const NLCarriedColumn column(state->buffer(columnIndex),
                                     output,
                                     selectGatherForChunkType(loopVariable.getType()));
        loopData->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runRowLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

NLRowState* NLTranslator::rowStateFor(mlir::Value handle) const {
    const auto stateIt = _rowStates.find(handle);
    if (stateIt == _rowStates.end()) {
        throw IRException("row handle must be produced by an nl.row_buffer");
    }

    return stateIt->second;
}

NLSortState* NLTranslator::sortStateFor(mlir::Value handle) const {
    const auto stateIt = _sortStates.find(handle);
    if (stateIt == _sortStates.end()) {
        throw IRException("sort handle must be produced by an nl.sort_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateDistinctState(nl::Distinct distinct, NLStmtContainer* body) {
    // Allocate the runtime seen-set and map the handle to it, so the filter that
    // names the handle finds the same set.
    NLDistinctState* state = _program->allocDistinctState();
    _distinctStates[distinct.getState()] = state;

    // The reset empties the set each time the block holding this nl.distinct runs:
    // once at function scope for a top-level / mid-query DISTINCT.
    NLDistinctResetData* resetData = _program->allocFunctionData<NLDistinctResetData>(state);
    body->emplaceStmt(&NLExecutor::runDistinctReset, resetData);
}

void NLTranslator::translateDistinctFilter(nl::DistinctFilter filter, NLStmtContainer* body) {
    // The handle is a required operand, so distinctStateFor returns its state or
    // throws if it was not produced by an nl.distinct.
    NLDistinctState* state = distinctStateFor(filter.getState());

    NLDistinctFilterData* data = _program->allocFunctionData<NLDistinctFilterData>(state);

    // Reserve the surviving-indices scratch so the per-step gather stays
    // allocation-free, the same as the edge and sort loops' indices column.
    data->getIndices()->reserve(_program->getChunkSize());

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = filter.getColumns();
    const mlir::ResultRange results = filter.getResults();
    std::vector<NLGroupHashKey> keys(columns.size());
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addDistinctColumn(columns[columnIndex], results[columnIndex], data);
        buildGroupHashKey(columns[columnIndex].getType(), keys[columnIndex]);
    }

    // The branches of a union each filter into the one seen-set, every one with the columns
    // the union's result types them as, so the first filter keys the hash table for all
    NLGroupHashTable& hashTable = state->hashTable();
    const std::vector<NLGroupHashKey>& tableKeys = hashTable.getKeys();

    if (tableKeys.empty()) {
        for (const NLGroupHashKey& key : keys) {
            hashTable.addKey(key);
        }

        hashTable.chooseTable();
    } else if (tableKeys != keys) {
        throw IRException("the filters of an nl.distinct must read columns of the same types");
    }

    body->emplaceStmt(&NLExecutor::runDistinctFilter, data);
}

void NLTranslator::addDistinctColumn(mlir::Value inputValue,
                                     mlir::Value resultValue,
                                     NLDistinctFilterData* data) {
    const Column* input = getColumn(inputValue);

    // The output keeps the input's element type; only duplicate rows are removed.
    // The gather copies the survivors into the fresh output - the same gather family
    // the edge and sort loops use, selected by chunk type.
    Column* output = allocColumnForChunkType(inputValue.getType());
    _valueSlots[resultValue] = output;

    const NLDistinctFilterData::FilterColumn column {input,
                                                     output,
                                                     selectGatherForChunkType(inputValue.getType())};
    data->addColumn(column);
}

NLDistinctState* NLTranslator::distinctStateFor(mlir::Value handle) const {
    const auto stateIt = _distinctStates.find(handle);
    if (stateIt == _distinctStates.end()) {
        throw IRException("distinct handle must be produced by an nl.distinct");
    }

    return stateIt->second;
}

void NLTranslator::translateEachRowLoop(const IteratorConfig& config,
                                        mlir::Block& loopBody,
                                        NLLimitState* limit,
                                        NLStmtContainer* body) {
    const std::span<const mlir::Value> columns = config._eachRowColumns;

    // The row count is read from the first column, so a walk over no column cannot be
    // sized. DBLowering never emits it, but the nl IR may come from elsewhere.
    if (columns.empty()) {
        throw IRException("nl.each_row needs at least one column");
    }

    if (loopBody.getNumArguments() != columns.size()) {
        throw IRException("nl.each_row loop must bind one variable per column");
    }

    NLEachRowLoopData* loopData = _program->allocFunctionData<NLEachRowLoopData>();
    loopData->setLimit(limit);
    loopData->getIndices()->resize(1);

    // Each loop variable is a one-row chunk of its column's own kind, filled by the gather
    // an edge hop uses over a one-row index column.
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(columnIndex));
        const mlir::Type chunkType = loopVariable.getType();

        Column* output = allocColumnForChunkType(chunkType);
        _valueSlots[loopVariable] = output;

        loopData->addColumn(NLCarriedColumn {getColumn(columns[columnIndex]),
                                             output,
                                             selectGatherForChunkType(chunkType)});
    }

    body->emplaceStmt(&NLExecutor::runEachRowLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}
