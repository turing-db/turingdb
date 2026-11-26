#include "NLTranslator.h"

#include <span>

#include "NLOps.h"

#include "mlir/IR/Block.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void NLTranslator::translateHashJoinBuffer(nl::HashJoinBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime build side and map the handle to it, so the collect and the
    // probe that name the handle share the same buffers and index. The buffer columns
    // themselves are allocated by the collect, which knows their types.
    NLHashJoinState* state = _program->allocHashJoinState();
    _hashJoinStates[buffer.getState()] = state;

    // The reset empties the buffers and the index each time the block holding this
    // nl.hash_join_buffer runs: once at function scope for a top-level join.
    NLHashJoinResetData* resetData = _program->allocFunctionData<NLHashJoinResetData>(state);
    body->emplaceStmt(&NLExecutor::runHashJoinReset, resetData);
}

void NLTranslator::translateHashJoinCollect(nl::HashJoinCollect collect, NLStmtContainer* body) {
    const mlir::Value collectState = collect.getState();
    NLHashJoinState* state = hashJoinStateFor(collectState);

    // The buffers are allocated once, by the single collect feeding a build side.
    // Generated IR has exactly one collect per build side; a second would append to the
    // same buffers and index the same rows twice, so it is rejected here.
    if (!state->buffers().empty()) {
        throw IRException("an nl.hash_join_buffer must be fed by a single nl.hash_join_collect");
    }

    const mlir::OperandRange columns = collect.getColumns();
    const uint64_t buildKey = hashJoinBufferOf(collectState).getBuildKey();
    if (buildKey >= columns.size()) {
        throw IRException("hash join build key column index is out of range");
    }

    NLHashJoinCollectData* data = _program->allocFunctionData<NLHashJoinCollectData>(state);

    // One growing buffer per build column, row-aligned. The buffer keeps the column's
    // element type; the append copies a chunk's rows onto its tail, exactly as a sort
    // accumulates them. The types are recorded beside them for the probe, which declares
    // these columns among its own results and is checked against what was allocated here.
    llvm::SmallVector<mlir::Type, 4>& buildTypes = _hashJoinBuildTypes[collectState];

    for (const mlir::Value column : columns) {
        const mlir::Type chunkType = column.getType();
        buildTypes.push_back(chunkType);

        Column* bufferColumn = allocColumnForChunkType(chunkType);
        state->addColumnBuffer(bufferColumn);

        const NLOwnedAppendFunction appendOwned = NLExecutor::selectOwnedAppendFunction(bufferColumn);
        const NLAppendFunction append = appendOwned ? nullptr : selectAppendForChunkType(chunkType);

        data->addAppend(NLSortCollectData::Append {getColumn(column),
                                                   bufferColumn,
                                                   append,
                                                   appendOwned});
    }

    // The key is one of those columns, read a second time to chain each row under its
    // hash; the match gate keeps a row no probe key can match - a null, a NaN - out of
    // the index.
    const mlir::Value keyColumn = columns[buildKey];
    data->setKeyColumns(getColumn(keyColumn),
                        state->buffer(buildKey),
                        selectJoinKeyFunctionsForChunkType(keyColumn.getType()));
    data->getHashScratch()->reserve(_program->getChunkSize());

    body->emplaceStmt(&NLExecutor::runHashJoinCollect, data);
}

void NLTranslator::translateHashJoinProbeLoop(const IteratorConfig& config,
                                              mlir::Block& loopBody,
                                              NLLimitState* limit,
                                              NLStmtContainer* body) {
    const mlir::Value probeState = config._hashJoinState;
    NLHashJoinState* state = hashJoinStateFor(probeState);

    const std::span<const mlir::Value> columns = config._probeColumns;

    // The build buffers are allocated by the collect, which the lowering places in the
    // build loop ahead of this probe, so by here they say how many columns the build side
    // contributes - and the loop binds one variable per probe column, then one per build
    // column, the order the iterator lays its chunks out in.
    const size_t buildCount = state->buffers().size();
    if (loopBody.getNumArguments() != columns.size() + buildCount) {
        throw IRException("nl.hash_join_probe emits one column per probe column and per build column");
    }

    nl::HashJoinBuffer buffer = hashJoinBufferOf(probeState);
    const uint64_t probeKey = buffer.getProbeKey();
    const uint64_t buildKey = buffer.getBuildKey();
    if (probeKey >= columns.size()) {
        throw IRException("hash join probe key column index is out of range");
    }

    NLHashJoinProbeLoopData* data = _program->allocFunctionData<NLHashJoinProbeLoopData>(state);
    data->setLimit(limit);

    // Reserve the matched-pair scratches so a step that pairs no more than a chunk's worth
    // of rows stays allocation-free, the same as the edge and sort loops' indices.
    data->getProbeIndices()->reserve(_program->getChunkSize());
    data->getBuildIndices()->reserve(_program->getChunkSize());
    data->getHashScratch()->reserve(_program->getChunkSize());

    // A probe column is read from this step's chunk and repeated once per match; a build
    // column is read back from its buffer at the matched row. Both are the gather the
    // edge and sort loops use, over the pair of index scratches the probe fills.
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value column = columns[columnIndex];

        Column* output = allocColumnForChunkType(column.getType());
        _valueSlots[loopBody.getArgument(static_cast<unsigned>(columnIndex))] = output;

        data->addProbeColumn(NLCarriedColumn(getColumn(column),
                                             output,
                                             selectGatherForChunkType(column.getType())));
    }

    // A build chunk is gathered out of the buffer the collect allocated, so the type the
    // iterator declares has to be the type that buffer holds: reading a column of one
    // element type as another would take its rows apart at the wrong width.
    const llvm::SmallVector<mlir::Type, 4>& buildTypes = _hashJoinBuildTypes[probeState];
    for (size_t columnIndex = 0; columnIndex < buildCount; columnIndex++) {
        const mlir::Value bound = loopBody.getArgument(static_cast<unsigned>(columns.size() + columnIndex));
        const mlir::Type bufferType = buildTypes[columnIndex];

        if (bound.getType() != bufferType) {
            throw IRException("nl.hash_join_probe must declare each build chunk as the chunk type the nl.hash_join_collect appended");
        }

        Column* output = allocColumnForChunkType(bufferType);
        _valueSlots[bound] = output;

        data->addBuildColumn(NLCarriedColumn(state->buffer(columnIndex),
                                             output,
                                             selectGatherForChunkType(bufferType)));
    }

    if (buildKey >= buildTypes.size()) {
        throw IRException("hash join build key column index is out of range");
    }

    const mlir::Value keyColumn = columns[probeKey];
    const mlir::Type keyType = keyColumn.getType();
    const Column* probeKeyColumn = getColumn(keyColumn);
    const Column* buildKeyColumn = state->buffer(buildKey);

    NLJoinKeyFunctions keyFunctions = selectJoinKeyFunctionsForChunkType(keyType);
    if (keyType != buildTypes[buildKey]) {
        keyFunctions._equal = NLExecutor::selectCrossTypeKeyEqualFunction(probeKeyColumn, buildKeyColumn);
    }

    data->setKeyColumns(probeKeyColumn,
                        buildKeyColumn,
                        keyFunctions);

    body->emplaceStmt(&NLExecutor::runHashJoinProbeLoop, data);

    translateBlock(loopBody, data->getStmts());
}

NLHashJoinState* NLTranslator::hashJoinStateFor(mlir::Value handle) const {
    const auto stateIt = _hashJoinStates.find(handle);
    if (stateIt == _hashJoinStates.end()) {
        throw IRException("hash join handle must be produced by an nl.hash_join_buffer");
    }

    return stateIt->second;
}

nl::HashJoinBuffer NLTranslator::hashJoinBufferOf(mlir::Value handle) {
    nl::HashJoinBuffer buffer = handle.getDefiningOp<nl::HashJoinBuffer>();
    if (!buffer) {
        throw IRException("hash join handle must be produced by an nl.hash_join_buffer");
    }

    return buffer;
}

// The join's sibling of selectKeyAppendForChunkType: a key is hashed and compared where it
// lives rather than serialized.
NLJoinKeyFunctions NLTranslator::selectJoinKeyFunctionsForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptJoinKeyFunctions(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellJoinKeyFunctions();
    }

    return NLExecutor::selectJoinKeyFunctions(chunkKindFromElementType(elementType));
}

void NLTranslator::translateCrossProductLoop(const IteratorConfig& config,
                                             mlir::Block& loopBody,
                                             NLLimitState* limit,
                                             NLStmtContainer* body) {
    const std::span<const mlir::Value> outerColumns = config._crossOuterColumns;
    const std::span<const mlir::Value> innerColumns = config._crossInnerColumns;

    // At run time the loop takes N from the first outer column and M from the first
    // inner column, so a side with no column cannot be sized. Reject that here:
    // DBLowering never emits it, but the nl IR may come from elsewhere.
    if (outerColumns.empty() || innerColumns.empty()) {
        throw IRException("nl.cross_product needs at least one column on each side");
    }

    NLCrossProductLoopData* loopData = _program->allocFunctionData<NLCrossProductLoopData>();
    loopData->setLimit(limit);

    // The loop binds one variable per crossed column - the outer columns followed by
    // the inner, the order inferReturnTypes lays the iterator's chunks out - so walk
    // the block arguments in step.
    unsigned argumentIndex = 0;

    for (const mlir::Value column : outerColumns) {
        addCrossColumn(column, loopBody.getArgument(argumentIndex), /*isOuter=*/true, loopData);
        argumentIndex++;
    }

    for (const mlir::Value column : innerColumns) {
        addCrossColumn(column, loopBody.getArgument(argumentIndex), /*isOuter=*/false, loopData);
        argumentIndex++;
    }

    body->emplaceStmt(&NLExecutor::runCrossProductLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::allocBroadcastColumn(mlir::Value inputValue,
                                        bool isOuter,
                                        Column*& output,
                                        NLBroadcastFunction& broadcast) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type, only the row count changes. A
    // nullable chunk allocates a ColumnOptVector and broadcasts on the kind it wraps; a
    // !storage.any chunk allocates a column of tagged scalars, which carry their own
    // type; any other chunk allocates on its own kind.

    // A constant column holds one value standing for every row, so repeating it block-wise
    // and tiling it give the same column: the one broadcast serves both sides.
    if (isConstantColumn(input)) {
        output = _memory->allocSame(input);
        broadcast = NLExecutor::selectConstBlockRepeatFunction();
    } else if (isNullableList(elementType)) {
        output = allocOptListColumn();
        broadcast = isOuter ? NLExecutor::selectOptListBlockRepeatFunction()
                            : NLExecutor::selectOptListTileFunction();
    } else if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const NLChunkKind kind = chunkKindFromNullableValue(nullableType.getValueType());
        output = allocOptColumnForKind(kind);
        broadcast = isOuter ? NLExecutor::selectOptBlockRepeatFunction(kind)
                            : NLExecutor::selectOptTileFunction(kind);
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        output = allocCellColumn();
        broadcast = isOuter ? NLExecutor::selectCellBlockRepeatFunction()
                            : NLExecutor::selectCellTileFunction();
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        broadcast = isOuter ? NLExecutor::selectBlockRepeatFunction(kind)
                            : NLExecutor::selectTileFunction(kind);
    }
}

void NLTranslator::addCrossColumn(mlir::Value inputValue,
                                  mlir::Value resultValue,
                                  bool isOuter,
                                  NLCrossProductLoopData* data) {
    const Column* input = getColumn(inputValue);

    Column* output = nullptr;
    NLBroadcastFunction broadcast = nullptr;
    allocBroadcastColumn(inputValue, isOuter, output, broadcast);

    _valueSlots[resultValue] = output;

    const NLCrossColumn crossColumn(input, output, broadcast);
    if (isOuter) {
        data->addOuterColumn(crossColumn);
    } else {
        data->addInnerColumn(crossColumn);
    }
}
