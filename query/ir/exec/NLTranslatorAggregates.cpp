#include "NLTranslator.h"

#include <optional>
#include <string_view>

#include "NLOps.h"
#include "StorageEnums.h"

#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// The runtime reduction the interpreter dispatches on, from the MLIR one the op
// carries. The two enums are kept separate so the runtime (NLProgram / NLExecutor)
// stays free of the MLIR dialect headers; this is the one place they meet.
AggregateKind toRuntimeAggregateKind(storage::AggregateKind kind) {
    switch (kind) {
        case storage::AggregateKind::Sum:
            return AggregateKind::Sum;
        break;

        case storage::AggregateKind::Min:
            return AggregateKind::Min;
        break;

        case storage::AggregateKind::Max:
            return AggregateKind::Max;
        break;

        case storage::AggregateKind::Avg:
            return AggregateKind::Avg;
        break;

        case storage::AggregateKind::StDev:
            return AggregateKind::StDev;
        break;

        case storage::AggregateKind::StDevP:
            return AggregateKind::StDevP;
        break;
    }

    throw IRException("Unhandled aggregate kind");
}

// The grouped reduction whose state a keyless aggregate of this kind keeps for its one group
GroupAggregateKind groupAggregateKindOf(AggregateKind kind) {
    switch (kind) {
        case AggregateKind::Sum:
            return GroupAggregateKind::Sum;
        break;

        case AggregateKind::Min:
            return GroupAggregateKind::Min;
        break;

        case AggregateKind::Max:
            return GroupAggregateKind::Max;
        break;

        case AggregateKind::Avg:
            return GroupAggregateKind::Avg;
        break;

        case AggregateKind::StDev:
            return GroupAggregateKind::StDev;
        break;

        case AggregateKind::StDevP:
            return GroupAggregateKind::StDevP;
        break;
    }

    throw IRException("Unhandled aggregate kind");
}

}

void NLTranslator::translateCountState(nl::Count count, NLStmtContainer* body) {
    // Allocate the runtime tally and map the handle to it, so the update and the
    // emit loop that name the handle share the same counter.
    NLCountState* state = _program->allocCountState();
    _countStates[count.getState()] = state;

    // The reset zeroes the tally each time the block holding this nl.count runs:
    // once at function scope for a top-level / mid-query COUNT.
    NLCountResetData* resetData = _program->allocFunctionData<NLCountResetData>(state);
    body->emplaceStmt(&NLExecutor::runCountReset, resetData);
}

void NLTranslator::translateCountUpdate(nl::CountUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so countStateFor returns its tally or
    // throws if it was not produced by an nl.count.
    NLCountState* state = countStateFor(update.getState());

    // Measure the chunk's non-null rows the way its type demands: all rows for an
    // ID chunk, the present values for a nullable value chunk. count(*) reads no value,
    // so it charges every row whatever the chunk holds.
    const mlir::Value rows = update.getRows();
    const Column* input = getColumn(rows);
    const NLCountFunction count = update.getAllRows() ? &NLExecutor::countAllRows
                                                      : selectCountForChunkType(rows.getType());

    NLCountUpdateData* data = _program->allocFunctionData<NLCountUpdateData>(state, input, count);
    body->emplaceStmt(&NLExecutor::runCountUpdate, data);
}

void NLTranslator::translateCountResult(nl::CountResult result, NLStmtContainer* body) {
    // The handle is a required operand, so countStateFor returns its tally or
    // throws if it was not produced by an nl.count.
    NLCountState* state = countStateFor(result.getState());

    // The result is the unsigned i64 count chunk (!nl.chunk<ui64>) runCountResult
    // fills with the single tally row.
    ColumnVector<uint64_t>* output = allocCountColumn();
    _valueSlots[result.getResult()] = output;

    NLCountResultData* data = _program->allocFunctionData<NLCountResultData>(state, output);
    body->emplaceStmt(&NLExecutor::runCountResult, data);
}

void NLTranslator::translateCountScanRows(nl::CountScanRows countScanRows, NLStmtContainer* body) {
    // The result is the unsigned i64 count chunk (!nl.chunk<ui64>) runCountScanRows fills
    // with the single product row - the chunk an nl.count_result emits after its loop.
    ColumnVector<uint64_t>* output = allocCountColumn();
    _valueSlots[countScanRows.getResult()] = output;

    const mlir::ArrayAttr scans = countScanRows.getLabels();

    NLCountScanRowsData* data = _program->allocFunctionData<NLCountScanRowsData>(output);
    data->reserveLabelSets(scans.size());

    for (const mlir::Attribute scan : scans) {
        llvm::SmallVector<llvm::StringRef, 4> labels;
        resolveLabelNames(mlir::cast<mlir::ArrayAttr>(scan), labels);

        LabelSet labelset;
        if (!resolveLabelSet(labels, labelset)) {
            data->markUnmatchable();
        }

        data->addLabelSet(labelset);
    }

    // A property the schema never had is held by no node, so the tally over its holders
    // is 0 - the same unsatisfiable conjunction an absent label leaves.
    if (const std::optional<llvm::StringRef> property = countScanRows.getProperty()) {
        const std::optional<PropertyType> propertyType = _view->metadata().propTypes().get(std::string_view(property->data(), property->size()));
        if (propertyType) {
            data->setProperty(propertyType->_id, countScanRows.getPropertyScan().value_or(0));
        } else {
            data->markUnmatchable();
        }
    }

    body->emplaceStmt(&NLExecutor::runCountScanRows, data);
}

NLCountState* NLTranslator::countStateFor(mlir::Value handle) const {
    const auto stateIt = _countStates.find(handle);
    if (stateIt == _countStates.end()) {
        throw IRException("count handle must be produced by an nl.count");
    }

    return stateIt->second;
}

void NLTranslator::translateAggregateState(nl::Aggregate aggregate, NLStmtContainer* body) {
    // Allocate the runtime state and map the handle to it, so the update and the emit that
    // name the handle share the same state.
    NLAggregateState* state = _program->allocAggregateState();
    _aggregateStates[aggregate.getState()] = state;

    // The state handle's element type is the accumulator's value type (f64 for an avg of
    // numbers, the input type otherwise), baked during lowering. The aggregate keeps the state
    // its grouped counterpart keeps per group: over type-erased cells, which name no value
    // type, a sum, an avg, a min or a max keeps a tagged sum or a cell.
    const auto stateType = mlir::cast<nl::AggregateStateType>(aggregate.getState().getType());
    const mlir::Type accumulatorElement = stateType.getElementType();
    const GroupAggregateKind kind = groupAggregateKindOf(toRuntimeAggregateKind(aggregate.getKind()));

    NLGroupAggregateStateType placedStateType;
    if (mlir::isa<storage::AnyType>(accumulatorElement)) {
        NLExecutor::selectTaggedGroupAggregateState(kind, placedStateType);
    } else {
        NLExecutor::selectGroupAggregateState(kind, valueTypeFromElementType(accumulatorElement), placedStateType);
    }

    state->setStateType(placedStateType);

    // The reset re-initializes the state each time the block holding this nl.aggregate runs:
    // once at function scope for a top-level aggregate.
    NLAggregateResetData* resetData = _program->allocFunctionData<NLAggregateResetData>(state);
    body->emplaceStmt(&NLExecutor::runAggregateReset, resetData);
}

void NLTranslator::translateAggregateUpdate(nl::AggregateUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so aggregateStateFor returns its state or throws if
    // it was not produced by an nl.aggregate.
    NLAggregateState* state = aggregateStateFor(update.getState());

    // Fold the chunk's non-null values the way its kind and chunk type demand. The input
    // value type may differ from the accumulator's (an avg of i64 folds into an f64 total),
    // so the fold is selected from the input here.
    const mlir::Value rows = update.getRows();
    const Column* input = getColumn(rows);
    const AggregateKind kind = toRuntimeAggregateKind(update.getKind());
    const NLAggregateFoldFunction fold = selectAggregateFoldForChunkType(kind, rows.getType());

    NLAggregateUpdateData* data = _program->allocFunctionData<NLAggregateUpdateData>(state, input, fold);
    body->emplaceStmt(&NLExecutor::runAggregateUpdate, data);
}

void NLTranslator::translateAggregateResult(nl::AggregateResult result, NLStmtContainer* body) {
    // The handle is a required operand, so aggregateStateFor returns its state or throws if
    // it was not produced by an nl.aggregate.
    NLAggregateState* state = aggregateStateFor(result.getState());

    // The result is a single-row chunk runAggregateResult fills with the reduced value: a
    // nullable value or a tagged cell. Allocate its column on the result type and map the op
    // result to it.
    const mlir::Value resultChunk = result.getResult();
    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultChunk.getType()).getElementType();

    Column* output = nullptr;
    if (mlir::isa<storage::AnyType>(resultElement)) {
        output = allocCellColumn();
    } else {
        output = allocOptColumnForValueType(nullableChunkValueType(resultChunk.getType()));
    }

    _valueSlots[resultChunk] = output;

    NLAggregateResultData* data = _program->allocFunctionData<NLAggregateResultData>(state,
                                                                                    output,
                                                                                    _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runAggregateResult, data);
}

NLAggregateState* NLTranslator::aggregateStateFor(mlir::Value handle) const {
    const auto stateIt = _aggregateStates.find(handle);
    if (stateIt == _aggregateStates.end()) {
        throw IRException("aggregate handle must be produced by an nl.aggregate");
    }

    return stateIt->second;
}

// The value-reduction sibling of selectCountForChunkType: a type-erased column of tagged
// cells is folded cell by cell, every other one through the value type it shares.
NLAggregateFoldFunction NLTranslator::selectAggregateFoldForChunkType(AggregateKind kind, mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectTaggedAggregateFold(kind);
    }

    return NLExecutor::selectAggregateFold(kind, nullableChunkValueType(chunkType));
}

NLCountFunction NLTranslator::selectCountForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListCountFunction();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptCountFunction(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellCountFunction();
    }

    // An entity an OPTIONAL MATCH did not match is an invalid ID and a path it did not match
    // is null, so those chunks count their other rows; every other plain chunk holds no
    // null and counts them all, whether or not it is one chunkKindFromElementType has a
    // kind for.
    if (isEntityIDElement(elementType)) {
        return NLExecutor::selectIDCountFunction(chunkKindFromElementType(elementType));
    } else if (mlir::isa<storage::PathType>(elementType)) {
        return NLExecutor::selectPathCountFunction();
    }

    return &NLExecutor::countAllRows;
}
