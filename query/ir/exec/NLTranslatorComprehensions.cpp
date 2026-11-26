#include "NLTranslator.h"

#include "NLOps.h"
#include "StorageEnums.h"

#include "mlir/IR/Block.h"
#include "llvm/ADT/DenseMap.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

ListPredicateKind listPredicateKind(storage::ListPredicateKind kind) {
    switch (kind) {
        case storage::ListPredicateKind::All:
            return ListPredicateKind::All;
        break;

        case storage::ListPredicateKind::Any:
            return ListPredicateKind::Any;
        break;

        case storage::ListPredicateKind::None:
            return ListPredicateKind::None;
        break;

        case storage::ListPredicateKind::Single:
            return ListPredicateKind::Single;
        break;
    }

    return ListPredicateKind::All;
}

}

template <typename Data, typename Op, typename... Extra>
Data* NLTranslator::allocElementData(Op op, Extra... extra) {
    const mlir::Value sourceValue = op.getSource();
    const mlir::Type sourceElement = mlir::cast<nl::ChunkType>(sourceValue.getType()).getElementType();

    mlir::Block& bodyBlock = op.getBody().front();
    const mlir::Value elementValue = bodyBlock.getArgument(0);
    const mlir::Value rowTagValue = bodyBlock.getArgument(1);

    NLUnwindElementCountFunction elementCount = nullptr;
    NLUnwindElementEmitFunction elementEmit = nullptr;
    selectElementDrain(sourceElement, elementValue.getType(), elementCount, elementEmit);

    Column* const elementOutput = elementEmit ? allocColumn(elementValue) : nullptr;

    const mlir::Value resultValue = op.getResult();
    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    ColumnVector<uint64_t>* const rowTags =
        static_cast<ColumnVector<uint64_t>*>(allocColumn(rowTagValue));

    return _program->allocFunctionData<Data>(getColumn(sourceValue),
                                             elementCount,
                                             elementEmit,
                                             selectCellAbsent(sourceElement),
                                             elementOutput,
                                             rowTags,
                                             result,
                                             extra...);
}

template <typename Op>
mlir::Value NLTranslator::translateElementBody(Op op, NLElementBodyData* data) {
    const mlir::Value sourceValue = op.getSource();
    const Column* source = data->getSource();

    // The body binds the element, then the row tag, then one chunk per carried column -
    // the block argument order of the op's region.
    mlir::Block& bodyBlock = op.getBody().front();
    const mlir::Value elementValue = bodyBlock.getArgument(0);

    const size_t chunkSize = _program->getChunkSize();
    data->getRows()->reserve(chunkSize);
    data->getPositions()->reserve(chunkSize);
    data->getRowTags()->reserve(chunkSize);

    if (!data->getElementEmitFunc()) {
        const NLCarriedColumn elementColumn(source,
                                            allocColumn(elementValue),
                                            selectGatherForChunkType(sourceValue.getType()));
        data->addCarriedColumn(elementColumn);
    }

    const mlir::OperandRange carriedColumns = op.getColumnsToFilter();
    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const mlir::Value carriedValue = carriedColumns[carriedIndex];
        const mlir::Value bodyChunk = bodyBlock.getArgument(static_cast<unsigned>(2 + carriedIndex));

        const NLCarriedColumn carriedColumn(getColumn(carriedValue),
                                            allocColumn(bodyChunk),
                                            selectGatherForChunkType(bodyChunk.getType()));
        data->addCarriedColumn(carriedColumn);
    }

    translateBlock(bodyBlock, data->getStmts());

    // The op's verifier guarantees the terminator, so a body without one here means
    // unverified IR.
    nl::ComprehensionYield yield = mlir::dyn_cast<nl::ComprehensionYield>(bodyBlock.back());
    if (!yield) {
        throw IRException("The body of an op over the elements of a list does not end with an nl.comprehension_yield");
    }

    const mlir::Value valueValue = yield.getValue();
    data->setYield(getColumn(yield.getRowTags()), getColumn(valueValue));

    return valueValue;
}

void NLTranslator::translateListComprehension(nl::ListComprehension comprehension, NLStmtContainer* body) {
    NLListComprehensionData* data = allocElementData<NLListComprehensionData>(comprehension, _program->allocStepBuffers());

    const mlir::Value valueValue = translateElementBody(comprehension, data);
    data->setValueRead(selectListItemRead(valueValue.getType()));

    body->emplaceStmt(&NLExecutor::runListComprehension, data);
}

void NLTranslator::translateListPredicate(nl::ListPredicate predicate, NLStmtContainer* body) {
    NLListPredicateData* data = allocElementData<NLListPredicateData>(predicate, listPredicateKind(predicate.getKind()));

    const mlir::Value valueValue = translateElementBody(predicate, data);
    data->setTruthRead(NLExecutor::selectTruthRead(truthColumnKindOf(valueValue.getType(), getColumn(valueValue))));

    body->emplaceStmt(&NLExecutor::runListPredicate, data);
}

template <typename Data, typename Op, typename... Extra>
Data* NLTranslator::allocFoldData(Op op, Column* accumulator, Extra... extra) {
    const mlir::Value sourceValue = op.getSource();
    const mlir::Type sourceElement = mlir::cast<nl::ChunkType>(sourceValue.getType()).getElementType();

    const mlir::Value elementValue = op.getBody().front().getArgument(0);

    NLUnwindElementCountFunction elementCount = nullptr;
    NLUnwindElementEmitFunction elementEmit = nullptr;
    selectElementDrain(sourceElement, elementValue.getType(), elementCount, elementEmit);

    Column* const elementOutput = elementEmit ? allocColumn(elementValue) : nullptr;

    return _program->allocFunctionData<Data>(getColumn(sourceValue),
                                             elementCount,
                                             elementEmit,
                                             selectCellAbsent(sourceElement),
                                             elementOutput,
                                             getColumn(op.getInitialValue()),
                                             accumulator,
                                             _program->allocStepBuffers(),
                                             extra...);
}

template <typename Op>
void NLTranslator::translateFoldBody(Op op, NLFoldData* data) {
    const mlir::Value sourceValue = op.getSource();
    Column* const accumulator = data->getAccumulator();

    // The body binds the element, then the accumulator, then one chunk per carried column
    mlir::Block& bodyBlock = op.getBody().front();
    const mlir::Value elementValue = bodyBlock.getArgument(0);
    const mlir::Value accumulatorValue = bodyBlock.getArgument(1);
    const mlir::Type accumulatorType = accumulatorValue.getType();

    const size_t chunkSize = _program->getChunkSize();
    data->getRows()->reserve(chunkSize);
    data->getPositions()->reserve(chunkSize);

    if (!data->getElementEmitFunc()) {
        const NLCarriedColumn elementColumn(data->getSource(),
                                            allocColumn(elementValue),
                                            selectGatherForChunkType(sourceValue.getType()));
        data->addCarriedColumn(elementColumn);
    }

    const NLCarriedColumn accumulatorColumn(accumulator,
                                            allocColumn(accumulatorValue),
                                            selectGatherForChunkType(accumulatorType));
    data->addCarriedColumn(accumulatorColumn);

    const mlir::OperandRange carriedColumns = op.getColumnsToFilter();
    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const mlir::Value bodyChunk = bodyBlock.getArgument(static_cast<unsigned>(2 + carriedIndex));

        const NLCarriedColumn carriedColumn(getColumn(carriedColumns[carriedIndex]),
                                            allocColumn(bodyChunk),
                                            selectGatherForChunkType(bodyChunk.getType()));
        data->addCarriedColumn(carriedColumn);
    }

    translateBlock(bodyBlock, data->getStmts());

    const mlir::Value yieldedValue = mlir::cast<nl::ReduceYield>(bodyBlock.getTerminator()).getValue();
    const Column* yielded = getColumn(yieldedValue);
    data->setValue(yielded);

    const mlir::Value initialValue = op.getInitialValue();
    const bool accumulatesTaggedCells = isAnyChunk(accumulatorType);

    data->setReset(selectCaseResetForChunkType(accumulatorType), selectCaseFinishForChunkType(accumulatorType));
    data->setWrites(selectCellWriteForChunkType(accumulatorType, data->getInitialValue(), initialValue.getType()),
                    selectCellWriteForChunkType(accumulatorType, yielded, yieldedValue.getType()));

    const NLReownRowsFunction reownRows = accumulatesTaggedCells ? nullptr
                                                                 : NLExecutor::selectReownRowsFunction(accumulator);
    data->setReowns(NLExecutor::selectReownFunction(accumulator), reownRows);
}

void NLTranslator::translateReduce(nl::Reduce reduce, NLStmtContainer* body) {
    const mlir::Value resultValue = reduce.getResult();
    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    NLFoldData* data = allocFoldData<NLFoldData>(reduce, result);
    translateFoldBody(reduce, data);

    body->emplaceStmt(&NLExecutor::runReduce, data);
}

void NLTranslator::translateAllReduce(nl::AllReduce allReduce, NLStmtContainer* body) {
    mlir::Block& bodyBlock = allReduce.getBody().front();
    Column* const accumulator = allocColumnForChunkType(bodyBlock.getArgument(1).getType());

    const mlir::Value resultValue = allReduce.getResult();
    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    NLAllReduceData* data = allocFoldData<NLAllReduceData>(allReduce, accumulator, result);
    translateFoldBody(allReduce, data);

    mlir::Block& predicateBlock = allReduce.getPredicate().front();
    const mlir::Value testedAccumulatorValue = predicateBlock.getArgument(1);

    for (const mlir::BlockArgument argument : predicateBlock.getArguments()) {
        if (argument != testedAccumulatorValue) {
            _valueSlots[argument] = getColumn(bodyBlock.getArgument(argument.getArgNumber()));
        }
    }

    data->setTestedAccumulator(allocColumn(testedAccumulatorValue),
                               selectGatherForChunkType(testedAccumulatorValue.getType()));

    translateBlock(predicateBlock, data->getPredicateStmts());

    const mlir::Value holdsValue = mlir::cast<nl::AllReduceYield>(predicateBlock.getTerminator()).getHolds();
    const Column* holds = getColumn(holdsValue);
    data->setHolds(holds, NLExecutor::selectTruthRead(truthColumnKindOf(holdsValue.getType(), holds)));

    body->emplaceStmt(&NLExecutor::runAllReduce, data);
}
