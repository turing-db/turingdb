#include "NLTranslator.h"

#include <optional>

#include "NLOps.h"

#include "llvm/ADT/DenseMap.h"

#include "columns/ColumnConst.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "NLExecutor.h"
#include "NLFunctionSelectors.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void NLTranslator::translateCurrentDateTime(nl::CurrentDateTime currentDateTime) {
    if (!_queryInstant.has_value()) {
        _queryInstant = DateTime::now();
    }

    ColumnConst<types::DateTime::Primitive>* instant
        = _memory->alloc<ColumnConst<types::DateTime::Primitive>>();
    instant->set(*_queryInstant);

    _valueSlots[currentDateTime.getResult()] = instant;
}

void NLTranslator::translateSubstring(nl::Substring substring, NLStmtContainer* body) {
    const mlir::Value resultValue = substring.getResult();
    const mlir::Type resultType = resultValue.getType();

    Column* const result = allocColumnForChunkType(resultType);
    _valueSlots[resultValue] = result;

    const Column* const string = getColumn(substring.getString());
    const Column* const start = getColumn(substring.getStart());

    NLSubstringData::IntegerArgument length;
    if (const mlir::Value lengthValue = substring.getLength()) {
        length._column = getColumn(lengthValue);
        length._read = NLExecutor::selectIntegerArgumentRead(length._column);
    }

    const NLSubstringData::StringArgument stringArgument {string, NLExecutor::selectStringArgumentRead(string)};
    const NLSubstringData::IntegerArgument startArgument {start, NLExecutor::selectIntegerArgumentRead(start)};

    NLSubstringData* data = _program->allocFunctionData<NLSubstringData>(result, stringArgument, startArgument, length);

    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultType).getElementType();
    const bool resultNullable = mlir::isa<storage::NullableType>(resultElement);

    if (resultNullable) {
        body->emplaceStmt(&NLExecutor::runSubstring<std::optional<types::String::Primitive>>, data);
    } else {
        body->emplaceStmt(&NLExecutor::runSubstring<types::String::Primitive>, data);
    }
}

void NLTranslator::translateSplit(nl::Split split, NLStmtContainer* body) {
    const mlir::Value resultValue = split.getResult();
    const mlir::Type resultType = resultValue.getType();

    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultType).getElementType();
    const bool resultNullable = mlir::isa<storage::NullableType>(resultElement);

    Column* const result = allocColumnForChunkType(resultType);
    _valueSlots[resultValue] = result;

    const Column* const string = getColumn(split.getString());
    const Column* const delimiter = getColumn(split.getDelimiter());

    const NLSplitData::StringArgument stringArgument {string, NLExecutor::selectStringArgumentRead(string)};
    const NLSplitData::DelimitersArgument delimitersArgument {delimiter, NLExecutor::selectStringListArgumentRead(delimiter)};

    NLSplitData* data = _program->allocFunctionData<NLSplitData>(result, _program->allocStepBuffers(), stringArgument, delimitersArgument);

    if (resultNullable) {
        body->emplaceStmt(&NLExecutor::runSplit<std::optional<ListView>>, data);
    } else {
        body->emplaceStmt(&NLExecutor::runSplit<ListView>, data);
    }
}

void NLTranslator::translateReplace(nl::Replace replace, NLStmtContainer* body) {
    const mlir::Value resultValue = replace.getResult();
    const mlir::Type resultType = resultValue.getType();

    Column* const result = allocColumnForChunkType(resultType);
    _valueSlots[resultValue] = result;

    const Column* const string = getColumn(replace.getString());
    const Column* const search = getColumn(replace.getSearch());
    const Column* const replacement = getColumn(replace.getReplacement());

    const NLReplaceData::StringArgument stringArgument {string, NLExecutor::selectStringArgumentRead(string)};
    const NLReplaceData::StringArgument searchArgument {search, NLExecutor::selectStringArgumentRead(search)};
    const NLReplaceData::StringArgument replacementArgument {replacement, NLExecutor::selectStringArgumentRead(replacement)};

    NLReplaceData* data = _program->allocFunctionData<NLReplaceData>(result,
                                                                     _program->allocStepBuffers(),
                                                                     stringArgument,
                                                                     searchArgument,
                                                                     replacementArgument);

    if (isNullableChunkType(resultType)) {
        body->emplaceStmt(&NLExecutor::runReplace<std::optional<types::String::Primitive>>, data);
    } else {
        body->emplaceStmt(&NLExecutor::runReplace<types::String::Primitive>, data);
    }
}

void NLTranslator::translatePointFunction(mlir::Operation* operation, NLHandlerFunction run, NLStmtContainer* body) {
    const mlir::Value resultValue = operation->getResult(0);

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    NLPointFunctionData* data = _program->allocFunctionData<NLPointFunctionData>(result);

    for (const mlir::Value operand : operation->getOperands()) {
        const Column* const argument = getColumn(operand);
        data->addArgument(NLPointFunctionData::PointArgument {argument, NLExecutor::selectPointArgumentRead(argument)});
    }

    body->emplaceStmt(run, data);
}

void NLTranslator::translateUnaryFunction(mlir::Operation* op, NLStmtContainer* body) {
    const NLUnaryFunctionSelector select = lookupUnaryFunctionSelector(*op);
    bioassert(select, "translateUnaryFunction called on a non-function op");

    const mlir::Value inputValue = op->getOperand(0);
    const Column* input = getColumn(inputValue);

    const auto inputChunk = mlir::cast<nl::ChunkType>(inputValue.getType());
    const bool inputNullable = mlir::isa<storage::NullableType>(inputChunk.getElementType());

    Column* result = nullptr;
    const NLUnaryFunctionKernel kernel = select(input, inputNullable, _memory, result);
    bindKernelResult(op->getResult(0), result);

    NLUnaryFunctionData* data = _program->allocFunctionData<NLUnaryFunctionData>(input, result, kernel, _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runUnaryFunction, data);
}

void NLTranslator::translateBinaryFunction(mlir::Operation* op, NLStmtContainer* body) {
    const NLBinaryFunctionSelector select = lookupBinaryFunctionSelector(*op);
    bioassert(select, "translateBinaryFunction called on a non-function op");

    const Column* lhs = getColumn(op->getOperand(0));
    const Column* rhs = getColumn(op->getOperand(1));

    Column* result = nullptr;
    const NLBinaryFn fn = select(lhs, rhs, _memory, result);
    bindKernelResult(op->getResult(0), result);

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn, _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runBinary, data);
}
