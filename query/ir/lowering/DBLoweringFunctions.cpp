#include "DBLowering.h"

#include <spdlog/fmt/fmt.h>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"
#include "DBLoweringFunctionEmitters.h"

#include "DBColumnElement.h"
#include "NullLiteralType.h"

#include "metadata/GraphMetadata.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void DBLowering::lowerSubstring(mlir::db::Substring substring) {
    const mlir::Type stringChunkType = mapValue(substring.getString()).getType();
    const bool stringMayBeNull = mayHoldANull(stringChunkType);

    llvm::SmallVector<mlir::Value, 3> columns {substring.getString(), substring.getStart()};

    const mlir::Value length = substring.getLength();
    if (length) {
        columns.push_back(length);
    }

    llvm::SmallVector<mlir::Value, 3> arguments;
    containerCellChunks(columns, arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type resultElement = stringMayBeNull ? storage::NullableType::get(context, stringType) : stringType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    nl::Substring substrings = _builder.create<nl::Substring>(_builder.getUnknownLoc(),
                                                              resultType,
                                                              arguments[0],
                                                              arguments[1],
                                                              length ? arguments[2] : mlir::Value {});

    _valueMap[substring.getResult()] = substrings.getResult();
}

void DBLowering::lowerSplit(mlir::db::Split split) {
    const mlir::Type stringChunkType = mapValue(split.getString()).getType();
    const mlir::Type delimiterChunkType = mapValue(split.getDelimiter()).getType();

    const bool stringMayBeNull = mayHoldANull(stringChunkType);

    // A list may hold a null whatever its element type, and a null delimiter splits into null
    const bool delimiterMayBeNull = mayHoldANull(delimiterChunkType) || isListChunk(delimiterChunkType);

    llvm::SmallVector<mlir::Value, 2> arguments;
    containerCellChunks(split->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type listType = storage::ListType::get(context, stringType);

    const bool resultMayBeNull = stringMayBeNull || delimiterMayBeNull;
    const mlir::Type resultElement = resultMayBeNull ? storage::NullableType::get(context, listType) : listType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    nl::Split parts = _builder.create<nl::Split>(_builder.getUnknownLoc(),
                                                 resultType,
                                                 arguments[0],
                                                 arguments[1]);

    _valueMap[split.getResult()] = parts.getResult();
}

void DBLowering::lowerReplace(mlir::db::Replace replace) {
    const bool anArgumentMayBeNull = llvm::any_of(replace->getOperands(), [this](mlir::Value operand) {
        return mayHoldANull(mapValue(operand).getType());
    });

    llvm::SmallVector<mlir::Value, 3> arguments;
    containerCellChunks(replace->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type resultElement = anArgumentMayBeNull ? storage::NullableType::get(context, stringType) : stringType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    nl::Replace replaced = _builder.create<nl::Replace>(_builder.getUnknownLoc(),
                                                        resultType,
                                                        arguments[0],
                                                        arguments[1],
                                                        arguments[2]);

    _valueMap[replace.getResult()] = replaced.getResult();
}

void DBLowering::lowerCurrentDateTime(mlir::db::CurrentDateTime currentDateTime) {
    setInsertionAfterHoisted();

    const nl::ChunkType resultType
        = nl::ChunkType::get(_builder.getContext(), storage::DateTimeType::get(_builder.getContext()));

    nl::CurrentDateTime lowered
        = _builder.create<nl::CurrentDateTime>(_builder.getUnknownLoc(), resultType);

    _valueMap[currentDateTime.getResult()] = lowered.getResult();
    _lastHoisted = lowered.getOperation();
}

void DBLowering::lowerUnaryFunction(mlir::Operation* op) {
    const UnaryFunctionLowering* spec = lookupUnaryFunctionLowering(*op);
    bioassert(spec, "lowerUnaryFunction called on a non-function op");

    const mlir::Value inputChunk = mapValue(op->getOperand(0));

    const mlir::Type inputElement = mlir::cast<nl::ChunkType>(inputChunk.getType()).getElementType();
    const auto inputNullableElement = mlir::dyn_cast<storage::NullableType>(inputElement);
    const mlir::Type inputValueElement = inputNullableElement ? inputNullableElement.getValueType() : inputElement;

    const mlir::Type baseElement = unaryFunctionElement(op, inputValueElement);
    if (!baseElement) {
        const mlir::OperationName name = op->getName();
        throw IRException(fmt::format("{} reads no column of this element type", name.getStringRef().str()));
    }

    const bool inputCanBeNull = mayHoldANull(inputChunk.getType());

    const bool alwaysNull = spec->nullability == ResultNullability::AlwaysNullable;
    const bool specNull = spec->nullability == ResultNullability::FollowsInput && inputCanBeNull;
    const bool resultNullable = alwaysNull || specNull;

    mlir::Type resultElement = baseElement;
    if (resultNullable) {
        resultElement = nullableElementType(baseElement);
    }

    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForUnaryOp(inputChunk);

    _valueMap[op->getResult(0)] = spec->emit(_builder, _builder.getUnknownLoc(), resultType, inputChunk, op);
}

void DBLowering::lowerHeadOrLast(mlir::Operation& operation) {
    const mlir::Value listChunk = mapValue(operation.getOperand(0));
    const mlir::Type listChunkType = listChunk.getType();

    const bool listsTypedElements = isListChunk(listChunkType)
                                 && !mlir::isa<storage::AnyType, mlir::NoneType>(indexedListElementType(listChunkType));
    if (!listsTypedElements) {
        lowerUnaryFunction(&operation);
        return;
    }

    const int64_t position = mlir::isa<mlir::db::Head>(operation) ? 0 : -1;

    setInsertionAfterHoisted();
    nl::Constant positionConstant = _builder.create<nl::Constant>(_builder.getUnknownLoc(), _builder.getI64IntegerAttr(position));
    _lastHoisted = positionConstant.getOperation();

    const mlir::Value positionChunk = positionConstant.getResult();
    const mlir::Type resultElement = binaryResultElement(BinaryResultKind::Index, listChunkType, positionChunk.getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForBinaryOp(listChunk, positionChunk);

    nl::ListIndex element = _builder.create<nl::ListIndex>(_builder.getUnknownLoc(), resultType, listChunk, positionChunk);
    _valueMap[operation.getResult(0)] = element.getResult();
}

void DBLowering::lowerBinaryFunction(mlir::Operation* op) {
    const BinaryFunctionLowering* spec = lookupBinaryFunctionLowering(*op);
    bioassert(spec, "lowerBinaryFunction called on a non-function op");

    const mlir::Value lhsChunk = mapValue(op->getOperand(0));
    const mlir::Value rhsChunk = mapValue(op->getOperand(1));

    const mlir::Type lhsElement = mlir::cast<nl::ChunkType>(lhsChunk.getType()).getElementType();
    const mlir::Type baseElement = spec->element(_builder, lhsElement);

    mlir::Type resultElement = baseElement;
    if (mayHoldANull(lhsChunk.getType()) || mayHoldANull(rhsChunk.getType())) {
        resultElement = nullableElementType(baseElement);
    }

    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForBinaryOp(lhsChunk, rhsChunk);

    _valueMap[op->getResult(0)] = spec->emit(_builder, _builder.getUnknownLoc(), resultType, lhsChunk, rhsChunk);
}

void DBLowering::lowerEntityIDOf(mlir::db::EntityIDOf entityIDOf) {
    const mlir::Value inputChunk = mapValue(entityIDOf.getInput());
    const mlir::db::ColumnType resultColumn = mlir::cast<mlir::db::ColumnType>(entityIDOf.getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultColumn.getType());

    setInsertionForUnaryOp(inputChunk);

    nl::EntityIDOf nlEntityIDOf = _builder.create<nl::EntityIDOf>(_builder.getUnknownLoc(), resultType, inputChunk);
    _valueMap[entityIDOf.getResult()] = nlEntityIDOf.getResult();
}
