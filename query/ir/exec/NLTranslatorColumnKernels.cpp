#include "NLTranslator.h"

#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

NLAppendFunction NLTranslator::selectAppendForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListAppendFunction();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptAppendFunction(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellAppendFunction();
    }

    return NLExecutor::selectAppendFunction(chunkKindFromElementType(elementType));
}

NLGatherFunction NLTranslator::selectGatherForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListGatherFunction();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptGatherFunction(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellGatherFunction();
    }

    return NLExecutor::selectGatherFunction(chunkKindFromElementType(elementType));
}

NLCompareFunction NLTranslator::selectCompareForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListCompareFunction();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptCompareFunction(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellCompareFunction();
    }

    if (llvm::isa<storage::ListType>(elementType)) {
        return NLExecutor::selectListCompareFunction();
    }

    return NLExecutor::selectCompareFunction(chunkKindFromElementType(elementType));
}

NLKeyAppendFunction NLTranslator::selectKeyAppendForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListKeyAppendFunction();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptKeyAppendFunction(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellKeyAppendFunction();
    }

    return NLExecutor::selectKeyAppendFunction(chunkKindFromElementType(elementType));
}

void NLTranslator::buildGroupHashKey(mlir::Type chunkType, NLGroupHashKey& key) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(elementType);
    const mlir::Type valueType = nullable ? nullable.getValueType() : elementType;

    key._keyAppend = selectKeyAppendForChunkType(chunkType);
    key._nullable = static_cast<bool>(nullable);

    if (mlir::isa<storage::StringType>(valueType)) {
        key._shape = NLGroupKeyShape::String;
        return;
    } else if (mlir::isa<storage::AnyType>(valueType)) {
        key._shape = NLGroupKeyShape::Other;
        return;
    }

    if (nullable) {
        key._packing = NLExecutor::selectOptKeyPacking(chunkKindFromNullableValue(valueType));
    } else {
        key._packing = NLExecutor::selectKeyPacking(chunkKindFromElementType(elementType));
    }

    key._shape = key._packing._pack ? NLGroupKeyShape::Fixed : NLGroupKeyShape::Other;
}

NLGroupKeyGatherFunction NLTranslator::selectGroupKeyGatherForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListGroupKeyGatherFunction();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptGroupKeyGather(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellGroupKeyGatherFunction();
    }

    return NLExecutor::selectGroupKeyGather(chunkKindFromElementType(elementType));
}

NLCopyFunction NLTranslator::selectCopyForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListCopyFunction();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptCopyFunction(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellCopyFunction();
    }

    return NLExecutor::selectCopyFunction(chunkKindFromElementType(elementType));
}
