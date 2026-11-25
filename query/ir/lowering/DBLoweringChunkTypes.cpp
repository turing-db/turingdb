#include "DBLoweringChunkTypes.h"

#include "NLOps.h"

#include "DBColumnElement.h"
#include "NullLiteralType.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

mlir::Type db::chunkValueElement(mlir::OpBuilder& builder, mlir::Type chunkType) {
    mlir::Type element = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    if (const auto nullable = mlir::dyn_cast<storage::NullableType>(element)) {
        element = nullable.getValueType();
    }

    if (mlir::isa<storage::BoolType>(element)) {
        return builder.getI1Type();
    }

    return element;
}

bool db::isNullableChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        throw IRException("Tried to check nullity of non-chunk.");
    }

    return mlir::isa<storage::NullableType>(chunk.getElementType());
}

bool db::isListChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type listed = nullable ? nullable.getValueType() : element;

    return mlir::isa<storage::ListType>(listed);
}

mlir::Type db::indexedListElementType(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return {};
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type indexed = nullable ? nullable.getValueType() : element;

    const auto listType = mlir::dyn_cast<storage::ListType>(indexed);
    return listType ? listType.getElementType() : mlir::Type {};
}

bool db::isUntypedNullChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);

    return chunk && mlir::isNullLiteralType(chunk.getElementType());
}

bool db::isTaggedCellChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    return chunk && mlir::isa<storage::AnyType>(chunk.getElementType());
}

bool db::mayHoldANull(mlir::Type chunkType) {
    return isNullableChunk(chunkType) || isTaggedCellChunk(chunkType);
}

mlir::Type db::promoteNumeric(mlir::OpBuilder& builder, mlir::Type lhs, mlir::Type rhs) {
    return promotedNumberElement(builder.getContext(), lhs, rhs);
}

mlir::Type db::selectedValueElement(mlir::OpBuilder& builder, mlir::Type lhs, mlir::Type rhs) {
    const auto lhsInteger = mlir::dyn_cast<mlir::IntegerType>(lhs);
    const auto rhsInteger = mlir::dyn_cast<mlir::IntegerType>(rhs);
    const bool bothIntegers = lhsInteger && rhsInteger && lhsInteger.getWidth() == 64 && rhsInteger.getWidth() == 64;

    if (lhs == rhs) {
        return lhs;
    } else if (bothIntegers) {
        return builder.getIntegerType(64);
    } else if (mlir::isa<storage::ListType>(lhs) && mlir::isa<storage::ListType>(rhs)) {
        return storage::joinedElementType(lhs, rhs);
    }

    return mlir::Type {};
}

bool db::holdsItsNull(mlir::Type chunkType) {
    const mlir::Type element = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    return mlir::isa<storage::NullableType>(element) || holdsItsOwnNull(element);
}
