#include "DBLowering.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"

#include "IRConstantColumn.h"
#include "NullLiteralType.h"

#include "metadata/PropertyType.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

mlir::Value DBLowering::listItemChunk(mlir::Value chunk) {
    const mlir::Type element = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();
    if (mlir::isReadAsCells(element)) {
        return chunk;
    }

    return nullableValueChunk(chunk);
}

mlir::Value DBLowering::toAnyChunk(mlir::Value chunk, mlir::Type chunkType) {
    const mlir::Value itemChunk = listItemChunk(chunk);

    mlir::OpBuilder::InsertionGuard guard(_builder);
    _builder.setInsertionPointAfterValue(itemChunk);

    return _builder.create<nl::ToAny>(_builder.getUnknownLoc(), chunkType, itemChunk).getResult();
}

// The null literal's chunk read as the column the result carries: one absent value per row,
// in the type the branches saying what the column holds resolved it to.
mlir::Value DBLowering::typedNullChunk(mlir::Value chunk, mlir::Type chunkType) {
    mlir::OpBuilder::InsertionGuard guard(_builder);
    _builder.setInsertionPointAfterValue(chunk);

    return _builder.create<nl::ToNullable>(_builder.getUnknownLoc(), chunkType, chunk).getResult();
}

mlir::Value DBLowering::laidOutNullAs(mlir::Value chunk, mlir::Type valueElement) {
    const bool laidOutNull = isUntypedNullChunk(chunk.getType())
                          && !yieldsConstantColumn(chunk, _constantColumns);
    const bool namesAType = !mlir::isa<mlir::NoneType>(valueElement);

    if (!laidOutNull || !namesAType) {
        return chunk;
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType chunkType = nl::ChunkType::get(context, nullableElementType(valueElement));

    return typedNullChunk(chunk, chunkType);
}

mlir::Value DBLowering::chunkAsElement(mlir::Value chunk, mlir::Type resultElement) {
    const mlir::Type chunkType = chunk.getType();

    // A null branch is left as the untyped null it is: the selection writes an absent
    // value for the rows that take it, whatever the column holds elsewhere
    if (isUntypedNullChunk(chunkType)) {
        return chunk;
    } else if (chunkValueElement(_builder, chunkType) == resultElement) {
        return chunk;
    } else if (mlir::isa<storage::ListType>(resultElement)) {
        return chunk;
    } else if (mlir::isa<storage::AnyType>(resultElement)) {
        return listItemChunk(chunk);
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType convertedType
        = nl::ChunkType::get(context, storage::NullableType::get(context, resultElement));

    mlir::OpBuilder::InsertionGuard guard(_builder);
    setInsertionForUnaryOp(chunk);

    if (mlir::isa<mlir::Float64Type>(resultElement)) {
        return _builder.create<nl::ToFloat>(_builder.getUnknownLoc(), convertedType, chunk).getResult();
    }

    return _builder.create<nl::ToInteger>(_builder.getUnknownLoc(), convertedType, chunk).getResult();
}

// A scalar a procedure yielded is a plain value chunk, while a reduction, a collect or a
// comparison with null read a nullable one: the plain chunk is read as nullable right
// where it is bound, so those kernels take it as they take a property value.
mlir::Value DBLowering::nullableValueChunk(mlir::Value chunk) {
    const auto chunkType = mlir::cast<nl::ChunkType>(chunk.getType());
    const mlir::Type element = chunkType.getElementType();
    if (mlir::isa<storage::NullableType>(element)) {
        return chunk;
    }

    mlir::Type valueElement = element;
    if (mlir::isa<storage::BoolType>(element)) {
        valueElement = _builder.getI1Type();
    }

    // A node or an edge reads as its ID's integer, absent where an OPTIONAL MATCH left the
    // ID invalid
    if (mlir::isa<storage::NodeIDType, storage::EdgeIDType>(element)) {
        valueElement = _builder.getIntegerType(64, /*isSigned=*/false);
    }

    const ValueType valueType = elementTypeToValueType(valueElement);
    const bool isScalar = valueType != ValueType::Invalid
                       && valueType != ValueType::List
                       && valueType != ValueType::Map;
    const bool isSchemaID = storage::namesASchemaID(valueElement);
    if (!isScalar && !isSchemaID) {
        throw IRException("Only a scalar value column can be read as a nullable value column");
    }

    return toNullableChunk(chunk, valueElement);
}

mlir::Value DBLowering::numberOperandChunk(mlir::Value chunk) {
    if (isTaggedCellChunk(chunk.getType())) {
        return chunk;
    }

    return nullableValueChunk(chunk);
}

// The nullable column a list or a map rides where a row of it can be absent - the drain
// padding the input rows a subquery body yielded nothing for. Kept apart from the value
// sibling so the reductions and comparisons that read only a scalar still turn one away.
mlir::Value DBLowering::nullableContainerChunk(mlir::Value chunk) {
    const auto chunkType = mlir::cast<nl::ChunkType>(chunk.getType());
    const mlir::Type element = chunkType.getElementType();
    if (mlir::isa<storage::NullableType>(element)) {
        return chunk;
    }

    return toNullableChunk(chunk, element);
}

mlir::Value DBLowering::toNullableChunk(mlir::Value chunk, mlir::Type valueElement) {
    mlir::MLIRContext* const context = _builder.getContext();
    const storage::NullableType nullableType = storage::NullableType::get(context, valueElement);
    const nl::ChunkType resultType = nl::ChunkType::get(context, nullableType);

    mlir::OpBuilder::InsertionGuard guard(_builder);
    if (mlir::Operation* const definingOp = chunk.getDefiningOp()) {
        _builder.setInsertionPointAfter(definingOp);
    } else {
        _builder.setInsertionPointToStart(mlir::cast<mlir::BlockArgument>(chunk).getOwner());
    }

    return _builder.create<nl::ToNullable>(_builder.getUnknownLoc(), resultType, chunk).getResult();
}

// The chunk shape a drain can pad a missed row of. A node, an edge, a path and a tagged cell
// spell their own null. Any other column is read as a nullable one: a mask holds a bit, a
// number a zero and a label ID the invalid ID, which the query cannot tell from a value.
mlir::Value DBLowering::paddedColumnChunk(mlir::Value chunk) {
    const nl::ChunkType chunkType = mlir::dyn_cast<nl::ChunkType>(chunk.getType());
    if (!chunkType || holdsItsNull(chunkType)) {
        return chunk;
    }

    const mlir::Type element = chunkType.getElementType();

    if (mlir::isa<storage::ListType, storage::MapType>(element)) {
        return nullableContainerChunk(chunk);
    } else if (mlir::isa<storage::BoolType>(element)) {
        return nullableValueChunk(chunk);
    }

    return toNullableChunk(chunk, element);
}
