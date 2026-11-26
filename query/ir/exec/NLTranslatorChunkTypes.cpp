#include "NLTranslatorChunkTypes.h"

#include "NLOps.h"

#include "IRValueTypes.h"
#include "NullLiteralType.h"
#include "mlir/IR/BuiltinTypes.h"
#include "llvm/Support/raw_ostream.h"

#include "NLRuntimeProperty.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

NLTruthColumnKind truthColumnKindFromChunkType(mlir::Type chunkType) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (isUntypedNullChunkType(chunkType)) {
        return NLTruthColumnKind::UntypedNull;
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLTruthColumnKind::TaggedCells;
    } else if (mlir::isa<storage::NullableType>(elementType)) {
        return NLTruthColumnKind::NullableBooleans;
    }

    return NLTruthColumnKind::Booleans;
}

}

ValueType db::valueTypeFromElementType(mlir::Type elementType) {
    // A null literal carries no type of its own, and a column has to have one to be
    // laid out: it is carried as a null integer, which every reader sees as the
    // absent value it is - the count that skips it, the key that groups on it
    if (mlir::isa<mlir::NoneType>(elementType)) {
        return ValueType::Int64;
    }

    const ValueType valueType = elementTypeToValueType(elementType);
    if (valueType == ValueType::Invalid) {
        throw IRException("Unsupported nullable value chunk element type");
    }

    return valueType;
}

ValueType db::nullableChunkValueType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const auto nullableType = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());
    if (!nullableType) {
        throw IRException("Expected a nullable value chunk");
    }

    return valueTypeFromElementType(nullableType.getValueType());
}

bool db::isNullableChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);

    return chunk && mlir::isa<storage::NullableType>(chunk.getElementType());
}

bool db::isUntypedNullChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);

    return chunk && mlir::isNullLiteralType(chunk.getElementType());
}

bool db::isAnyChunk(mlir::Type chunkType) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    return mlir::isa<storage::AnyType>(elementType);
}

bool db::isNullableList(mlir::Type elementType) {
    const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType);

    return nullableType && mlir::isa<storage::ListType>(nullableType.getValueType());
}

bool db::isNullableMap(mlir::Type elementType) {
    const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType);

    return nullableType && mlir::isa<storage::MapType>(nullableType.getValueType());
}

ValueType db::valueTypeFromChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return valueTypeFromElementType(nullableType.getValueType());
    }

    return valueTypeFromElementType(elementType);
}

bool db::isConstantColumn(const Column* column) {
    return column->getContainerKind() == ContainerKind::code<ColumnConst>();
}

bool db::holdsTruthColumn(NLTruthColumnKind kind, const Column* column) {
    const ColumnKind::Code columnKind = column->getKind();

    switch (kind) {
        case NLTruthColumnKind::Booleans:
            return columnKind == ColumnVector<CustomBool>::staticKind() || columnKind == ColumnConst<CustomBool>::staticKind();
        break;

        case NLTruthColumnKind::NullableBooleans:
            return columnKind == ColumnOptVector<CustomBool>::staticKind() || columnKind == ColumnConst<std::optional<CustomBool>>::staticKind();
        break;

        case NLTruthColumnKind::TaggedCells:
            return columnKind == ColumnVector<CellView>::staticKind() || columnKind == ColumnConst<CellView>::staticKind();
        break;

        case NLTruthColumnKind::UntypedNull:
            return true;
        break;
    }

    return false;
}

NLTruthColumnKind db::truthColumnKindOf(mlir::Type chunkType, const Column* column) {
    const NLTruthColumnKind kind = truthColumnKindFromChunkType(chunkType);

    if (!holdsTruthColumn(kind, column)) {
        std::string chunkTypeText;
        llvm::raw_string_ostream stream(chunkTypeText);
        chunkType.print(stream);

        throw IRException(fmt::format("A {} truth value is read out of a {}, not the column its type names",
                                      chunkTypeText,
                                      column->getTypeName()));
    }

    return kind;
}
