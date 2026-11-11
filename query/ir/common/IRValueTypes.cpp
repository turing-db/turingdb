#include "IRValueTypes.h"

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/Builders.h"

#include "NullLiteralType.h"
#include "StorageEnums.h"
#include "StorageTypes.h"

#include "IRException.h"

using namespace db;

mlir::Type db::valueTypeToElementType(mlir::OpBuilder& builder, ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return builder.getIntegerType(64);
        break;

        case ValueType::UInt64:
            return builder.getIntegerType(64, /*isSigned=*/false);
        break;

        case ValueType::Double:
            return builder.getF64Type();
        break;

        case ValueType::Bool:
            return builder.getI1Type();
        break;

        case ValueType::String:
            return mlir::storage::StringType::get(builder.getContext());
        break;

        case ValueType::Embedding:
            return mlir::storage::EmbeddingType::get(builder.getContext());
        break;

        case ValueType::List:
            return builder.getType<mlir::storage::ListType>(builder.getType<mlir::storage::AnyType>());
        break;

        case ValueType::DateTime:
            return mlir::storage::DateTimeType::get(builder.getContext());
        break;

        case ValueType::Duration:
            return mlir::storage::DurationType::get(builder.getContext());
        break;

        case ValueType::Point:
            return mlir::storage::PointType::get(builder.getContext());
        break;

        case ValueType::Map:
            return mlir::storage::MapType::get(builder.getContext());
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Invalid property value type");
        break;
    }

    throw IRException("Unhandled property value type");
}

ValueType db::elementTypeToValueType(mlir::Type elementType) {
    if (mlir::isa<mlir::storage::StringType>(elementType)) {
        return ValueType::String;
    } else if (mlir::isa<mlir::storage::EmbeddingType>(elementType)) {
        return ValueType::Embedding;
    } else if (mlir::isa<mlir::storage::ListType>(elementType)) {
        return ValueType::List;
    } else if (mlir::isa<mlir::storage::DateTimeType>(elementType)) {
        return ValueType::DateTime;
    } else if (mlir::isa<mlir::storage::DurationType>(elementType)) {
        return ValueType::Duration;
    } else if (mlir::isa<mlir::storage::PointType>(elementType)) {
        return ValueType::Point;
    } else if (mlir::isa<mlir::storage::MapType>(elementType)) {
        return ValueType::Map;
    } else if (mlir::isa<mlir::Float64Type>(elementType)) {
        return ValueType::Double;
    }

    const auto intType = mlir::dyn_cast<mlir::IntegerType>(elementType);
    if (!intType) {
        return ValueType::Invalid;
    } else if (intType.getWidth() == 1) {
        return ValueType::Bool;
    } else if (intType.isUnsigned()) {
        return ValueType::UInt64;
    }

    return ValueType::Int64;
}

bool db::namesPropertyElementTypes(mlir::DictionaryAttr types) {
    if (!types) {
        return true;
    }

    for (const mlir::NamedAttribute entry : types) {
        const mlir::TypeAttr typeAttr = mlir::dyn_cast<mlir::TypeAttr>(entry.getValue());
        const bool namesAnElementType = typeAttr && elementTypeToValueType(typeAttr.getValue()) != ValueType::Invalid;
        if (!namesAnElementType) {
            return false;
        }
    }

    return true;
}

bool db::namesImportedPropertyTypes(mlir::storage::GraphImportFormat format, mlir::DictionaryAttr types) {
    if (!types) {
        return true;
    }

    for (const mlir::NamedAttribute entry : types) {
        const mlir::TypeAttr typeAttr = mlir::dyn_cast<mlir::TypeAttr>(entry.getValue());
        if (!typeAttr) {
            return false;
        }

        const ValueType type = elementTypeToValueType(typeAttr.getValue());
        const bool readsJsonlType = format == mlir::storage::GraphImportFormat::Jsonl
                                 && (type == ValueType::DateTime || type == ValueType::Duration || type == ValueType::Point);
        const bool readsParquetType = format == mlir::storage::GraphImportFormat::Parquet && type == ValueType::Duration;

        if (!readsJsonlType && !readsParquetType) {
            return false;
        }
    }

    return true;
}

mlir::Type db::unwoundElementType(mlir::MLIRContext* context, mlir::Type sourceElement) {
    // A cell that may be absent - what an index into a list hands back - contributes no
    // row where it is absent, so what the drain hands on is the tagged scalar itself.
    const auto nullableSource = mlir::dyn_cast<mlir::storage::NullableType>(sourceElement);
    const bool drainsATaggedCell = nullableSource
                                && mlir::isa<mlir::storage::AnyType>(nullableSource.getValueType());

    if (drainsATaggedCell) {
        return nullableSource.getValueType();
    }

    // A list read out of a property rides a nullable chunk, the way every property value
    // does; its elements are the list's all the same, and a row holding no list drains
    // into no row rather than into a null.
    const mlir::Type unwrapped = nullableSource ? nullableSource.getValueType() : sourceElement;

    // Any source but a list keeps the column it already rides - its cells are the
    // elements, and a tagged cell holding a list gives up tagged scalars again.
    const auto listType = mlir::dyn_cast<mlir::storage::ListType>(unwrapped);
    if (!listType) {
        return sourceElement;
    }

    // A column that spells a null as a value of its own rides a plain chunk, and one of a
    // property value type or a schema ID the nullable chunk every value-chunk consumer
    // reads. Any other list, one of mixed elements or of nulls alone, drains into tagged
    // scalars.
    const mlir::Type listElement = listType.getElementType();
    const bool drainsToANullableChunk = elementTypeToValueType(listElement) != ValueType::Invalid
                                     || mlir::storage::namesASchemaID(listElement);
    if (mlir::holdsItsOwnNull(listElement)) {
        return listElement;
    } else if (!drainsToANullableChunk) {
        return mlir::storage::AnyType::get(context);
    }

    return mlir::storage::NullableType::get(context, listElement);
}
