#include "IRValueTypes.h"

#include "mlir/IR/Builders.h"

#include "StorageTypes.h"

#include "IRException.h"

using namespace db;

namespace {

// The list element types an unwind can drain into a column of that very type: the entity
// IDs, the value types a nullable value chunk is laid out for, and a nested list, which
// drains into a list column one level shallower. An unresolved element, an embedding, or
// the list_element a heterogeneous list holds drains as tagged scalars instead - none of
// them names a column shape the drain could fill.
bool drainsToItsOwnElementType(mlir::Type listElement) {
    if (mlir::isa<mlir::storage::NodeIDType, mlir::storage::EdgeIDType, mlir::storage::StringType, mlir::storage::ListType>(listElement)) {
        return true;
    } else if (mlir::isa<mlir::Float64Type>(listElement)) {
        return true;
    }

    const auto intType = mlir::dyn_cast<mlir::IntegerType>(listElement);

    return intType && (intType.getWidth() == 1 || intType.getWidth() == 64);
}

}

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
            return mlir::storage::ListType::get(builder.getContext(), builder.getNoneType());
        break;

        case ValueType::DateTime:
            return mlir::storage::DateTimeType::get(builder.getContext());
        break;

        case ValueType::Duration:
            return mlir::storage::DurationType::get(builder.getContext());
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

mlir::Type db::unwoundElementType(mlir::MLIRContext* context, mlir::Type sourceElement) {
    // A cell that may be absent - what an index into a list hands back - contributes no
    // row where it is absent, so what the drain hands on is the tagged scalar itself.
    const auto nullableSource = mlir::dyn_cast<mlir::storage::NullableType>(sourceElement);
    const bool drainsATaggedCell = nullableSource
                                && mlir::isa<mlir::storage::ListElementType>(nullableSource.getValueType());

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

    // The elements of a list whose type is known are that type, so the unwind hands the
    // rest of the query a column it can read as one - a node stays a node, an integer an
    // integer. Only a list whose elements share no such type drains into the type-erased
    // column of tagged scalars.
    const mlir::Type listElement = listType.getElementType();
    if (!drainsToItsOwnElementType(listElement)) {
        return mlir::storage::ListElementType::get(context);
    }

    // An entity ID column spells a null entity as an invalid ID, so an entity rides a
    // plain chunk; a value or a nested list, either of which may be a tagged null, rides
    // the nullable one every value-chunk consumer dispatches on, as lowerUnwindConst's
    // homogeneous list does.
    if (mlir::isa<mlir::storage::NodeIDType, mlir::storage::EdgeIDType>(listElement)) {
        return listElement;
    }

    return mlir::storage::NullableType::get(context, listElement);
}
