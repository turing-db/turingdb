#pragma once

#include <stddef.h>
#include <stdint.h>

#include "mlir/IR/Attributes.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Types.h"

#include "NullLiteralType.h"
#include "StorageTypes.h"

namespace db {

inline mlir::Type sharedLiteralElementType(mlir::ArrayAttr elements);

// The type one literal list element carries: its own for a typed attribute, and the list
// type its own verdict names for a nested list. A null names none, and an embedding (a
// dense f32 array attr) or a map carries no type a list element is read under. A string
// written in IR text parses with no type of its own, and is a string all the same.
inline mlir::Type literalElementType(mlir::Attribute element) {
    mlir::MLIRContext* const context = element.getContext();

    if (const auto nested = mlir::dyn_cast<mlir::ArrayAttr>(element)) {
        return mlir::storage::ListType::get(context, sharedLiteralElementType(nested));
    } else if (mlir::isa<mlir::UnitAttr>(element)) {
        return mlir::NoneType::get(context);
    } else if (mlir::isa<mlir::StringAttr>(element)) {
        return mlir::storage::StringType::get(context);
    }

    const auto typed = mlir::dyn_cast<mlir::TypedAttr>(element);

    return typed ? typed.getType() : mlir::storage::AnyType::get(context);
}

// The homogeneity verdict over a literal list's elements, which both dialects' constant
// ops read: the type the elements' types join to. A null names none, so [1, null] is a
// list of integers and [] a list of none.
inline mlir::Type sharedLiteralElementType(mlir::ArrayAttr elements) {
    mlir::Type sharedType = mlir::NoneType::get(elements.getContext());

    for (const mlir::Attribute element : elements) {
        sharedType = mlir::storage::joinedElementType(sharedType, literalElementType(element));
    }

    return sharedType;
}

// Sets @param index to the element a list of @param count elements holds at @param position,
// which counts from the end where it is negative. False past either end, where it reads null.
inline bool listPositionIndex(size_t count, int64_t position, size_t& index) {
    const int64_t signedCount = static_cast<int64_t>(count);
    const int64_t fromStart = position < 0 ? signedCount + position : position;

    if (fromStart < 0 || fromStart >= signedCount) {
        return false;
    }

    index = static_cast<size_t>(fromStart);
    return true;
}

// The element a constant holding @param literal names: a list for an array, an embedding for
// a dense f32 array, a map for a dictionary, and the type of a typed attribute. Null for the
// unit a null is, which names none.
inline mlir::Type literalValueType(mlir::Attribute literal) {
    mlir::MLIRContext* const context = literal.getContext();

    if (const auto elements = mlir::dyn_cast<mlir::ArrayAttr>(literal)) {
        return mlir::storage::ListType::get(context, sharedLiteralElementType(elements));
    } else if (mlir::isa<mlir::DenseF32ArrayAttr>(literal)) {
        return mlir::storage::EmbeddingType::get(context);
    } else if (mlir::isa<mlir::DictionaryAttr>(literal)) {
        return mlir::storage::MapType::get(context);
    }

    const auto typed = mlir::dyn_cast<mlir::TypedAttr>(literal);

    return typed ? typed.getType() : mlir::Type {};
}

// Whether a tagged cell holds @param literal: a scalar, an embedding, a list, a map or the unit
// a null is. Not the null literal, whose empty string a cell would hold as a string
inline bool isCellLiteral(mlir::Attribute literal) {
    const auto typed = mlir::dyn_cast<mlir::TypedAttr>(literal);
    if (typed && mlir::isNullLiteralType(typed.getType())) {
        return false;
    }

    return mlir::isa<mlir::IntegerAttr,
                     mlir::FloatAttr,
                     mlir::StringAttr,
                     mlir::DenseF32ArrayAttr,
                     mlir::UnitAttr,
                     mlir::ArrayAttr,
                     mlir::DictionaryAttr>(literal);
}

}
