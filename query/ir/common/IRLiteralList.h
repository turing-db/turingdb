#pragma once

#include "mlir/IR/Attributes.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Types.h"

#include "StorageTypes.h"

namespace db {

inline mlir::Type sharedLiteralElementType(mlir::ArrayAttr elements);

// The type one literal carries, as a constant or as a list element: its own for a typed
// attribute, the list type its elements' verdict names for a list, an embedding for a dense
// f32 array and a map for a dictionary. A null names none. A string written in IR text
// parses with no type of its own, and is a string all the same.
inline mlir::Type literalType(mlir::Attribute literal) {
    mlir::MLIRContext* const context = literal.getContext();

    if (const auto nested = mlir::dyn_cast<mlir::ArrayAttr>(literal)) {
        return mlir::storage::ListType::get(context, sharedLiteralElementType(nested));
    } else if (mlir::isa<mlir::DenseF32ArrayAttr>(literal)) {
        return mlir::storage::EmbeddingType::get(context);
    } else if (mlir::isa<mlir::DictionaryAttr>(literal)) {
        return mlir::storage::MapType::get(context);
    } else if (mlir::isa<mlir::UnitAttr>(literal)) {
        return mlir::NoneType::get(context);
    } else if (mlir::isa<mlir::StringAttr>(literal)) {
        return mlir::storage::StringType::get(context);
    }

    const auto typed = mlir::dyn_cast<mlir::TypedAttr>(literal);

    return typed ? typed.getType() : mlir::storage::AnyType::get(context);
}

// The homogeneity verdict over a literal list's elements, which both dialects' constant
// ops read: the type the elements' types join to. A null names none, so [1, null] is a
// list of integers and [] a list of none.
inline mlir::Type sharedLiteralElementType(mlir::ArrayAttr elements) {
    mlir::Type sharedType = mlir::NoneType::get(elements.getContext());

    for (const mlir::Attribute element : elements) {
        sharedType = mlir::storage::joinedElementType(sharedType, literalType(element));
    }

    return sharedType;
}

}
