#pragma once

#include "mlir/IR/Attributes.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Types.h"

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

}
