#pragma once

#include "mlir/IR/Attributes.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/Types.h"

#include "StorageTypes.h"

namespace db {

inline mlir::Type sharedLiteralElementType(mlir::ArrayAttr elements);

// The type one literal list element carries: its own for a typed attribute, and the list
// type its own verdict names for a nested list. Null where the element carries no type at
// all, which a null (a unit attr) and an embedding (a dense f32 array attr) do not.
inline mlir::Type literalElementType(mlir::Attribute element) {
    if (const auto nested = mlir::dyn_cast<mlir::ArrayAttr>(element)) {
        mlir::MLIRContext* const context = element.getContext();
        const mlir::Type shared = sharedLiteralElementType(nested);
        const mlir::Type nestedElement = shared ? shared
                                               : mlir::storage::ListElementType::get(context);

        return mlir::storage::ListType::get(context, nestedElement);
    }

    const auto typed = mlir::dyn_cast<mlir::TypedAttr>(element);

    return typed ? typed.getType() : mlir::Type {};
}

// The homogeneity verdict over a literal list's elements: the one type they all carry, or
// null when they disagree - or when any of them carries none at all. An empty list has no
// type to read either.
//
// A null verdict is the type-erased form, a list of tagged scalars. This is what both
// dialects' constant ops read to infer the column a list literal produces.
inline mlir::Type sharedLiteralElementType(mlir::ArrayAttr elements) {
    if (elements.empty()) {
        return nullptr;
    }

    const mlir::Type firstType = literalElementType(elements[0]);
    if (!firstType) {
        return nullptr;
    }

    for (const mlir::Attribute element : elements) {
        if (literalElementType(element) != firstType) {
            return nullptr;
        }
    }

    return firstType;
}

}
