#pragma once

#include "mlir/IR/Attributes.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/Types.h"

namespace db {

// The homogeneity verdict over a literal list's elements: the one type they all carry, or
// null when they disagree - or when any of them carries none at all, which a null (a unit
// attr) and a nested list (an array attr) do not. An empty list has no type to read either.
//
// A null verdict is the type-erased form, a list of tagged scalars. An array attribute
// carries no type of its own, so this is what both dialects' constant ops read to infer the
// column a list literal produces.
inline mlir::Type sharedLiteralElementType(mlir::ArrayAttr elements) {
    if (elements.empty()) {
        return nullptr;
    }

    const auto first = mlir::dyn_cast<mlir::TypedAttr>(elements[0]);
    if (!first) {
        return nullptr;
    }

    const mlir::Type firstType = first.getType();
    for (const mlir::Attribute element : elements) {
        const auto typedElement = mlir::dyn_cast<mlir::TypedAttr>(element);
        if (!typedElement || typedElement.getType() != firstType) {
            return nullptr;
        }
    }

    return firstType;
}

}
