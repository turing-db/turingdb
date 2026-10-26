#pragma once

#include "mlir/IR/BuiltinTypes.h"

#include "StorageEnums.h"

#define GET_TYPEDEF_CLASSES
#include "StorageTypes.h.inc"

namespace mlir::storage {

// The element type of a list holding an element of each type. None, which no element
// names, joins to the other side and a list to a list element by element. A number is
// never promoted, since an element is read under its list's type, and any other pair
// joins to !storage.any.
Type joinedElementType(Type lhs, Type rhs);

}
