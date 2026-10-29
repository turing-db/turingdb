#pragma once

#include <optional>

#include "mlir/IR/BuiltinTypes.h"

#include "StorageEnums.h"

#include "metadata/PropertyType.h"
#include "metadata/ValueTypeSet.h"

#define GET_TYPEDEF_CLASSES
#include "StorageTypes.h.inc"


namespace mlir::storage {

// The value type a column of @param elementType holds, or none for an element that is
// not a property value: an entity id, a label, a mask, a tagged cell
std::optional<::db::ValueType> valueTypeOf(mlir::Type elementType);

}
