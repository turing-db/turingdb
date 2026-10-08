#pragma once

#include "metadata/PropertyType.h"

namespace mlir {
class MLIRContext;
class OpBuilder;
class Type;
}

namespace db {

// Map a stored property value type to the MLIR element type baked into the nullable value
// chunk a property read carries. The element only has to round-trip back to this value type
// during translation, so each kind takes a distinct builtin.
mlir::Type valueTypeToElementType(mlir::OpBuilder& builder, ValueType valueType);

// The element type an unwind of @param sourceElement produces: a drained list gives up the
// type its own elements carry, falling back to the type-erased list_element when they share
// none, and any other source keeps its own element, its cells being the elements themselves
mlir::Type unwoundElementType(mlir::MLIRContext* context, mlir::Type sourceElement);

}
