#pragma once

#include "metadata/PropertyType.h"

namespace mlir {
class OpBuilder;
class Type;
}

namespace db {

// Map a stored property value type to the MLIR element type baked into the nullable value
// chunk a property read carries. The element only has to round-trip back to this value type
// during translation, so each kind takes a distinct builtin.
mlir::Type valueTypeToElementType(mlir::OpBuilder& builder, ValueType valueType);

}
