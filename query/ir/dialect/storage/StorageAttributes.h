#pragma once

#include "mlir/IR/Attributes.h"
#include "mlir/IR/BuiltinAttributeInterfaces.h"

#define GET_ATTRDEF_CLASSES
#include "StorageAttributes.h.inc"

namespace mlir::storage {

// The attribute of an argument of a program's main function naming the parameter the
// argument is bound to
inline constexpr llvm::StringLiteral PARAMETER_NAME_ATTRIBUTE = "storage.name";

}
