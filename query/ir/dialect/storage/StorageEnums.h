#pragma once

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

#include "StorageEnums.h.inc"

namespace mlir::storage {

// avg and the standard deviations reduce numbers of any type to a double
bool reducesToADouble(AggregateKind kind);

}
