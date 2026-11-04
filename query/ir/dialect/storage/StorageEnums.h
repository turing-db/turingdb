#pragma once

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

#include "StorageEnums.h.inc"

namespace mlir::storage {

// The standard deviations reduce numbers of any type to a double, and so does avg. avg of
// durations is a duration, and avg of type-erased cells a cell holding either.
bool reducesToADouble(AggregateKind kind, Type values);
bool reducesToADouble(GroupAggregateKind kind, Type values);

}
