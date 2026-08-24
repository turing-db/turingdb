#pragma once

#include "mlir/IR/OpDefinition.h"

namespace mlir {
class Value;
}

namespace mlir::OpTrait {

// An op computing one row of its result per row of its operands, so its result carries
// the rows of whichever operand it was computed over
template <typename ConcreteType>
class RowAlignedThroughOperands
    : public TraitBase<ConcreteType, RowAlignedThroughOperands> {};

}

namespace db {

// Whether @param column holds one row per row of @param reference: it is that chunk, it
// is another chunk of the step that produced it, or it is computed row by row over one
// that is. A step which drops, reorders or repeats rows carries no such trait, so the
// walk stops there and the column stands on its own.
bool rowAlignedWith(mlir::Value column, mlir::Value reference);

}
