#pragma once

#include "mlir/IR/OpDefinition.h"
#include "mlir/IR/Types.h"

namespace mlir {
class Value;
}

namespace mlir::TypeTrait {

// A type holding one value per row of a relation, as opposed to a handle standing for
// one thing the whole query long - a resolved property name, a row budget
template <typename ConcreteType>
class CarriesRows : public TraitBase<ConcreteType, CarriesRows> {};

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
// is another chunk of the step that produced it, or it is computed row by row over
// chunks which all are - a constant or a handle brings no rows of its own, so neither is
// walked. A step which drops, reorders or repeats rows carries no such trait either.
bool rowAlignedWith(mlir::Value column, mlir::Value reference);

}
