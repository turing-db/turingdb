#pragma once

#include "mlir/IR/OpDefinition.h"

namespace mlir {
class Value;
}

namespace mlir::OpTrait {

// An op such that const arguments => const results
template <typename ConcreteType>
class ConstantThroughOperands
    : public TraitBase<ConcreteType, ConstantThroughOperands> {};

}

namespace db {

// Whether @param value holds the same value in every row: a constant, or a computation
// over constants alone
bool yieldsConstantColumn(mlir::Value value);

}
