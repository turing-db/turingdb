#pragma once

#include "mlir/IR/OpDefinition.h"
#include "llvm/ADT/DenseMap.h"

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

// The same question answered against @param classified, which holds what an earlier call
// already decided: a caller asking it of every op of a block would otherwise rewalk the
// whole cone under each one.
bool yieldsConstantColumn(mlir::Value value, llvm::DenseMap<mlir::Value, bool>& classified);

}
