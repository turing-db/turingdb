#pragma once

#include "mlir/IR/OpDefinition.h"
#include "llvm/ADT/DenseMap.h"

namespace mlir {
class Operation;
class Value;
}

namespace mlir::OpTrait {

// An op such that const arguments => const results, as long as it cannot raise: one that
// may is computed over the rows reaching it, which need not be any
template <typename ConcreteType>
class ConstantThroughOperands
    : public TraitBase<ConcreteType, ConstantThroughOperands> {};

// An op whose result holds one value for every row, whatever its operands hold
template <typename ConcreteType>
class ConstantResult
    : public TraitBase<ConcreteType, ConstantResult> {};

}

namespace db {

// Whether @param value holds the same value in every row: a constant, or a computation
// over constants alone
bool yieldsConstantColumn(mlir::Value value);

// The same question answered against @param classified, which holds what an earlier call
// already decided: a caller asking it of every op of a block would otherwise rewalk the
// whole cone under each one.
bool yieldsConstantColumn(mlir::Value value, llvm::DenseMap<mlir::Value, bool>& classified);

// Whether @param operation, and every op nested in it, runs on any value it reads without
// raising, so it can run where no row has asked for it
bool runsWithoutRaising(mlir::Operation* operation);

}
