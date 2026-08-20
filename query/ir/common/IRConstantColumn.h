#pragma once

#include "mlir/IR/OpDefinition.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"

#include "llvm/ADT/STLExtras.h"

namespace mlir::OpTrait {

// An op such that const arguments => const results
template <typename ConcreteType>
class ConstantThroughOperands
    : public TraitBase<ConcreteType, ConstantThroughOperands> {};

}

namespace db {

inline bool yieldsConstantColumn(mlir::Value value) {
    mlir::Operation* const definingOp = value.getDefiningOp();
    if (!definingOp) {
        return false;
    }

    if (definingOp->hasTrait<mlir::OpTrait::ConstantLike>()) {
        return true;
    }

    if (!definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>()) {
        return false;
    }

    return llvm::all_of(definingOp->getOperands(),
                        [](mlir::Value operand) { return yieldsConstantColumn(operand); });
}

}
