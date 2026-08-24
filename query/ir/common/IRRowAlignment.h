#pragma once

#include "mlir/IR/OpDefinition.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"

#include "llvm/ADT/STLExtras.h"

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
inline bool rowAlignedWith(mlir::Value column, mlir::Value reference) {
    if (column == reference) {
        return true;
    }

    mlir::Operation* const definingOp = column.getDefiningOp();

    // A block argument is a column of a loop step, computed from nothing: two of them are
    // aligned when one loop binds both
    if (!definingOp) {
        const mlir::BlockArgument columnArg = mlir::cast<mlir::BlockArgument>(column);
        const mlir::BlockArgument referenceArg = mlir::dyn_cast<mlir::BlockArgument>(reference);

        return referenceArg && columnArg.getOwner() == referenceArg.getOwner();
    }

    // An op yields one chunk per result, each over the rows of the step it ran in
    if (definingOp == reference.getDefiningOp()) {
        return true;
    }

    if (!definingOp->hasTrait<mlir::OpTrait::RowAlignedThroughOperands>()) {
        return false;
    }

    return llvm::any_of(definingOp->getOperands(), [reference](mlir::Value operand) {
        return rowAlignedWith(operand, reference);
    });
}

}
