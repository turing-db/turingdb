#include "IRConstantColumn.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/STLExtras.h"

using namespace db;

bool db::yieldsConstantColumn(mlir::Value value) {
    mlir::Operation* const definingOp = value.getDefiningOp();

    if (!definingOp) {
        return false;
    } else if (definingOp->hasTrait<mlir::OpTrait::ConstantLike>()) {
        return true;
    } else if (!definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>()) {
        return false;
    }

    return llvm::all_of(definingOp->getOperands(),
                        [](mlir::Value operand) { return yieldsConstantColumn(operand); });
}
