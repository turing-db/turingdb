#include "IRConstantColumn.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/STLExtras.h"

using namespace db;

namespace {

bool isConstantColumn(mlir::Value value, llvm::DenseMap<mlir::Value, bool>* classified) {
    if (classified) {
        const auto classifiedIt = classified->find(value);
        if (classifiedIt != classified->end()) {
            return classifiedIt->second;
        }
    }

    mlir::Operation* const definingOp = value.getDefiningOp();

    bool isConstant = false;
    if (definingOp) {
        if (definingOp->hasTrait<mlir::OpTrait::ConstantLike>()) {
            isConstant = true;
        } else if (definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>()) {
            isConstant = llvm::all_of(definingOp->getOperands(),
                                      [classified](mlir::Value operand) { return isConstantColumn(operand, classified); });
        }
    }

    if (classified) {
        (*classified)[value] = isConstant;
    }

    return isConstant;
}

}

bool db::yieldsConstantColumn(mlir::Value value) {
    return isConstantColumn(value, nullptr);
}

bool db::yieldsConstantColumn(mlir::Value value, llvm::DenseMap<mlir::Value, bool>& classified) {
    return isConstantColumn(value, &classified);
}
