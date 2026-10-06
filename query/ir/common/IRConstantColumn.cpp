#include "IRConstantColumn.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/SmallVector.h"

using namespace db;

namespace {

bool isConstantColumn(mlir::Value root, llvm::DenseMap<mlir::Value, bool>& classified) {
    llvm::SmallVector<mlir::Value> worklist {root};
    llvm::SmallVector<mlir::Value> pendingOperands;

    while (!worklist.empty()) {
        const mlir::Value value = worklist.back();
        if (classified.contains(value)) {
            worklist.pop_back();
            continue;
        }

        mlir::Operation* const definingOp = value.getDefiningOp();
        const bool isConstantLike = definingOp && definingOp->hasTrait<mlir::OpTrait::ConstantLike>();
        const bool dependsOnOperands = definingOp
                                    && !isConstantLike
                                    && definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>();

        if (!dependsOnOperands) {
            classified[value] = isConstantLike;
            worklist.pop_back();
            continue;
        }

        pendingOperands.clear();
        bool readsANonConstant = false;
        for (const mlir::Value operand : definingOp->getOperands()) {
            const auto classifiedIt = classified.find(operand);
            if (classifiedIt == classified.end()) {
                pendingOperands.push_back(operand);
            } else if (!classifiedIt->second) {
                readsANonConstant = true;
                break;
            }
        }

        if (readsANonConstant) {
            classified[value] = false;
            worklist.pop_back();
        } else if (pendingOperands.empty()) {
            classified[value] = true;
            worklist.pop_back();
        } else {
            worklist.append(pendingOperands);
        }
    }

    return classified.at(root);
}

}

bool db::yieldsConstantColumn(mlir::Value value) {
    llvm::DenseMap<mlir::Value, bool> classified;
    return isConstantColumn(value, classified);
}

bool db::yieldsConstantColumn(mlir::Value value, llvm::DenseMap<mlir::Value, bool>& classified) {
    return isConstantColumn(value, classified);
}
