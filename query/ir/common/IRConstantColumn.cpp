#include "IRConstantColumn.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "llvm/ADT/SmallVector.h"

using namespace db;

namespace {

// What @param operation reads: its operands, and the values its regions take from above it
void collectReadValues(mlir::Operation* operation, llvm::SmallVectorImpl<mlir::Value>& reads) {
    reads.append(operation->operand_begin(), operation->operand_end());

    operation->walk([operation, &reads](mlir::Operation* nested) {
        if (nested == operation) {
            return;
        }

        for (mlir::Value operand : nested->getOperands()) {
            if (!operation->isAncestor(operand.getParentRegion()->getParentOp())) {
                reads.push_back(operand);
            }
        }
    });
}

bool isConstantColumn(mlir::Value root, llvm::DenseMap<mlir::Value, bool>& classified) {
    llvm::SmallVector<mlir::Value> worklist {root};
    llvm::SmallVector<mlir::Value> reads;
    llvm::SmallVector<mlir::Value> pendingOperands;

    while (!worklist.empty()) {
        const mlir::Value value = worklist.back();
        if (classified.contains(value)) {
            worklist.pop_back();
            continue;
        }

        mlir::Operation* const definingOp = value.getDefiningOp();
        const bool isConstantLike = definingOp
                                 && (definingOp->hasTrait<mlir::OpTrait::ConstantLike>()
                                     || definingOp->hasTrait<mlir::OpTrait::ConstantResult>());
        const bool constantThroughOperands = definingOp
                                          && definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>()
                                          && runsWithoutRaising(definingOp);
        const bool dependsOnOperands = !isConstantLike && constantThroughOperands;

        if (!dependsOnOperands) {
            classified[value] = isConstantLike;
            worklist.pop_back();
            continue;
        }

        reads.clear();
        collectReadValues(definingOp, reads);

        pendingOperands.clear();
        bool readsANonConstant = false;
        for (const mlir::Value operand : reads) {
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

bool db::runsWithoutRaising(mlir::Operation* operation) {
    const mlir::WalkResult walked = operation->walk([](mlir::Operation* nested) {
        return mlir::isSpeculatable(nested) ? mlir::WalkResult::advance() : mlir::WalkResult::interrupt();
    });

    return !walked.wasInterrupted();
}
