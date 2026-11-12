#include "DBRowCone.h"

#include "DBOps.h"
#include "IRConstantColumn.h"

using namespace mlir;
using namespace mlir::db;

namespace {

// One suspended operand loop of the cone walk: the op whose operands are being
// visited, and how many of them are done.
struct ConeFrame {
    Operation* _op {nullptr};
    unsigned _nextOperand {0};
};

}

bool mlir::db::raisesOnSomeRows(Operation* op) {
    return !::db::runsWithoutRaising(op);
}

bool mlir::db::isRowWiseOp(Operation* op) {
    if (BroadcastConstant broadcast = dyn_cast<BroadcastConstant>(op)) {
        return static_cast<bool>(broadcast.getDriver());
    }

    return isa<EqOp, NeqOp, GtOp, LtOp, GteOp, LteOp,
               StartsWithOp, EndsWithOp, ContainsOp, InOp,
               AndOp, OrOp, XorOp, NotOp, IsNullOp, IsNotNullOp,
               AddOp, SubOp, MulOp, DivOp, ModOp, PowOp, ConcatOp, Negate, UnaryPlus,
               ConstantOp,
               GetNodeProperties, GetEdgeProperties, ElementID,
               GetNodeLabelSet, CheckLabelConstraint, CheckEdgeTypeConstraint,
               Labels, EdgeType, ToInteger, ToFloat, ToBoolean,
               CosineSimilarity, EuclideanDistance>(op);
}

bool mlir::db::computesPerRow(Operation* op) {
    return isRowWiseOp(op) || isa<ListIndex, ContainerIndex, ToNullable, GetEdgeTypes>(op);
}

bool mlir::db::computesFromTheRow(Operation* op) {
    if (op->getNumRegions() != 0) {
        return false;
    }

    const bool computesAValue = op->hasTrait<OpTrait::ConstantLike>()
                                || op->hasTrait<OpTrait::ConstantThroughOperands>();

    return computesAValue || computesPerRow(op) || isa<Range, MakeList, MakeMap, ListSlice>(op);
}

void mlir::db::collectRowCone(llvm::ArrayRef<Value> roots, Block* block, MaskCone& cone) {
    const auto computedInTheBlock = [block](Operation* op) {
        return op->getBlock() == block && computesFromTheRow(op) && !raisesOnSomeRows(op);
    };

    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value root : roots) {
        collectConePostOrder(root, computedInTheBlock, visited, cone);
    }
}

void mlir::db::collectConePostOrder(Value mask,
                                    llvm::function_ref<bool(Operation*)> joinsTheCone,
                                    llvm::SmallPtrSet<Operation*, 8>& visited,
                                    MaskCone& cone) {
    Operation* const maskDef = mask.getDefiningOp();

    if (!maskDef || !joinsTheCone(maskDef)) {
        cone._inputs.insert(mask);
        return;
    }

    // A caller walking several conjuncts into one cone shares the set across the
    // calls, so a root already collected by an earlier conjunct is dropped here.
    if (!visited.insert(maskDef).second) {
        return;
    }

    llvm::SmallVector<ConeFrame> stack {ConeFrame {maskDef, 0}};
    while (!stack.empty()) {
        ConeFrame& frame = stack.back();

        if (frame._nextOperand == frame._op->getNumOperands()) {
            cone._ops.push_back(frame._op);
            stack.pop_back();
            continue;
        }

        const Value operand = frame._op->getOperand(frame._nextOperand);
        frame._nextOperand++;

        Operation* const operandDef = operand.getDefiningOp();
        if (!operandDef || !joinsTheCone(operandDef)) {
            cone._inputs.insert(operand);
            continue;
        }

        // A second path can only reach an op through a value that something else
        // also reads, so a singly-used result needs no membership check. An OR
        // chain is singly-used throughout and never touches the set at all.
        const bool reachableOnce = operandDef->getNumResults() == 1 && operand.hasOneUse();
        if (reachableOnce || visited.insert(operandDef).second) {
            stack.push_back(ConeFrame {operandDef, 0});
        }
    }
}
