#include "IRConstantColumn.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

using namespace db;

namespace {

// What @param operation reads: its operands, and the values its regions take from above it.
// It looks only inside @param operation: codegen asks it of a body no op holds yet, and
// climbing above one, as mlir::getUsedValuesDefinedAbove does, asserts
void collectReadValues(mlir::Operation* operation, llvm::SmallVectorImpl<mlir::Value>& reads) {
    reads.append(operation->operand_begin(), operation->operand_end());

    llvm::SmallPtrSet<mlir::Region*, 4> innerRegions;
    operation->walk([&innerRegions](mlir::Operation* nested) {
        for (mlir::Region& region : nested->getRegions()) {
            innerRegions.insert(&region);
        }
    });

    operation->walk([operation, &innerRegions, &reads](mlir::Operation* nested) {
        if (nested == operation) {
            return;
        }

        for (mlir::Value operand : nested->getOperands()) {
            if (!innerRegions.contains(operand.getParentRegion())) {
                reads.push_back(operand);
            }
        }
    });
}

struct ConeValue {
    mlir::Value _value;
    size_t _reader {0};
};

bool isConstantLike(mlir::Value value) {
    mlir::Operation* const definingOp = value.getDefiningOp();

    return isFunctionArgument(value)
        || (definingOp
            && (definingOp->hasTrait<mlir::OpTrait::ConstantLike>() || definingOp->hasTrait<mlir::OpTrait::ConstantResult>()));
}

// Whether @param value is computed from its operands alone, so it holds one value where they do
bool dependsOnOperands(mlir::Value value) {
    mlir::Operation* const definingOp = value.getDefiningOp();

    return !isConstantLike(value)
        && definingOp
        && definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>()
        && mlir::isSpeculatable(definingOp);
}

// The cone is walked breadth first, so the read holding rows nearest @param root decides it, and every
// value on the way, however deep the cone runs elsewhere
bool isConstantColumn(mlir::Value root, llvm::DenseMap<mlir::Value, bool>& classified) {
    llvm::SmallVector<ConeValue> cone {ConeValue {._value = root}};
    llvm::DenseSet<mlir::Value> visited {root};
    llvm::SmallVector<mlir::Value> reads;

    for (size_t coneIndex = 0; coneIndex < cone.size(); coneIndex++) {
        const mlir::Value value = cone[coneIndex]._value;

        bool holdsRows = false;
        const auto classifiedIt = classified.find(value);
        if (classifiedIt != classified.end()) {
            holdsRows = !classifiedIt->second;
        } else if (!dependsOnOperands(value)) {
            holdsRows = !isConstantLike(value);
            classified[value] = !holdsRows;
        } else {
            reads.clear();
            collectReadValues(value.getDefiningOp(), reads);

            for (const mlir::Value read : reads) {
                if (visited.insert(read).second) {
                    cone.push_back(ConeValue {._value = read, ._reader = coneIndex});
                }
            }
        }

        if (!holdsRows) {
            continue;
        }

        for (size_t onTheWay = coneIndex; onTheWay > 0; onTheWay = cone[onTheWay]._reader) {
            classified[cone[cone[onTheWay]._reader]._value] = false;
        }

        return false;
    }

    for (const ConeValue& coneValue : cone) {
        classified[coneValue._value] = true;
    }

    return true;
}

}

bool db::isFunctionArgument(mlir::Value value) {
    const mlir::BlockArgument argument = mlir::dyn_cast<mlir::BlockArgument>(value);
    return argument && mlir::isa_and_present<mlir::func::FuncOp>(argument.getOwner()->getParentOp());
}

bool db::yieldsConstantColumn(mlir::Value value) {
    llvm::DenseMap<mlir::Value, bool> classified;
    return isConstantColumn(value, classified);
}

bool db::yieldsConstantColumn(mlir::Value value, llvm::DenseMap<mlir::Value, bool>& classified) {
    return isConstantColumn(value, classified);
}

void db::forgetErasedValues(mlir::Operation* operation, llvm::DenseMap<mlir::Value, bool>& classified) {
    operation->walk([&classified](mlir::Operation* nested) {
        for (const mlir::Value result : nested->getResults()) {
            classified.erase(result);
        }

        for (mlir::Region& region : nested->getRegions()) {
            for (mlir::Block& block : region) {
                for (const mlir::Value argument : block.getArguments()) {
                    classified.erase(argument);
                }
            }
        }
    });
}
