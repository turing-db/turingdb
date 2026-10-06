#include "IRRowAlignment.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"

using namespace db;

namespace {

// Whether two chunks computed from nothing else hold the rows of one step: the same
// chunk, two results of one op, or two columns one loop binds
bool sameRowSource(mlir::Value column, mlir::Value reference) {
    if (column == reference) {
        return true;
    }

    mlir::Operation* const definingOp = column.getDefiningOp();

    if (!definingOp) {
        const mlir::BlockArgument columnArg = mlir::cast<mlir::BlockArgument>(column);
        const mlir::BlockArgument referenceArg = mlir::dyn_cast<mlir::BlockArgument>(reference);

        return referenceArg && columnArg.getOwner() == referenceArg.getOwner();
    }

    return definingOp == reference.getDefiningOp();
}

// The chunk a column takes its rows from: one computed row by row over other chunks holds
// the rows they hold, so the walk ends on a chunk a step bound, or on one computed over no
// chunk at all - every operand a constant or a handle, as a CREATE over the single empty
// row. Null where its operands disagree, which is IR no step could run.
mlir::Value rowSource(mlir::Value column) {
    llvm::DenseMap<mlir::Value, mlir::Value> sources;
    llvm::DenseMap<mlir::Value, bool> classified;
    llvm::SmallVector<mlir::Value> worklist {column};

    const auto holdsRows = [&classified](mlir::Value operand) {
        const bool isHandle = !operand.getType().hasTrait<mlir::TypeTrait::CarriesRows>();
        return !isHandle && !yieldsConstantColumn(operand, classified);
    };

    while (!worklist.empty()) {
        const mlir::Value value = worklist.back();
        if (sources.contains(value)) {
            worklist.pop_back();
            continue;
        }

        mlir::Operation* const definingOp = value.getDefiningOp();
        const bool alignedWithFirstOperand = definingOp && definingOp->hasTrait<mlir::OpTrait::RowAlignedWithFirstOperand>();
        const bool computedRowByRow = definingOp && definingOp->hasTrait<mlir::OpTrait::RowAlignedThroughOperands>();

        if (alignedWithFirstOperand) {
            const mlir::Value operand = definingOp->getOperand(0);
            const auto operandIt = sources.find(operand);
            if (operandIt == sources.end()) {
                worklist.push_back(operand);
                continue;
            }

            sources[value] = operandIt->second;
        } else if (!computedRowByRow) {
            sources[value] = value;
        } else {
            bool operandsPending = false;
            for (const mlir::Value operand : definingOp->getOperands()) {
                if (holdsRows(operand) && !sources.contains(operand)) {
                    worklist.push_back(operand);
                    operandsPending = true;
                }
            }

            if (operandsPending) {
                continue;
            }

            mlir::Value source;
            bool operandsDisagree = false;
            for (const mlir::Value operand : definingOp->getOperands()) {
                if (!holdsRows(operand)) {
                    continue;
                }

                const mlir::Value operandSource = sources.at(operand);
                if (!operandSource || (source && !sameRowSource(source, operandSource))) {
                    operandsDisagree = true;
                    break;
                }

                source = operandSource;
            }

            if (operandsDisagree) {
                sources[value] = mlir::Value();
            } else {
                sources[value] = source ? source : value;
            }
        }

        worklist.pop_back();
    }

    return sources.at(column);
}

}

bool db::rowAlignedWith(mlir::Value column, mlir::Value reference) {
    if (column == reference) {
        return true;
    }

    const mlir::Value columnSource = rowSource(column);
    const mlir::Value referenceSource = rowSource(reference);

    if (!columnSource || !referenceSource) {
        return false;
    }

    return sameRowSource(columnSource, referenceSource);
}
