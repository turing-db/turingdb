#include "DBRowDependence.h"

#include "mlir/IR/Operation.h"
#include "mlir/Interfaces/ControlFlowInterfaces.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "DBInterfaces.h"
#include "DBTypes.h"
#include "IRConstantColumn.h"

using namespace mlir;

namespace {

// The values @param op reads: its operands, and the values its regions take from above it
void collectReadValues(Operation* op, llvm::SmallVectorImpl<Value>& reads) {
    reads.append(op->operand_begin(), op->operand_end());

    op->walk([op, &reads](Operation* nested) {
        if (nested == op) {
            return;
        }

        for (Value operand : nested->getOperands()) {
            if (!op->isAncestor(operand.getParentRegion()->getParentOp())) {
                reads.push_back(operand);
            }
        }
    });
}

bool isRowColumn(Value value, llvm::DenseMap<Value, bool>& constantColumns) {
    return isa<mlir::db::ColumnType>(value.getType()) && !::db::yieldsConstantColumn(value, constantColumns);
}

// A constant, a value computed once over constants, or a handle standing for one thing the
// whole query long
bool holdsOneValue(Operation* op, llvm::DenseMap<Value, bool>& constantColumns) {
    if (op->hasTrait<OpTrait::ConstantLike>() || op->hasTrait<OpTrait::ConstantResult>()) {
        return true;
    }

    if (op->getNumResults() == 0) {
        return false;
    }

    return llvm::all_of(op->getResults(), [&constantColumns](Value result) {
        return !isRowColumn(result, constantColumns);
    });
}

bool readsARowColumn(Operation* op, llvm::DenseMap<Value, bool>& constantColumns) {
    llvm::SmallVector<Value, 8> reads;
    collectReadValues(op, reads);

    return llvm::any_of(reads, [&constantColumns](Value read) {
        return isRowColumn(read, constantColumns);
    });
}

bool isLaidOutOverTheRowsInFlight(Operation* op, llvm::DenseMap<Value, bool>& constantColumns) {
    const bool ownsNoRows = op->hasTrait<OpTrait::ReadsNoRowsInFlight>() || op->hasTrait<OpTrait::ReturnLike>();
    if (ownsNoRows || holdsOneValue(op, constantColumns)) {
        return false;
    }

    return !readsARowColumn(op, constantColumns);
}

}

bool mlir::db::readsTheRowsInFlight(Operation* op) {
    llvm::DenseMap<Value, bool> constantColumns;

    return isLaidOutOverTheRowsInFlight(op, constantColumns);
}

LogicalResult mlir::db::verifyRowDependence(Operation* root) {
    llvm::DenseMap<Value, bool> constantColumns;

    const WalkResult walked = root->walk([&constantColumns](Block* block) {
        Operation* rowsInFlight = nullptr;

        for (Operation& op : *block) {
            if (rowsInFlight && isLaidOutOverTheRowsInFlight(&op, constantColumns)) {
                op.emitOpError("reads no column of the rows ") << rowsInFlight->getName() << " opened";
                return WalkResult::interrupt();
            }

            // An op opening rows and handing back none leaves the single row in flight where
            // it ran over no input, and the rows it read where it did
            if (!op.hasTrait<OpTrait::OpensRows>()) {
                continue;
            } else if (op.getNumResults() > 0) {
                rowsInFlight = &op;
            } else if (!readsARowColumn(&op, constantColumns)) {
                rowsInFlight = nullptr;
            }
        }

        return WalkResult::advance();
    });

    return failure(walked.wasInterrupted());
}
