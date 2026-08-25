#include "IRRowAlignment.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"

#include "IRConstantColumn.h"

using namespace db;

bool db::rowAlignedWith(mlir::Value column, mlir::Value reference) {
    if (column == reference) {
        return true;
    }

    mlir::Operation* const definingOp = column.getDefiningOp();

    if (!definingOp) {
        // A block argument is a column of a loop step, computed from nothing: two of them
        // are aligned when one loop binds both
        const mlir::BlockArgument columnArg = mlir::cast<mlir::BlockArgument>(column);
        const mlir::BlockArgument referenceArg = mlir::dyn_cast<mlir::BlockArgument>(reference);

        return referenceArg && columnArg.getOwner() == referenceArg.getOwner();
    } else if (definingOp == reference.getDefiningOp()) {
        // An op yields one chunk per result, each over the rows of the step it ran in
        return true;
    } else if (!definingOp->hasTrait<mlir::OpTrait::RowAlignedThroughOperands>()) {
        return false;
    }

    bool carriesRows = false;
    for (const mlir::Value operand : definingOp->getOperands()) {
        const bool isHandle = !operand.getType().hasTrait<mlir::TypeTrait::CarriesRows>();
        const bool standsForEveryRow = yieldsConstantColumn(operand);

        if (isHandle || standsForEveryRow) {
            continue;
        }

        if (!rowAlignedWith(operand, reference)) {
            return false;
        }

        carriesRows = true;
    }

    return carriesRows;
}
