#include "IRRowAlignment.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"

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
// the rows they hold, so the walk ends on a chunk a step bound. Null where the column
// brings no rows of its own - every operand a constant or a handle - or where its
// operands disagree, which is IR no step could run.
mlir::Value rowSource(mlir::Value column) {
    mlir::Operation* const definingOp = column.getDefiningOp();

    const bool computedRowByRow = definingOp
                               && definingOp->hasTrait<mlir::OpTrait::RowAlignedThroughOperands>();

    if (!computedRowByRow) {
        return column;
    }

    mlir::Value source;

    for (const mlir::Value operand : definingOp->getOperands()) {
        const bool isHandle = !operand.getType().hasTrait<mlir::TypeTrait::CarriesRows>();
        const bool standsForEveryRow = yieldsConstantColumn(operand);

        if (isHandle || standsForEveryRow) {
            continue;
        }

        const mlir::Value operandSource = rowSource(operand);

        if (!operandSource) {
            return {};
        }

        if (source && !sameRowSource(source, operandSource)) {
            return {};
        }

        source = operandSource;
    }

    return source;
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
