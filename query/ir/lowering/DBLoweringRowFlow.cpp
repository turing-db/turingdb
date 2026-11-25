#include "DBLoweringRowFlow.h"

#include "DBOps.h"

using namespace db;

namespace {

bool isFirstOf(const mlir::MutableOperandRange& operands, mlir::OpOperand& operand) {
    return !operands.empty() && &operands[0] == &operand;
}

}

bool db::runsPerRow(mlir::Operation* operation) {
    if (mlir::db::CallSubquery call = mlir::dyn_cast<mlir::db::CallSubquery>(operation)) {
        return !call.getCarriesScope();
    } else if (mlir::db::ExistsSubquery exists = mlir::dyn_cast<mlir::db::ExistsSubquery>(operation)) {
        return !exists.getCarriesScope();
    } else if (mlir::db::CountSubquery count = mlir::dyn_cast<mlir::db::CountSubquery>(operation)) {
        return !count.getCarriesScope();
    } else if (mlir::db::CollectSubquery collect = mlir::dyn_cast<mlir::db::CollectSubquery>(operation)) {
        return !collect.getCarriesScope();
    }

    return false;
}

bool db::readsAheadOfTheRows(mlir::OpOperand& operand) {
    mlir::Operation* const owner = operand.getOwner();
    if (mlir::isa<mlir::db::ConstScanNodes, mlir::db::ConstScanEdges, mlir::db::ScanNodesByPropertyValue>(owner)) {
        return true;
    } else if (mlir::db::Limit limit = mlir::dyn_cast<mlir::db::Limit>(owner)) {
        return isFirstOf(limit.getBoundCountMutable(), operand);
    } else if (mlir::db::Skip skip = mlir::dyn_cast<mlir::db::Skip>(owner)) {
        return isFirstOf(skip.getBoundCountMutable(), operand);
    } else if (mlir::db::ExplorePaths exploration = mlir::dyn_cast<mlir::db::ExplorePaths>(owner)) {
        return isFirstOf(exploration.getEndNodesMutable(), operand);
    }

    return false;
}
