#include "DBRegionColumns.h"

using namespace mlir;
using namespace mlir::db;

void mlir::db::fillTrailingRegionCarrySet(Operation* op, Region& region, size_t count, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._operandOffset = op->getNumOperands() - count,
                               ._argumentOffset = region.getNumArguments() - count,
                               ._count = count};
}

void mlir::db::collectCarriedResult(Value value, size_t firstCarriedResult, ValueRange carriedColumns, llvm::SmallVectorImpl<Value>& carried) {
    const OpResult result = dyn_cast<OpResult>(value);
    if (!result || result.getResultNumber() < firstCarriedResult) {
        return;
    }

    const size_t carriedIndex = result.getResultNumber() - firstCarriedResult;
    if (carriedIndex < carriedColumns.size()) {
        carried.push_back(carriedColumns[carriedIndex]);
    }
}

void mlir::db::collectCarriedArgument(Value value, ValueRange carriedColumns, llvm::SmallVectorImpl<Value>& carried) {
    const BlockArgument argument = dyn_cast<BlockArgument>(value);
    if (argument && argument.getArgNumber() < carriedColumns.size()) {
        carried.push_back(carriedColumns[argument.getArgNumber()]);
    }
}

Yield mlir::db::getFactorYield(Region& factor) {
    Operation* terminator = nullptr;
    if (!factor.empty()) {
        Block& block = factor.front();
        if (!block.empty()) {
            terminator = &block.back();
        }
    }

    return dyn_cast_or_null<Yield>(terminator);
}
