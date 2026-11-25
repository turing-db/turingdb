#pragma once

#include "DBOps.h"

namespace mlir::db {

void fillTrailingRegionCarrySet(Operation* op, Region& region, size_t count, RegionCarrySet& carrySet);

void collectCarriedResult(Value value, size_t firstCarriedResult, ValueRange carriedColumns, llvm::SmallVectorImpl<Value>& carried);

void collectCarriedArgument(Value value, ValueRange carriedColumns, llvm::SmallVectorImpl<Value>& carried);

// The db.yield that terminates a factor region, or a null Yield if the region
// is empty or does not end with one. A factor's yield names the columns that
// factor contributes to the product.
Yield getFactorYield(Region& factor);

}
