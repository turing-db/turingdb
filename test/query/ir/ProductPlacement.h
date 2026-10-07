#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinOps.h"

namespace turing::test {

// Filters over a cross product whose mask reads the columns of one factor alone
size_t countFiltersOverOneFactor(mlir::ModuleOp module);

// Unwinds carrying a column a cross product made, which repeat every row of the product
size_t countUnwindsOverAProduct(mlir::ModuleOp module);

}
