#pragma once

#include "mlir/IR/Types.h"

namespace mlir::storage {

// The types a const node scan reads the IDs bound when the query runs from: an integer,
// the one ID it names, or a list of integers, the IDs it lists. Shared by
// db.const_scan_nodes and its nl sibling.
bool isBoundNodeIDsType(Type type);

}
