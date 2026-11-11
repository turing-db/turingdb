#pragma once

#include "mlir/Support/LogicalResult.h"

namespace mlir {
class Operation;
}

namespace mlir::db {

// Whether lowering lays @param op out over the rows in flight of its block: it reads no column
// of rows, holds more than one value, and is no source and no terminator
bool readsTheRowsInFlight(Operation* op);

// Fails where an op reads the rows in flight of its block after an op opened them: nothing
// but the order of the block would then tie it to the rows it runs over, and a pass erasing
// the op that opened them as unread would change its rows
LogicalResult verifyRowDependence(Operation* root);

}
