#pragma once

namespace mlir {

class Operation;

namespace db {

bool isWriteOp(Operation* op);

// Whether a write stands after @param from and before @param to in program order
bool writesBetween(Operation* from, Operation* to);

}

}
