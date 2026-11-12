#pragma once

#include <stddef.h>

#include "mlir/IR/Operation.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

namespace mlir::db {

bool isGraphWrite(Operation* op);

// The ops of a function numbered in the order they are written, an op before the ops it holds,
// so an op is written before another exactly when its position is lower
class DBWrittenOrder {
public:
    DBWrittenOrder();
    ~DBWrittenOrder();

    void build(Operation* function);

    // Whether a graph write lies after @param first and every op it holds, and before @param last
    bool writesBetween(Operation* first, Operation* last) const;

private:
    llvm::DenseMap<Operation*, size_t> _positions;
    llvm::DenseMap<Operation*, size_t> _lastHeldPositions;
    llvm::SmallVector<size_t> _writePositions;
    size_t _count {0};
};

}
