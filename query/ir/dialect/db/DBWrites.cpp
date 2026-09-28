#include "DBWrites.h"

#include "mlir/IR/Operation.h"

#include "DBOps.h"

using namespace mlir;
using namespace mlir::db;

bool mlir::db::isWriteOp(Operation* op) {
    return isa<CreateNode,
               CreateEdge,
               Merge,
               SetNodeProperty,
               SetEdgeProperty,
               SetNodeProperties,
               SetEdgeProperties,
               DeleteNode,
               DeleteEdge>(op);
}

bool mlir::db::writesBetween(Operation* from, Operation* to) {
    bool afterFrom = false;
    bool crossesAWrite = false;

    Operation* root = to;
    while (Operation* const parent = root->getParentOp()) {
        root = parent;
    }

    root->walk<WalkOrder::PreOrder>([&](Operation* op) {
        if (op == to) {
            return WalkResult::interrupt();
        } else if (afterFrom && isWriteOp(op)) {
            crossesAWrite = true;
            return WalkResult::interrupt();
        } else if (op == from) {
            afterFrom = true;
        }

        return WalkResult::advance();
    });

    return crossesAWrite;
}
