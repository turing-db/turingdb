#include "DBWrittenOrder.h"

#include "mlir/IR/Visitors.h"
#include "llvm/ADT/STLExtras.h"

#include "DBOps.h"

using namespace mlir;
using namespace mlir::db;

bool mlir::db::isGraphWrite(Operation* op) {
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

DBWrittenOrder::DBWrittenOrder()
{
}

DBWrittenOrder::~DBWrittenOrder() {
}

void DBWrittenOrder::build(Operation* function) {
    function->walk([this](Operation* op, const mlir::WalkStage& stage) {
        if (stage.isBeforeAllRegions()) {
            const size_t position = _count++;
            _positions[op] = position;

            if (isGraphWrite(op)) {
                _writePositions.push_back(position);
            }
        }

        if (stage.isAfterAllRegions() && op->getNumRegions() > 0) {
            _lastHeldPositions[op] = _count - 1;
        }
    });
}

bool DBWrittenOrder::writesBetween(Operation* first, Operation* last) const {
    if (_writePositions.empty()) {
        return false;
    }

    const auto lastHeld = _lastHeldPositions.find(first);
    const size_t afterFirst = lastHeld != _lastHeldPositions.end() ? lastHeld->second : _positions.at(first);
    const size_t lastPosition = _positions.at(last);

    const size_t* const firstWriteAfter = llvm::upper_bound(_writePositions, afterFirst);

    return firstWriteAfter != _writePositions.end() && *firstWriteAfter < lastPosition;
}
