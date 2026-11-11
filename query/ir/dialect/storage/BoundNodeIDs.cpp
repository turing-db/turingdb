#include "BoundNodeIDs.h"

#include "StorageTypes.h"

using namespace mlir;

bool mlir::storage::isBoundNodeIDsType(Type type) {
    return type.isSignlessInteger(64) || isa<storage::NodeIDType, storage::ListType>(type);
}

bool mlir::storage::isNodeIDOfInputType(Type type) {
    return !isa<storage::EdgeTypeIDType, storage::PathType, storage::PathRefType>(type);
}
