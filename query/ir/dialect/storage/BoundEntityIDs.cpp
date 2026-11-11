#include "BoundEntityIDs.h"

#include "StorageTypes.h"

using namespace mlir;

bool mlir::storage::isEntityIDType(Type type) {
    return isa<storage::NodeIDType, storage::EdgeIDType>(type);
}

bool mlir::storage::isBoundEntityIDsType(Type type, Type entity) {
    return type.isSignlessInteger(64) || type == entity || isa<storage::ListType>(type);
}

bool mlir::storage::isEntityIDOfInputType(Type type) {
    return !isa<storage::EdgeTypeIDType, storage::PathType, storage::PathRefType>(type);
}
