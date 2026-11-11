#include "BoundNodeIDs.h"

#include "StorageTypes.h"

using namespace mlir;

bool mlir::storage::isBoundNodeIDsType(Type type) {
    return type.isSignlessInteger(64) || type.isF64() || isa<storage::ListType>(type);
}
