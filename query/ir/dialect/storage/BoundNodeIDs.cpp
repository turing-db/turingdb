#include "BoundNodeIDs.h"

#include "StorageTypes.h"

using namespace mlir;

bool mlir::storage::isBoundNodeIDsType(Type type) {
    const storage::ListType list = dyn_cast<storage::ListType>(type);
    const Type element = list ? list.getElementType() : type;

    return element.isSignlessInteger(64);
}
