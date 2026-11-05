#include "StorageDialect.h"

#include "mlir/Interfaces/FunctionInterfaces.h"

#include "StorageAttributes.h"
#include "StorageTypes.h"

using namespace mlir;
using namespace mlir::storage;

#include "StorageDialect.cpp.inc"

void Storage::initialize() {
    registerTypes();
    registerAttributes();
}

LogicalResult Storage::verifyRegionArgAttribute(Operation* op,
                                                unsigned regionIndex,
                                                unsigned argIndex,
                                                NamedAttribute attribute) {
    if (attribute.getName() != PARAMETER_NAME_ATTRIBUTE) {
        return op->emitError() << "unknown storage argument attribute " << attribute.getName();
    }

    const StringAttr name = dyn_cast<StringAttr>(attribute.getValue());
    if (!name || name.getValue().empty()) {
        return op->emitError() << "storage.name of argument " << argIndex << " must be a non-empty string";
    }

    auto function = dyn_cast<FunctionOpInterface>(op);
    if (!function) {
        return op->emitError() << "storage.name names an argument of a function";
    }

    for (size_t other = 0; other < argIndex; other++) {
        if (function.getArgAttr(static_cast<unsigned>(other), PARAMETER_NAME_ATTRIBUTE) == name) {
            return op->emitError() << "storage.name '" << name.getValue() << "' of argument " << argIndex
                                   << " is also the name of argument " << other;
        }
    }

    return success();
}
