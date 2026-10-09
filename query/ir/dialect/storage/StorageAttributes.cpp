#include "StorageAttributes.h"

#include "StorageDialect.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

using namespace mlir::storage;

#define GET_ATTRDEF_CLASSES
#include "StorageAttributes.cpp.inc"

mlir::LogicalResult ParameterAttr::verify(llvm::function_ref<mlir::InFlightDiagnostic()> emitError,
                                          llvm::StringRef name,
                                          mlir::Type type) {
    if (name.empty()) {
        return emitError() << "a parameter needs a name";
    }

    return mlir::success();
}

void Storage::registerAttributes() {
    addAttributes<
#define GET_ATTRDEF_LIST
#include "StorageAttributes.cpp.inc"
    >();
}
