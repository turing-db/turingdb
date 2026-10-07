#include "StorageAttributes.h"

#include "StorageDialect.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

using namespace mlir::storage;

#define GET_ATTRDEF_CLASSES
#include "StorageAttributes.cpp.inc"

void Storage::registerAttributes() {
    addAttributes<
#define GET_ATTRDEF_LIST
#include "StorageAttributes.cpp.inc"
    >();
}
