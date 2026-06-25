#include "StorageTypes.h"

#include "StorageDialect.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

using namespace mlir::storage;

#define GET_TYPEDEF_CLASSES
#include "StorageTypes.cpp.inc"

void Storage::registerTypes() {
    addTypes<
#define GET_TYPEDEF_LIST
#include "StorageTypes.cpp.inc"
    >();
}
