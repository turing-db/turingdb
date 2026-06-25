#include "StorageDialect.h"
#include "StorageTypes.h"

using namespace mlir;
using namespace mlir::storage;

#include "StorageDialect.cpp.inc"

void Storage::initialize() {
    registerTypes();
}
