#include "DBTypes.h"

#include "DBDialect.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

using namespace mlir::turing;

#define GET_TYPEDEF_CLASSES
#include "DBTypes.cpp.inc"

void TuringDB::registerTypes() {
    addTypes<
#define GET_TYPEDEF_LIST
#include "DBTypes.cpp.inc"
    >();
}
