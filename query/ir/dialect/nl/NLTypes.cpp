#include "NLTypes.h"

#include "NLDialect.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

using namespace mlir::nl;

#define GET_TYPEDEF_CLASSES
#include "NLTypes.cpp.inc"

void NL::registerTypes() {
    addTypes<
#define GET_TYPEDEF_LIST
#include "NLTypes.cpp.inc"
    >();
}
