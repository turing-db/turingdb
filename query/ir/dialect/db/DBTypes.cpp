#include "DBTypes.h"

#include "DBDialect.h"

using namespace mlir::turing;

#define GET_TYPEDEF_CLASSES
#include "DBTypes.cpp.inc"

void TuringDB::registerTypes() {
    addTypes<
#define GET_TYPEDEF_LIST
#include "DBTypes.cpp.inc"
    >();
}
