#include "DBDialect.h"
#include "DBOps.h"

using namespace mlir;
using namespace mlir::db;

#include "DBDialect.cpp.inc"

void DB::initialize() {
    addOperations<
#define GET_OP_LIST
#include "DBOps.cpp.inc"
    >();
    registerTypes();
}
