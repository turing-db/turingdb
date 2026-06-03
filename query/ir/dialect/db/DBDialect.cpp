#include "DBDialect.h"
#include "DBOps.h"

using namespace mlir;
using namespace mlir::turing;

#include "DBDialect.cpp.inc"

void TuringDB::initialize() {
    addOperations<
#define GET_OP_LIST
#include "DBOps.cpp.inc"
    >();
}
