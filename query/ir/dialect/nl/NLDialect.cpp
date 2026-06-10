#include "NLDialect.h"
#include "NLOps.h"

using namespace mlir;
using namespace mlir::nl;

#include "NLDialect.cpp.inc"

void NL::initialize() {
    addOperations<
#define GET_OP_LIST
#include "NLOps.cpp.inc"
    >();
    registerTypes();
}
