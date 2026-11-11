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

Operation* DB::materializeConstant(OpBuilder& builder, Attribute value, Type type, Location loc) {
    if (!ConstantOp::isBuildableWith(value, type)) {
        return nullptr;
    }

    return builder.create<ConstantOp>(loc, type, value);
}
