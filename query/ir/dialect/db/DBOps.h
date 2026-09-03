#pragma once

#include "mlir/Bytecode/BytecodeOpInterface.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Dialect.h"
#include "mlir/IR/OpDefinition.h"
#include "mlir/IR/OpImplementation.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Interfaces/InferTypeOpInterface.h"

#include "DBDialect.h"
#include "DBTypes.h"

#include "IRConstantColumn.h"

#define GET_OP_CLASSES
#include "DBOps.h.inc"

namespace mlir::db {

// The literal kinds a property column can be scanned against, and the only ones the
// query language spells: an integer, a double, a boolean or a string.
bool isPropertyScanLiteral(TypedAttr literal);

}
