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

// Whether @param op takes rows from its columns and every column it reads is a constant,
// against @param classified as yieldsConstantColumn reads it: such an op runs over one row
bool readsConstantColumnsAlone(Operation* op, llvm::DenseMap<Value, bool>& classified);

}
