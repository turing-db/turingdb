#pragma once

#include "mlir/IR/Operation.h"

#include "NLProgram.h"

namespace db {

using NLUnaryFunctionSelector = NLUnaryFunctionKernel (*)(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

NLUnaryFunctionSelector lookupUnaryFunctionSelector(mlir::Operation& operation);

using NLBinaryFunctionSelector = NLBinaryFn (*)(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);

NLBinaryFunctionSelector lookupBinaryFunctionSelector(mlir::Operation& operation);

}
