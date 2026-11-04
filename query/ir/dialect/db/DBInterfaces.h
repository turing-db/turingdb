#pragma once

#include <stddef.h>

#include "mlir/IR/OpDefinition.h"
#include "llvm/ADT/SmallVector.h"

namespace mlir::db {

// _count operands from _operandOffset, each coming back as the result at the same position
// from _resultOffset
struct CarrySet {
    size_t _operandOffset {0};
    size_t _resultOffset {0};
    size_t _count {0};
};

// The operand of @param op's carry set that @param result comes back from, null for a result
// outside it or an op with none
Value carrySetRowSource(Operation* op, OpResult result);

// Appends the row source of @param value where it is a result that has one
void collectRowSource(Value value, llvm::SmallVectorImpl<Value>& carried);

}

namespace mlir::OpTrait {

template <typename ConcreteType>
class KeepsRowsByPosition : public TraitBase<ConcreteType, KeepsRowsByPosition> {
};

}

#include "DBInterfaces.h.inc"
