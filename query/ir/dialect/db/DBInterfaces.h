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

// _count operands from _operandOffset, each coming into a region as the argument at the same
// position from _argumentOffset. An op that _handsBack those columns leads its results with
// them: as the region left them where it _yieldsBack, the region then leading its yield with
// them, and as the operands hold them otherwise.
struct RegionCarrySet {
    size_t _operandOffset {0};
    size_t _argumentOffset {0};
    size_t _count {0};
    bool _handsBack {false};
    bool _yieldsBack {false};
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
