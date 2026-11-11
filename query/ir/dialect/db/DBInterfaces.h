#pragma once

#include <stddef.h>

#include "mlir/IR/OpDefinition.h"
#include "llvm/ADT/SmallVector.h"

namespace mlir::db {

// _count operands from _operandOffset, each coming back as the result at the same position
// from _resultOffset. An op that _repeatsRows may give one operand row several result rows,
// as a hop gives one per edge.
struct CarrySet {
    size_t _operandOffset {0};
    size_t _resultOffset {0};
    size_t _count {0};
    bool _repeatsRows {false};
};

// _count operands from _operandOffset, each coming into a region as the argument at the same
// position from _argumentOffset. An op that _handsBack those columns leads its results with
// them: as the region left them where it _yieldsBack, the region then leading its yield with
// them, and as the operands hold them otherwise. An op that _repeatsRows may run the region
// more than once for one operand row, as a body runs once per element of a list.
struct RegionCarrySet {
    size_t _operandOffset {0};
    size_t _argumentOffset {0};
    size_t _count {0};
    bool _handsBack {false};
    bool _yieldsBack {false};
    bool _repeatsRows {false};
};

// The operand of @param op's carry set that @param result comes back from, null for a result
// outside it or an op with none
Value carrySetRowSource(Operation* op, OpResult result);

// Appends the row source of @param value where it is a result that has one
void collectRowSource(Value value, llvm::SmallVectorImpl<Value>& carried);

// The carry set, or the carry set into its regions, of an op that does not size its
// operand segments
MutableOperandRange unsegmentedCarrySet(Operation* op);
MutableOperandRange unsegmentedRegionCarrySet(Operation* op);

// The index of the first operand of @param run, which may be empty
size_t operandRunOffset(const MutableOperandRange& run);

// The number of leading arguments of @param region holding the columns the regions before it
// forward, which is also where the columns @param region forwards start in a later region
size_t forwardedArgumentCount(Region& region);

// Sets @param source to the region forwarding the column @param argument takes, and
// @param forwardIndex to its place among the columns @param source forwards; false for an
// argument no region forwards
bool matchForwardedArgument(BlockArgument argument, Region*& source, size_t& forwardIndex);

bool matchRegionCarrySet(Region& region, RegionCarrySet& carrySet);

// The operand @param argument is carried in from; null for an argument the op binds itself: a
// list element, an accumulator or a row tag
Value carriedColumnOf(BlockArgument argument);

// The operand whose cells @param argument takes the elements of; null for an argument binding
// no element
Value elementSourceOf(BlockArgument argument);

}

namespace mlir::OpTrait {

template <typename ConcreteType>
class KeepsRowsByPosition : public TraitBase<ConcreteType, KeepsRowsByPosition> {
};

}

#include "DBInterfaces.h.inc"
