#pragma once

#include <stddef.h>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"

namespace mlir::db {

OptionalYield patternYield(OptionalMatch optional);

// Rebuilds an optional match over the columns its yield names, which a pass has just added
// to or dropped some of. The old results keptResults lists stand, in order, for the first
// new ones.
OptionalMatch rebuildOptionalMatch(OptionalMatch optional, llvm::ArrayRef<size_t> keptResults, mlir::OpBuilder& builder);

size_t collectValueCount(Collect collect);

bool matchCarrySet(Operation* op, CarrySet& carrySet);

// The operands a trim drops in pairs with results: the carry set, and a grouping op's
// aggregate inputs with the aggregates they reduce to
MutableOperandRange trimmedOperandsOf(Operation* op);

Operation* rebuildOp(Operation* op, OperationState& state, mlir::OpBuilder& builder);

void trimCarrySet(Operation* op, const CarrySet& carrySet, llvm::ArrayRef<size_t> kept, mlir::OpBuilder& builder);

void keptResultTypes(Operation* op, const llvm::SmallBitVector& keep, llvm::SmallVectorImpl<Type>& types);

// Hands the two factors to the op built in its place, drops the columns their yields no
// longer name, and rewires every kept result. Shared by the cross product and the hash
// join, which differ only in the op the caller built.
void replaceWithTrimmedFactors(Operation* op,
                               Operation* trimmed,
                               const llvm::SmallBitVector& keep,
                               size_t leftCount);

bool matchRegionCarrySet(Region& region, RegionCarrySet& carrySet);

// Null for an argument the op binds itself: a list element, an accumulator or a row tag
Value carriedColumnOf(BlockArgument argument);

BlockArgument carriedArgument(Region& region, size_t carriedIndex);

// The position of @param column in the carry set of @param op into its regions, appended to
// the run where the op does not carry it yet: as an operand, and as an argument of every
// region with a body
size_t carryIntoRegions(Operation* op, Value column, mlir::RewriterBase& rewriter, bool& appended);

BlockArgument carryIntoRegion(Region& region, Value column, mlir::RewriterBase& rewriter);

// Widens an op's carry set by one column and hands back the result it comes out as. The
// column ends the run of operands and the run of results the carry set spans; an op's
// result count is fixed once built, hence the rebuild.
Value appendCarriedColumn(Operation* op, Value column, mlir::RewriterBase& rewriter);

}
