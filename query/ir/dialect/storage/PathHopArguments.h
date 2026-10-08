#pragma once

#include <stddef.h>
#include <stdint.h>

#include "mlir/IR/Operation.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Support/LogicalResult.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/SmallVector.h"

namespace mlir {

// The arguments of the hop region of step @param step of an explore_paths, shared by the
// db and nl ops: the walk of one repetition up to the step's end node, node 2i and edge
// 2i + 1, then one argument per hop import.
size_t hopWalkArgumentCount(size_t step);
unsigned hopSourceArgument(size_t step);
unsigned hopEdgeArgument(size_t step);
unsigned hopEndArgument(size_t step);

// The earliest step whose hop region takes the walk argument at @param walkPosition: the
// first step takes its source, and every step its edge and end node
size_t firstStepTakingHopArgument(size_t walkPosition);

bool hasHopPredicate(MutableArrayRef<Region> hops);

void buildHopArgumentTypes(size_t step,
                           Type nodeType,
                           Type edgeType,
                           ValueRange imports,
                           llvm::SmallVectorImpl<Type>& argumentTypes);

LogicalResult verifyPathSteps(Operation* op, llvm::ArrayRef<int64_t> directions, size_t regionCount);

LogicalResult verifyHopArguments(Operation* op,
                                 Block& block,
                                 size_t step,
                                 Type nodeType,
                                 Type edgeType,
                                 ValueRange imports);

}
