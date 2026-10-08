#pragma once

#include <stddef.h>

#include "mlir/IR/Region.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"

namespace mlir {

// The arguments of the hop region of step @param step of an explore_paths, shared by the
// db and nl ops: the walk of one repetition up to the step's end node, node 2i and edge
// 2i + 1, then one argument per hop import.
size_t hopWalkArgumentCount(size_t step);
unsigned hopSourceArgument(size_t step);
unsigned hopEdgeArgument(size_t step);
unsigned hopEndArgument(size_t step);

bool hasHopPredicate(MutableArrayRef<Region> hops);

void buildHopArgumentTypes(size_t step,
                           Type nodeType,
                           Type edgeType,
                           ValueRange imports,
                           llvm::SmallVectorImpl<Type>& argumentTypes);

}
