#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/Support/LogicalResult.h"

namespace mlir {

class Operation;

// Verifies the shape of a MERGE path pattern, shared by db.merge and nl.merge: one
// entry per chain node in the two node lists, one per hop in the three hop lists, and
// an operand group whose size each of those lists accounts for. The two ops carry the
// same chain over different column types, so only the counts are checked here.
LogicalResult verifyMergePattern(Operation* op,
                                 ArrayAttr nodeLabels,
                                 ArrayAttr nodePropNames,
                                 ArrayAttr edgeTypes,
                                 ArrayAttr edgePropNames,
                                 DenseI64ArrayAttr edgeDirections,
                                 DenseI64ArrayAttr pendingNodes,
                                 size_t boundNodes,
                                 size_t boundPending,
                                 size_t nodePropValues,
                                 size_t edgePropValues);

// How many of @param nodeLabels' chain nodes the merge looks up rather than takes a
// bound column for: the ones carrying a label set.
size_t mergeMatchedNodeCount(ArrayAttr nodeLabels);

// The result count a merge of @param nodeLabels' chain produces: an ID column and a
// pending mask per looked-up chain node and per hop, the created mask, then one per
// carried column.
size_t mergeResultCount(ArrayAttr nodeLabels, size_t carriedColumns);

}
