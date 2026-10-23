#pragma once

#include <optional>

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/Types.h"
#include "mlir/Support/LogicalResult.h"
#include "llvm/ADT/ArrayRef.h"

namespace mlir {

class Operation;

// Verifies the operands of a path build, shared by db.make_path and nl.make_path: it opens
// on a node or on a reversed walk, then each edge lands on a node, a forward walk stands
// alone, and a reversed walk closes on the node it seeded from. The two ops carry the same
// operands over different column types, so @param elementTypes holds their element types.
LogicalResult verifyMakePath(Operation* op,
                             llvm::ArrayRef<Type> elementTypes,
                             std::optional<ArrayAttr> reversedPaths);

}
