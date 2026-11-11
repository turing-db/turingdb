#pragma once

#include "mlir/IR/Types.h"

namespace mlir::storage {

// The types a const node scan reads the IDs bound when the query runs from: an integer or a
// node, the one node it names, or a list, whose numbers are the IDs it lists. Shared by
// db.const_scan_nodes and its nl sibling.
bool isBoundNodeIDsType(Type type);

// The types node_id_of reads: any value, which names the node whose ID it equals or none, a
// node, which names itself, and an edge, which an ID equals by its number. A path has no ID.
// Shared by db.node_id_of and its nl sibling.
bool isNodeIDOfInputType(Type type);

}
