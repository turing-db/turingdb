#pragma once

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

namespace mlir {

// Custom assembly-format directive shared by db.merge and nl.merge for the per-hop
// direction list of a MERGE pattern. It prints and parses the list as EdgeDirection
// keywords - `[forward, undirected]` rather than the raw `[2, 0]` - while the ops
// store it as a compact DenseI64ArrayAttr, the way the aggregate kinds are stored.
ParseResult parseEdgeDirections(OpAsmParser& parser, DenseI64ArrayAttr& directions);

void printEdgeDirections(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr directions);

}
