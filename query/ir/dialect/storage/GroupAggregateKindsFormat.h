#pragma once

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

namespace mlir {

// Custom assembly-format directive shared by db.group_aggregate and
// nl.group_aggregate_buffer for their aggregate-kind list. It prints and parses the
// list as GroupAggregateKind keywords - `[count, sum, max]` rather than the raw
// `[0, 1, 3]` - while the ops still store it as a compact DenseI64ArrayAttr. The
// keyword <-> integer mapping reuses the symbolize/stringify functions generated for
// the storage-dialect GroupAggregateKind enum, so the two stay in sync by
// construction.
ParseResult parseGroupAggregateKinds(OpAsmParser& parser, DenseI64ArrayAttr& kinds);

void printGroupAggregateKinds(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr kinds);

}
