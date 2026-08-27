#pragma once

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

namespace mlir {

// Custom assembly-format directive shared by db.collect and nl.collect_buffer for the
// indices naming which of their value columns dedupe. It prints and parses the list in
// brackets - `[0, 2]` - rather than the builtin `array<i64: 0, 2>` spelling, so it reads
// like the aggregate-kind list beside it while the ops still store a DenseI64ArrayAttr.
ParseResult parseColumnIndices(OpAsmParser& parser, DenseI64ArrayAttr& indices);

void printColumnIndices(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr indices);

}
