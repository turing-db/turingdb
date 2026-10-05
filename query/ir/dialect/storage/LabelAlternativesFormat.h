#pragma once

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

namespace mlir {

// Custom assembly-format directive shared by db.check_label_constraint and
// nl.check_label_constraint. A single conjunction is written flat, `["Person", "Founder"]`;
// several alternatives are written nested, `[["Person", "Founder"], ["Interest"]]`.
ParseResult parseLabelAlternatives(OpAsmParser& parser, ArrayAttr& alternatives);

void printLabelAlternatives(OpAsmPrinter& printer, Operation* op, ArrayAttr alternatives);

}
