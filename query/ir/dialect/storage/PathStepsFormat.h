#pragma once

#include <memory>

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

namespace mlir {

// Assembly-format directives of db.explore_paths and nl.explore_paths: a body of one step
// prints as the hop it is (`forward`, `["KNOWS"]`, its region bare), a longer one as lists
// with one region per step, `{}` for a step without a predicate.
ParseResult parsePathDirections(OpAsmParser& parser, DenseI64ArrayAttr& directions);

void printPathDirections(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr directions);

ParseResult parseStepNames(OpAsmParser& parser, ArrayAttr& names);

void printStepNames(OpAsmPrinter& printer, Operation* op, ArrayAttr names);

ParseResult parseHopRegions(OpAsmParser& parser,
                            llvm::SmallVectorImpl<std::unique_ptr<Region>>& hops,
                            DenseI64ArrayAttr directions);

void printHopRegions(OpAsmPrinter& printer, Operation* op, MutableArrayRef<Region> hops, DenseI64ArrayAttr directions);

}
