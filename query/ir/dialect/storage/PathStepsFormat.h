#pragma once

#include <memory>

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

namespace mlir {

// Custom assembly-format directives shared by db.explore_paths and nl.explore_paths for the
// body of steps a walk repeats. A body of one step prints as the hop it is - `forward`,
// `["KNOWS"]`, its region bare - and a longer one as lists: `[forward, backward]`,
// `[["KNOWS"], []]`, then one region per step, `{}` for a step without a predicate. A body
// without any predicate prints no region at all.
ParseResult parsePathDirections(OpAsmParser& parser, DenseI64ArrayAttr& directions);

void printPathDirections(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr directions);

ParseResult parseStepNames(OpAsmParser& parser, ArrayAttr& names);

void printStepNames(OpAsmPrinter& printer, Operation* op, ArrayAttr names);

ParseResult parseHopRegions(OpAsmParser& parser,
                            llvm::SmallVectorImpl<std::unique_ptr<Region>>& hops,
                            DenseI64ArrayAttr directions);

void printHopRegions(OpAsmPrinter& printer, Operation* op, MutableArrayRef<Region> hops, DenseI64ArrayAttr directions);

}
