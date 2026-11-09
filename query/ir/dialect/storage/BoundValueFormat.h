#pragma once

#include <optional>

#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/OpImplementation.h"

namespace mlir {

// Custom assembly-format directives shared by db.load_csv, db.vector_search and their nl
// siblings for a value the query spells or binds when it runs: the literal, `"people.csv"`,
// `3` or `[1.0, 0.0]`, or the bound value and its type, `%path : !db.column<!storage.string>`.
ParseResult parseCSVPath(OpAsmParser& parser,
                         StringAttr& path,
                         std::optional<OpAsmParser::UnresolvedOperand>& boundPath,
                         Type& boundType);

void printCSVPath(OpAsmPrinter& printer, Operation* op, StringAttr path, Value boundPath, Type boundType);

ParseResult parseNeighbourCount(OpAsmParser& parser,
                                IntegerAttr& count,
                                std::optional<OpAsmParser::UnresolvedOperand>& boundCount,
                                Type& boundType);

void printNeighbourCount(OpAsmPrinter& printer, Operation* op, IntegerAttr count, Value boundCount, Type boundType);

ParseResult parseQueryVector(OpAsmParser& parser,
                             DenseF32ArrayAttr& queryVector,
                             std::optional<OpAsmParser::UnresolvedOperand>& boundQueryVector,
                             Type& boundType);

void printQueryVector(OpAsmPrinter& printer, Operation* op, DenseF32ArrayAttr queryVector, Value boundQueryVector, Type boundType);

}
