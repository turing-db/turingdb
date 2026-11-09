#pragma once

#include <optional>

#include "mlir/IR/BuiltinAttributeInterfaces.h"
#include "mlir/IR/OpImplementation.h"

#include "llvm/ADT/StringRef.h"

namespace mlir::storage {

// The literal kinds a property column can be scanned against, and the only ones the query
// language spells: an integer, a double, a boolean or a string. Shared by
// db.scan_nodes_by_property_value and its nl sibling so a literal the one accepts never
// fails to verify at the other.
bool isPropertyScanLiteral(TypedAttr literal);
bool isPropertyScanType(Type type);

// What both verifiers name when the literal is of another kind.
llvm::StringRef propertyScanLiteralKinds();

}

namespace mlir {

// Custom assembly-format directive shared by db.scan_nodes_by_property_value and its nl
// sibling for the value they scan for: the literal, `32 : i64`, or the value bound when the
// query runs and its type, `%age : !db.column<i64>`.
ParseResult parsePropertyScanValue(OpAsmParser& parser,
                                   TypedAttr& value,
                                   std::optional<OpAsmParser::UnresolvedOperand>& boundValue,
                                   Type& boundType);

void printPropertyScanValue(OpAsmPrinter& printer, Operation* op, TypedAttr value, Value boundValue, Type boundType);

}
