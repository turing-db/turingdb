#pragma once

#include "mlir/IR/BuiltinAttributeInterfaces.h"

#include "llvm/ADT/StringRef.h"

namespace mlir::storage {

// The literal kinds a property column can be scanned against, and the only ones the query
// language spells: an integer, a double, a boolean or a string. Shared by
// db.scan_nodes_by_property_value and its nl sibling so a literal the one accepts never
// fails to verify at the other.
bool isPropertyScanLiteral(TypedAttr literal);

// What both verifiers name when the literal is of another kind.
llvm::StringRef propertyScanLiteralKinds();

}
