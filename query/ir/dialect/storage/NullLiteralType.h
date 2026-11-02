#pragma once

#include "mlir/IR/Types.h"
#include "mlir/Support/LogicalResult.h"

namespace mlir {

class Operation;

// The element type the null literal is carried in: nullable, with no value type of its own
bool isNullLiteralType(Type elementType);

// Whether a column of @param elementType spells a null as a value of its own: the invalid ID
// of an entity, the tag of a cell, or the path with no node
bool holdsItsOwnNull(Type elementType);

// Whether a column of @param elementType is read only as a plain chunk of rows, never as a
// constant nor through a nullable: a node, an edge, a path, a walk's handle or a mask
bool isReadAsRows(Type elementType);

// Turns away an equality over the null literal, shared by db.eq, db.neq, nl.eq and nl.neq,
// which carry their operands over different column types: @param lhsElementType and
// @param rhsElementType are the element types of the two operands.
LogicalResult verifyComparesNoNullLiteral(Operation* op, Type lhsElementType, Type rhsElementType);

}
