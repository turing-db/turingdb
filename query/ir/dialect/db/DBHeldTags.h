#pragma once

#include <initializer_list>

#include "CellTag.h"

namespace mlir {
class Attribute;
class Value;
}

namespace db {

// Whether every row of @param column holds null or a value of one of @param tags. A column
// of type-erased cells is followed back to where its values were made: through the ops that
// carry it, and from a key or an element read off it to the value it was read out of.
bool holdsOnlyNullOr(mlir::Value column, std::initializer_list<CellTag> tags);

// Whether every row of @param column holds null, a map, a node or an edge: a value a key reads
bool holdsOnlyKeyedValues(mlir::Value column);

// Whether every row of @param column holds null or a list: a value a position reads
bool holdsOnlyLists(mlir::Value column);

// The tag of the cell holding @param literal; INVALID for a literal no cell holds
CellTag literalTag(mlir::Attribute literal);

}
