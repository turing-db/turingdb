#pragma once

#include <initializer_list>

#include "llvm/ADT/ArrayRef.h"

#include "CellTag.h"

namespace mlir {
class Value;
class StringAttr;
}

namespace db {

// Whether every row of @param column holds null or a value of one of @param tags. A column
// of type-erased cells is followed back to where its values were made: through the ops that
// carry it, and from a key or an element read off it to the value it was read out of.
bool holdsOnlyNullOr(mlir::Value column, std::initializer_list<CellTag> tags);

// The same of what @param reads, taken in order off each row's value, arrive at: the value
// under a key of a map, or each element of a list where the key is null
bool readsHoldOnlyNullOr(mlir::Value column,
                         llvm::ArrayRef<mlir::StringAttr> reads,
                         std::initializer_list<CellTag> tags);

}
