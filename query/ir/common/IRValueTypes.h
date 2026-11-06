#pragma once

#include <stdint.h>

#include "metadata/PropertyType.h"

namespace mlir {
class DictionaryAttr;
class MLIRContext;
class OpBuilder;
class Type;
}

namespace mlir::storage {
enum class GraphImportFormat : uint64_t;
}

namespace db {

// Map a stored property value type to the MLIR element type baked into the nullable value
// chunk a property read carries. The element only has to round-trip back to this value type
// during translation, so each kind takes a distinct builtin.
mlir::Type valueTypeToElementType(mlir::OpBuilder& builder, ValueType valueType);

// The inverse of valueTypeToElementType, over a list of any element: ValueType::Invalid
// for an element no value column is laid out for
ValueType elementTypeToValueType(mlir::Type elementType);

// Whether each entry of @param types, absent or not, is a TypeAttr of an element type a
// property value column is laid out for
bool namesPropertyElementTypes(mlir::DictionaryAttr types);

// Whether each entry of @param types, absent or not, is a TypeAttr of a type a @param format
// import reads a property as where the file spells none: a datetime, duration or point from
// JSONL, a duration from Parquet
bool namesImportedPropertyTypes(mlir::storage::GraphImportFormat format, mlir::DictionaryAttr types);

// The element type an unwind of @param sourceElement produces: a drained list gives up the
// type its own elements carry, falling back to the type-erased !storage.any when they share
// none, and any other source keeps its own element, its cells being the elements themselves
mlir::Type unwoundElementType(mlir::MLIRContext* context, mlir::Type sourceElement);

}
