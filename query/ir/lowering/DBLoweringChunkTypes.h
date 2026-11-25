#pragma once

#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

namespace db {

// The value element a chunk carries, with its nullability stripped and a mask read as the
// i1 it holds: what two chunks are compared on to see whether they carry the same values.
mlir::Type chunkValueElement(mlir::OpBuilder& builder, mlir::Type chunkType);

bool isNullableChunk(mlir::Type chunkType);

// A list column, read through the nullable a stored list wears: a property fetch produces
// its values nullable, since a node carrying no list reads as absent there.
bool isListChunk(mlir::Type chunkType);

// The element type an indexed list hands out, read through the nullable an optional list
// wears. None when the indexed operand is a type-erased cell rather than a list.
mlir::Type indexedListElementType(mlir::Type chunkType);

// The null literal's chunk: nullable with no value type of its own.
bool isUntypedNullChunk(mlir::Type chunkType);

bool isTaggedCellChunk(mlir::Type chunkType);

bool mayHoldANull(mlir::Type chunkType);

mlir::Type promoteNumeric(mlir::OpBuilder& builder, mlir::Type lhs, mlir::Type rhs);

// The element of one column the branches of a selection fill with values of @param lhs and
// of @param rhs, each value keeping its type: theirs when they agree, the one integer type
// Cypher has for two integers, the list two lists join to, and a null type when the column
// holds each row's value as a tagged cell
mlir::Type selectedValueElement(mlir::OpBuilder& builder, mlir::Type lhs, mlir::Type rhs);

// Whether a column can hold the null literal beside its values: in the optional a nullable
// chunk wraps them in, or in a value of its own
bool holdsItsNull(mlir::Type chunkType);

}
