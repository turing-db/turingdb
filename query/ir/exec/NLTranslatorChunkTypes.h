#pragma once

#include "mlir/IR/Types.h"

#include "metadata/PropertyType.h"

#include "NLProgram.h"

namespace db {

ValueType valueTypeFromElementType(mlir::Type elementType);

// The value type wrapped by a nullable value chunk (!nl.chunk<!storage.nullable<T>>).
// Every chunk carrying scalar values is such a chunk - an aggregate's input and result,
// a property fetch, a homogeneous unwind - so a chunk that is not one (an ID chunk) is
// rejected.
ValueType nullableChunkValueType(mlir::Type chunkType);

// Whether a chunk holds a value that may be absent (!nl.chunk<!storage.nullable<T>>), as
// opposed to a plain value column
bool isNullableChunkType(mlir::Type chunkType);

// Whether a chunk carries the null literal, which has no value type of its own: a
// !nl.chunk<!storage.nullable<none>>
bool isUntypedNullChunkType(mlir::Type chunkType);

bool isAnyChunk(mlir::Type chunkType);

bool isNullableList(mlir::Type elementType);

bool isNullableMap(mlir::Type elementType);

ValueType valueTypeFromChunkType(mlir::Type chunkType);

bool isConstantColumn(const Column* column);

// A truth value is held in the rows its kind names, or in the constant standing for every
// row. The null literal is read by no kernel, whatever column holds it.
bool holdsTruthColumn(NLTruthColumnKind kind, const Column* column);

// A chunk read as a truth value is read by the kernel its type names, so the column bound
// to it must be the one that type names
NLTruthColumnKind truthColumnKindOf(mlir::Type chunkType, const Column* column);

}
