#pragma once

#include "mlir/IR/Value.h"

#include "DBProgramGenerator.h"

#include "VariableDependency.h"

namespace db {

// Whether @param column is one @param reductionOp bound, or one computed from what it
// bound: anything computed before it holds the matched rows the reduction consumed. A
// column of another block is one the body being generated binds, which no reduction reduced
bool boundAtOrAfter(mlir::Value column, mlir::Operation* reductionOp);

// The untyped null column the null literal compiles to: nullable with no value type of
// its own, so nothing about it says which column would carry a value
bool isUntypedNullColumnType(mlir::Type type);

bool isUntypedNullColumn(mlir::Value column);

bool isPathColumn(mlir::Value column);

mlir::Value findVarOrThrow(const DBProgramGenerator::VariableIdentityMap& map,
                        const VariableDependency* var);

}
