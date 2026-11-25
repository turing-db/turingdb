#pragma once

#include "mlir/IR/Operation.h"

namespace db {

// A body whose own dataflow cannot keep the input rows paired with what it makes of them
// runs one input row at a time, whatever it ends on
bool runsPerRow(mlir::Operation* operation);

// A walk's end set is one relation every seed shares, consumed in full before the walk
// runs, and a scan's bound values and a cut's bound count hold one value for the run, read
// when it starts: no column of the rows in flight, though a pass binds it ahead of the
// function
bool readsAheadOfTheRows(mlir::OpOperand& operand);

}
