#pragma once

#include "DBOps.h"

namespace mlir::db {

Speculation::Speculatability speculatableWhen(bool cannotRaise);

// An op reading values of some types raises on a value of another, which such an operand may hold
Speculation::Speculatability speculatableOverKnownTypes(ValueRange operands);

bool matchIntegerLiteral(Value column, int64_t& value);

// A string function counting characters raises on a cell holding no string, and on a count
// that is null, negative or past the largest it takes. Its first operand is the string, every
// other one a count
Speculation::Speculatability speculatableOverCharacters(Operation* op);

}
