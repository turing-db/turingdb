#pragma once

#include <stdint.h>

#include "mlir/IR/OpImplementation.h"

namespace mlir::storage {

// The value types of a dynamic type, written as names: [Int64, Double]
FailureOr<uint32_t> parseValueTypeSet(AsmParser& parser);

void printValueTypeSet(AsmPrinter& printer, uint32_t bits);

}
