#include "ColumnIndicesFormat.h"

#include <stdint.h>

#include "mlir/IR/Builders.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

using namespace mlir;

ParseResult mlir::parseColumnIndices(OpAsmParser& parser, DenseI64ArrayAttr& indices) {
    llvm::SmallVector<int64_t> values;

    const auto parseIndex = [&]() -> ParseResult {
        int64_t value = 0;
        if (parser.parseInteger(value)) {
            return failure();
        }

        values.push_back(value);
        return success();
    };

    if (parser.parseCommaSeparatedList(OpAsmParser::Delimiter::Square, parseIndex)) {
        return failure();
    }

    indices = parser.getBuilder().getDenseI64ArrayAttr(values);
    return success();
}

void mlir::printColumnIndices(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr indices) {
    printer << "[";

    llvm::interleaveComma(indices.asArrayRef(), printer, [&](int64_t index) {
        printer << index;
    });

    printer << "]";
}
