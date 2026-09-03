#include "EdgeDirectionsFormat.h"

#include <optional>
#include <stdint.h>

#include "mlir/IR/Builders.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageEnums.h"

using namespace mlir;

namespace storage = mlir::storage;

ParseResult mlir::parseEdgeDirections(OpAsmParser& parser, DenseI64ArrayAttr& directions) {
    llvm::SmallVector<int64_t> values;

    const auto parseDirection = [&]() -> ParseResult {
        llvm::StringRef keyword;
        if (parser.parseKeyword(&keyword)) {
            return failure();
        }

        const std::optional<storage::EdgeDirection> direction = storage::symbolizeEdgeDirection(keyword);
        if (!direction) {
            return parser.emitError(parser.getCurrentLocation()) << "unknown edge direction '" << keyword << "'";
        }

        values.push_back(static_cast<int64_t>(*direction));
        return success();
    };

    if (parser.parseCommaSeparatedList(OpAsmParser::Delimiter::Square, parseDirection)) {
        return failure();
    }

    directions = parser.getBuilder().getDenseI64ArrayAttr(values);
    return success();
}

void mlir::printEdgeDirections(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr directions) {
    printer << "[";

    llvm::interleaveComma(directions.asArrayRef(), printer, [&](int64_t raw) {
        const std::optional<storage::EdgeDirection> direction = storage::symbolizeEdgeDirection(static_cast<uint64_t>(raw));

        // A verified op only ever carries valid directions, but the printer can run on
        // in-flight IR, so fall back to the raw integer rather than dereferencing a
        // null optional.
        if (direction) {
            printer << storage::stringifyEdgeDirection(*direction);
        } else {
            printer << raw;
        }
    });

    printer << "]";
}
