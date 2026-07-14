#include "GroupAggregateKindsFormat.h"

#include <optional>
#include <stdint.h>

#include "mlir/IR/Builders.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageEnums.h"

using namespace mlir;

namespace storage = mlir::storage;

ParseResult mlir::parseGroupAggregateKinds(OpAsmParser& parser, DenseI64ArrayAttr& kinds) {
    llvm::SmallVector<int64_t> values;

    const auto parseKind = [&]() -> ParseResult {
        llvm::StringRef keyword;
        if (parser.parseKeyword(&keyword)) {
            return failure();
        }

        const std::optional<storage::GroupAggregateKind> kind = storage::symbolizeGroupAggregateKind(keyword);
        if (!kind) {
            return parser.emitError(parser.getCurrentLocation()) << "unknown aggregate kind '" << keyword << "'";
        }

        values.push_back(static_cast<int64_t>(*kind));
        return success();
    };

    if (parser.parseCommaSeparatedList(OpAsmParser::Delimiter::Square, parseKind)) {
        return failure();
    }

    kinds = parser.getBuilder().getDenseI64ArrayAttr(values);
    return success();
}

void mlir::printGroupAggregateKinds(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr kinds) {
    printer << "[";

    llvm::interleaveComma(kinds.asArrayRef(), printer, [&](int64_t raw) {
        const std::optional<storage::GroupAggregateKind> kind = storage::symbolizeGroupAggregateKind(static_cast<uint64_t>(raw));

        // A verified op only ever carries valid kinds, but the printer can run on
        // in-flight IR, so fall back to the raw integer rather than dereferencing a
        // null optional.
        if (kind) {
            printer << storage::stringifyGroupAggregateKind(*kind);
        } else {
            printer << raw;
        }
    });

    printer << "]";
}
