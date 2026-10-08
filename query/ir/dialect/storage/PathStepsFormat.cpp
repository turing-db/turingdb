#include "PathStepsFormat.h"

#include <optional>
#include <stdint.h>

#include "mlir/IR/Builders.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageEnums.h"
#include "PathHopArguments.h"

using namespace mlir;

namespace storage = mlir::storage;

namespace {

void printDirection(OpAsmPrinter& printer, int64_t raw) {
    const std::optional<storage::PathDirection> direction = storage::symbolizePathDirection(static_cast<uint64_t>(raw));

    // The printer can run on in-flight IR the verifier has not seen yet
    if (direction) {
        printer << storage::stringifyPathDirection(*direction);
    } else {
        printer << raw;
    }
}

}

ParseResult mlir::parsePathDirections(OpAsmParser& parser, DenseI64ArrayAttr& directions) {
    llvm::SmallVector<int64_t> values;

    const auto parseDirection = [&]() -> ParseResult {
        llvm::StringRef keyword;
        if (parser.parseKeyword(&keyword)) {
            return failure();
        }

        const std::optional<storage::PathDirection> direction = storage::symbolizePathDirection(keyword);
        if (!direction) {
            return parser.emitError(parser.getCurrentLocation()) << "unknown path direction '" << keyword << "'";
        }

        values.push_back(static_cast<int64_t>(*direction));
        return success();
    };

    if (succeeded(parser.parseOptionalLSquare())) {
        if (parser.parseCommaSeparatedList(parseDirection) || parser.parseRSquare()) {
            return failure();
        }
    } else if (parseDirection()) {
        return failure();
    }

    directions = parser.getBuilder().getDenseI64ArrayAttr(values);
    return success();
}

void mlir::printPathDirections(OpAsmPrinter& printer, Operation* op, DenseI64ArrayAttr directions) {
    const llvm::ArrayRef<int64_t> values = directions.asArrayRef();
    if (values.size() == 1) {
        printDirection(printer, values.front());
        return;
    }

    printer << "[";
    llvm::interleaveComma(values, printer, [&](int64_t raw) { printDirection(printer, raw); });
    printer << "]";
}

ParseResult mlir::parseStepNames(OpAsmParser& parser, ArrayAttr& names) {
    ArrayAttr parsed;
    const SMLoc location = parser.getCurrentLocation();
    if (parser.parseAttribute(parsed)) {
        return failure();
    }

    const bool namesOneStep = parsed.empty() || isa<StringAttr>(parsed[0]);
    if (namesOneStep) {
        names = parser.getBuilder().getArrayAttr({parsed});
        return success();
    }

    for (const Attribute stepNames : parsed) {
        const auto list = dyn_cast<ArrayAttr>(stepNames);
        const bool listsNames = list && llvm::all_of(list, [](Attribute name) { return isa<StringAttr>(name); });
        if (!listsNames) {
            return parser.emitError(location) << "expects a list of names, or one list of names per step";
        }
    }

    names = parsed;
    return success();
}

void mlir::printStepNames(OpAsmPrinter& printer, Operation* op, ArrayAttr names) {
    if (names.size() == 1) {
        printer.printAttribute(names[0]);
        return;
    }

    printer.printAttribute(names);
}

ParseResult mlir::parseHopRegions(OpAsmParser& parser,
                                  llvm::SmallVectorImpl<std::unique_ptr<Region>>& hops,
                                  DenseI64ArrayAttr directions) {
    const size_t stepCount = directions.size();
    const SMLoc location = parser.getCurrentLocation();

    hops.push_back(std::make_unique<Region>());
    const OptionalParseResult parsedFirst = parser.parseOptionalRegion(*hops.back());
    if (!parsedFirst.has_value()) {
        hops.pop_back();
        for (size_t step = 0; step < stepCount; step++) {
            hops.push_back(std::make_unique<Region>());
        }

        return success();
    }

    if (failed(*parsedFirst)) {
        return failure();
    }

    while (succeeded(parser.parseOptionalComma())) {
        hops.push_back(std::make_unique<Region>());
        if (parser.parseRegion(*hops.back())) {
            return failure();
        }
    }

    if (hops.size() != stepCount) {
        return parser.emitError(location) << "expects one hop region per step, but takes " << stepCount
                                          << " steps and " << hops.size() << " regions";
    }

    return success();
}

void mlir::printHopRegions(OpAsmPrinter& printer, Operation* op, MutableArrayRef<Region> hops, DenseI64ArrayAttr directions) {
    const bool hasPredicate = hasHopPredicate(hops);
    if (!hasPredicate) {
        return;
    }

    printer << " ";
    llvm::interleave(hops,
                     [&](Region& hop) { printer.printRegion(hop); },
                     [&]() { printer << ", "; });
}
