#include "BoundValueFormat.h"

#include "mlir/IR/Builders.h"

using namespace mlir;

namespace {

OptionalParseResult parseBoundValue(OpAsmParser& parser,
                                    std::optional<OpAsmParser::UnresolvedOperand>& bound,
                                    Type& boundType) {
    OpAsmParser::UnresolvedOperand operand;
    const OptionalParseResult parsedOperand = parser.parseOptionalOperand(operand);
    if (!parsedOperand.has_value()) {
        return std::nullopt;
    } else if (failed(*parsedOperand) || parser.parseColonType(boundType)) {
        return failure();
    }

    bound = operand;
    return success();
}

}

ParseResult mlir::parseCSVPath(OpAsmParser& parser,
                               StringAttr& path,
                               std::optional<OpAsmParser::UnresolvedOperand>& boundPath,
                               Type& boundType) {
    const OptionalParseResult parsedBound = parseBoundValue(parser, boundPath, boundType);
    if (parsedBound.has_value()) {
        return *parsedBound;
    }

    return parser.parseAttribute(path);
}

void mlir::printCSVPath(OpAsmPrinter& printer, Operation* op, StringAttr path, Value boundPath, Type boundType) {
    if (boundPath) {
        printer << boundPath << " : " << boundType;
        return;
    }

    printer.printAttribute(path);
}

ParseResult mlir::parseNeighbourCount(OpAsmParser& parser,
                                      IntegerAttr& count,
                                      std::optional<OpAsmParser::UnresolvedOperand>& boundCount,
                                      Type& boundType) {
    const OptionalParseResult parsedBound = parseBoundValue(parser, boundCount, boundType);
    if (parsedBound.has_value()) {
        return *parsedBound;
    }

    Builder& builder = parser.getBuilder();
    return parser.parseAttribute(count, builder.getIntegerType(64, /*isSigned=*/false));
}

void mlir::printNeighbourCount(OpAsmPrinter& printer, Operation* op, IntegerAttr count, Value boundCount, Type boundType) {
    if (boundCount) {
        printer << boundCount << " : " << boundType;
        return;
    }

    printer.printAttributeWithoutType(count);
}

ParseResult mlir::parseQueryVector(OpAsmParser& parser,
                                   DenseF32ArrayAttr& queryVector,
                                   std::optional<OpAsmParser::UnresolvedOperand>& boundQueryVector,
                                   Type& boundType) {
    const OptionalParseResult parsedBound = parseBoundValue(parser, boundQueryVector, boundType);
    if (parsedBound.has_value()) {
        return *parsedBound;
    }

    return parser.parseCustomAttributeWithFallback(queryVector, Type());
}

void mlir::printQueryVector(OpAsmPrinter& printer, Operation* op, DenseF32ArrayAttr queryVector, Value boundQueryVector, Type boundType) {
    if (boundQueryVector) {
        printer << boundQueryVector << " : " << boundType;
        return;
    }

    printer.printStrippedAttrOrType(queryVector);
}
