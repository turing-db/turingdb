#include "PropertyScanLiteral.h"

#include "StorageTypes.h"

using namespace mlir;

bool mlir::storage::isPropertyScanLiteral(TypedAttr literal) {
    return isPropertyScanType(literal.getType());
}

bool mlir::storage::isPropertyScanType(Type type) {
    const bool isInteger = type.isSignlessInteger(64);
    const bool isBool = type.isSignlessInteger(1);
    const bool isDouble = type.isF64();
    const bool isString = isa<storage::StringType>(type);

    return isInteger || isBool || isDouble || isString;
}

llvm::StringRef mlir::storage::propertyScanLiteralKinds() {
    return "requires an i64, f64, i1 or !storage.string literal, got ";
}

ParseResult mlir::parsePropertyScanValue(OpAsmParser& parser,
                                         TypedAttr& value,
                                         std::optional<OpAsmParser::UnresolvedOperand>& boundValue,
                                         Type& boundType) {
    OpAsmParser::UnresolvedOperand operand;
    const OptionalParseResult parsedOperand = parser.parseOptionalOperand(operand);
    if (parsedOperand.has_value()) {
        if (failed(*parsedOperand) || parser.parseColonType(boundType)) {
            return failure();
        }

        boundValue = operand;
        return success();
    }

    Attribute literal;
    if (parser.parseAttribute(literal)) {
        return failure();
    }

    value = dyn_cast<TypedAttr>(literal);
    if (!value) {
        return parser.emitError(parser.getCurrentLocation(), "expected a typed literal");
    }

    return success();
}

void mlir::printPropertyScanValue(OpAsmPrinter& printer,
                                  Operation* op,
                                  TypedAttr value,
                                  Value boundValue,
                                  Type boundType) {
    if (boundValue) {
        printer << boundValue << " : " << boundType;
        return;
    }

    printer.printAttribute(value);
}
