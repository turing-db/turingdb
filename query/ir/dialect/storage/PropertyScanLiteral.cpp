#include "PropertyScanLiteral.h"

#include "StorageTypes.h"

using namespace mlir;

bool mlir::storage::isPropertyScanLiteral(TypedAttr literal) {
    const Type type = literal.getType();
    const bool isInteger = type.isSignlessInteger(64);
    const bool isBool = type.isSignlessInteger(1);
    const bool isDouble = type.isF64();
    const bool isString = isa<storage::StringType>(type);

    return isInteger || isBool || isDouble || isString;
}

llvm::StringRef mlir::storage::propertyScanLiteralKinds() {
    return "requires an i64, f64, i1 or !storage.string literal, got ";
}
