#include "NullLiteralType.h"

#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Operation.h"

#include "StorageTypes.h"

using namespace mlir;

bool mlir::isNullLiteralType(Type elementType) {
    const auto nullable = dyn_cast_or_null<storage::NullableType>(elementType);

    return nullable && isa<NoneType>(nullable.getValueType());
}

bool mlir::holdsItsOwnNull(Type elementType) {
    return isa<storage::AnyType, storage::NodeIDType, storage::EdgeIDType, storage::PathType>(elementType);
}

Type mlir::nullableElementType(Type valueType) {
    if (holdsItsOwnNull(valueType)) {
        return valueType;
    }

    return storage::NullableType::get(valueType.getContext(), valueType);
}

bool mlir::isReadAsRows(Type elementType) {
    return isa<storage::NodeIDType,
               storage::EdgeIDType,
               storage::LabelSetIDType,
               storage::EdgeTypeIDType,
               storage::PathType,
               storage::PathRefType,
               storage::BoolType>(elementType);
}

bool mlir::isReadAsCells(Type elementType) {
    return isa<storage::NodeIDType,
               storage::EdgeIDType,
               storage::PathType,
               storage::ListType,
               storage::MapType,
               storage::AnyType,
               storage::NullableType>(elementType);
}

LogicalResult mlir::verifyComparesNoNullLiteral(Operation* op, Type lhsElementType, Type rhsElementType) {
    if (isNullLiteralType(lhsElementType) || isNullLiteralType(rhsElementType)) {
        return op->emitOpError("compares against the null literal, which is null on every row: "
                               "is_null tests for null");
    }

    return success();
}
