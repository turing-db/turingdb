#include "DBColumnChecks.h"

#include "DBColumnElement.h"
#include "DBHeldTags.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

Type mlir::db::columnElement(Value column) {
    return ::db::declaredColumnElement(column);
}

bool mlir::db::mayHoldAnyType(Value operand) {
    const ColumnType column = cast<ColumnType>(operand.getType());
    const Type element = columnElement(operand);
    const bool unresolved = isa<NoneType>(column.getType()) && isa<NoneType>(element);

    return unresolved || isa<storage::AnyType>(element);
}

bool mlir::db::holdsOnlyKeyedValues(Value column) {
    return ::db::holdsOnlyNullOr(column, {::db::CellTag::MapView, ::db::CellTag::NodeID, ::db::CellTag::EdgeID});
}

bool mlir::db::holdsOnlyLists(Value column) {
    return ::db::holdsOnlyNullOr(column, {::db::CellTag::ListView});
}

bool mlir::db::holdsOnlyIntegers(Value column) {
    return ::db::holdsOnlyNullOr(column, {::db::CellTag::Int, ::db::CellTag::UInt});
}

bool mlir::db::isIntegerColumn(Value column) {
    return isa<ColumnType>(column.getType())
        && isa<IntegerType, storage::AnyType, NoneType>(::db::declaredColumnElement(column));
}

LogicalResult mlir::db::verifyStringResult(Operation* op, Value result) {
    const ColumnType resultColumn = dyn_cast<ColumnType>(result.getType());
    if (!resultColumn || !isa<storage::StringType>(resultColumn.getType())) {
        return op->emitOpError("result must be a column of strings");
    }

    return success();
}
