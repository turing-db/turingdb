#pragma once

#include "DBOps.h"

namespace mlir::db {

Type columnElement(Value column);

// A tagged cell, or a column whose type lowering resolves, may hold a value of any type. The
// null literal is a nullable none.
bool mayHoldAnyType(Value operand);

bool holdsOnlyKeyedValues(Value column);

bool holdsOnlyLists(Value column);

bool holdsOnlyIntegers(Value column);

// Whether @param type is a column of @param Element, nullable or not, or of a cell typed at
// run time
template <typename Element>
bool isColumnOf(Type type) {
    const auto column = dyn_cast<ColumnType>(type);
    if (!column) {
        return false;
    }

    const Type element = column.getType();
    const auto nullable = dyn_cast<storage::NullableType>(element);
    const Type value = nullable ? nullable.getValueType() : element;

    return isa<Element, storage::AnyType, NoneType>(value);
}

bool isIntegerColumn(Value column);

LogicalResult verifyStringResult(Operation* op, Value result);

}
