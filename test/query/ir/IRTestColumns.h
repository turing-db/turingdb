#pragma once

#include <stddef.h>

#include <optional>
#include <string>

#include "columns/Column.h"
#include "columns/ColumnOptVector.h"

namespace turing::test {

// The column as ColumnType when it is one, nullptr otherwise
template <typename ColumnType>
const ColumnType* columnAs(const db::Column* column) {
    if (column->getKind() != ColumnType::staticKind()) {
        return nullptr;
    }

    return static_cast<const ColumnType*>(column);
}

// A row of a nullable string column as an owned string, nullopt for a null
template <typename T>
std::optional<std::string> getOptionalString(const db::ColumnOptVector<T>* column, size_t row) {
    if (!column->isValid(row)) {
        return std::nullopt;
    }

    return std::string(column->getValue(row));
}

}
