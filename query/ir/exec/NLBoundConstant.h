#pragma once

#include <concepts>

#include "columns/ColumnConst.h"
#include "CellOrder.h"

namespace db {

// The value @param bound holds for the run, read off a constant column of T or of nullable T,
// where a cell holds its own null. False when it is neither.
template <typename T>
bool readBoundConstant(const Column* bound, std::optional<T>& value) {
    if (bound->getKind() == ColumnConst<T>::staticKind()) {
        value = static_cast<const ColumnConst<T>*>(bound)->getRaw();
        return true;
    } else if constexpr (!std::same_as<T, CellView>) {
        if (bound->getKind() == ColumnConst<std::optional<T>>::staticKind()) {
            value = static_cast<const ColumnConst<std::optional<T>>*>(bound)->get(0);
            return true;
        }
    }

    return false;
}

}
