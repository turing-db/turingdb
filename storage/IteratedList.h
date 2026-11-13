#pragma once

#include <optional>

#include "list/ListView.h"

namespace db {

// The list Cypher iterates a value held in a cell as: the list it holds, or a list of one
// viewing the cell itself where it holds any other value. A null has none.
inline std::optional<ListView> iteratedList(const CellView& cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::Null) {
        return std::nullopt;
    } else if (tag == CellTag::ListView) {
        return cell.getAs<ListView>();
    }

    return ListView::singleton(cell);
}

std::optional<ListView> iteratedList(const CellView&&) = delete;

}
