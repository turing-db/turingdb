#pragma once

#include "Column.h"

namespace db {

class ListColumn : public Column {
public:
    explicit ListColumn(ColumnKind::ColumnKindCode kind)
        : Column(kind) {
    }

    ListColumn(const ListColumn&) = default;
    ListColumn(ListColumn&&) noexcept = default;
    ListColumn& operator=(const ListColumn&) = default;
    ListColumn& operator=(ListColumn&&) noexcept = default;

    ~ListColumn() override = default;
};

}
