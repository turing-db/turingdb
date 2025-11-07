#pragma once

#include <type_traits>
#include <string_view>

#include "ColumnTag.h"

namespace db {

class DataframeManager;
class Column;

class NamedColumn {
public:
    friend DataframeManager;

    static NamedColumn* create(DataframeManager* dfMan,
                               Column* column,
                               ColumnTag tag);

    ColumnTag getTag() const { return _tag; }

    std::string_view getName() const;

    void setName(std::string_view name);

    Column* getColumn() const { return _column; }

    template <typename T>
    requires std::is_base_of_v<Column, T>
    const T* as() const {
        return dynamic_cast<const T*>(_column);
    }

private:
    DataframeManager* _dfMan {nullptr};
    ColumnTag _tag;
    Column* _column {nullptr};

    NamedColumn(DataframeManager* dfMan,
                ColumnTag tag,
                Column* column)
        : _tag(tag),
        _column(column)
    {
    }

    ~NamedColumn() = default;
};

}
