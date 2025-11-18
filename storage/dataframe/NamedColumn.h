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

    ColumnTag getTag() const { return _tag; }

    Column* getColumn() const { return _column; }

    std::string_view getName() const;

    void rename(std::string_view name);

    static NamedColumn* create(DataframeManager* dfMan,
                               Column* column,
                               ColumnTag tag);

    template <typename T>
    requires std::is_base_of_v<Column, T>
    T* as() const {
        return dynamic_cast<T*>(_column);
    }

private:
    DataframeManager* _dfMan {nullptr};
    ColumnTag _tag;
    Column* _column {nullptr};

    NamedColumn(DataframeManager* dfMan,
                ColumnTag tag,
                Column* column)
        : _dfMan(dfMan),
        _tag(tag),
        _column(column)
    {
    }

    ~NamedColumn() = default;
};

}
