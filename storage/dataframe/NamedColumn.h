#pragma once

#include "ColumnHeader.h"

namespace db {

class DataframeManager;
class Column;

class NamedColumn {
public:
    friend DataframeManager;

    const ColumnHeader& getHeader() const { return _header; }
    ColumnHeader& getHeader() { return _header; }

    ColumnTag getTag() const { return _header.getTag(); }

    Column* getColumn() const { return _column; }

    template <typename T>
    requires std::is_base_of_v<Column, T>
    const T* as() const {
        return dynamic_cast<const T*>(_column);
    }

    static NamedColumn* create(DataframeManager* dfMan,
                               Column* column,
                               const ColumnHeader& header);

private:
    ColumnHeader _header;
    Column* _column {nullptr};

    NamedColumn(const ColumnHeader& header,
                Column* column)
        : _header(header),
        _column(column)
    {
    }

    ~NamedColumn() = default;
};

}
