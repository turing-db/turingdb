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

    Column* getColumn() const { return _column; }

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
