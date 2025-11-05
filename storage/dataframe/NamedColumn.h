#pragma once

#include "ColumnHeader.h"

namespace db {

class Dataframe;
class Column;

class NamedColumn {
public:
    friend Dataframe;

    NamedColumn(Dataframe* parent,
                const ColumnHeader& header,
                Column* column)
        : _parent(parent),
        _header(header),
        _column(column)
    {
    }

    ~NamedColumn() = default;

    Dataframe* getParent() const { return _parent; }

    const ColumnHeader& getHeader() const { return _header; }
    ColumnHeader& getHeader() { return _header; }

    Column* getColumn() const { return _column; }

private:
    Dataframe* _parent {nullptr};
    ColumnHeader _header;
    Column* _column {nullptr};
};

}
