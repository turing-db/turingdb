#pragma once

#include "ColumnName.h"

namespace db {

class Dataframe;
class Column;

class NamedColumn {
public:
    friend Dataframe;

    ColumnName getPrimaryName() const { return _primaryName; }

    void setPrimaryName(ColumnName name);

    Column* getColumn() const { return _column; }

    static NamedColumn* create(Dataframe* df,
                               Column* column,
                               ColumnName name);

private:
    Dataframe* _parent {nullptr};
    ColumnName _primaryName;
    Column* _column {nullptr};

    NamedColumn(Dataframe* parent,
                ColumnName name,
                Column* column)
        : _parent(parent),
        _primaryName(name),
        _column(column)
    {
    }

    ~NamedColumn() = default;
};

}
