#pragma once

#include <vector>

#include "DataframeHeaderMap.h"
#include "ColumnName.h"

namespace db {

class NamedColumn;

class Dataframe {
public:
    using NamedColumns = std::vector<NamedColumn*>;
    friend NamedColumn;

    Dataframe();

    Dataframe(Dataframe&& other)
        : _cols(std::move(other._cols))
    {
    }

    Dataframe& operator=(Dataframe&& other) {
        _cols = std::move(other._cols);
        return *this;
    }

    Dataframe(const Dataframe&) = delete;
    Dataframe& operator=(const Dataframe&) = delete;
    
    ~Dataframe();

    const NamedColumns& cols() const { return _cols; }

    NamedColumn* getColumn(ColumnName name) const {
        return _headerMap.getColumn(name);
    }

private:
    DataframeHeaderMap _headerMap;
    NamedColumns _cols;

    void addColumn(NamedColumn* column);
};

}
