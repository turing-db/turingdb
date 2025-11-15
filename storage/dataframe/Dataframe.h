#pragma once

#include <iostream>
#include <vector>
#include <ostream>

#include "ColumnTag.h"
#include "NamedColumn.h"

#include "DynamicLookupTable.h"

namespace db {

class Column;

// A basic Dataframe class with columns indexed by ColumnTag.
class Dataframe {
public:
    using NamedColumns = std::vector<NamedColumn*>;
    friend NamedColumn;

    Dataframe();

    Dataframe(const Dataframe&) = delete;
    Dataframe& operator=(const Dataframe&) = delete;
    
    ~Dataframe();

    size_t size() const { return _cols.size(); }

    size_t getRowCount() const;

    const NamedColumns& cols() const { return _cols; }

    void addColumn(NamedColumn* column);

    NamedColumn* getColumn(ColumnTag tag) const {
        return _tagToColumnMap.lookup(tag.getValue());
    }

    template <typename T>
    requires std::is_base_of_v<Column, T>
    T* getColumn(ColumnTag tag) const {
        auto* col = getColumn(tag);
        if (!col) {
            return nullptr;
        }

        return dynamic_cast<T*>(col->getColumn());
    }

    void copyFromLine(const Dataframe* other, size_t startRow, size_t rowCount);
    void copyFrom(const Dataframe* other);

    void dump(std::ostream& out = std::cout) const;

private:
    NamedColumns _cols;
    DynamicLookupTable<NamedColumn*> _tagToColumnMap;
};

}
