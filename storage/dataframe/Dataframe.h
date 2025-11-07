#pragma once

#include <vector>
#include <ostream>

#include "ColumnTag.h"
#include "NamedColumn.h"

namespace db {

class Column;

// A basic Dataframe class with columns indexed by ColumnTag.
class Dataframe {
public:
    using NamedColumns = std::vector<NamedColumn*>;
    friend NamedColumn;

    Dataframe();

    Dataframe(Dataframe&& other)
        : _cols(std::move(other._cols)),
        _headerMap(std::move(other._headerMap))
    {
    }

    Dataframe& operator=(Dataframe&& other) {
        _cols = std::move(other._cols);
        _headerMap = std::move(other._headerMap);
        return *this;
    }

    Dataframe(const Dataframe&) = delete;
    Dataframe& operator=(const Dataframe&) = delete;
    
    ~Dataframe();

    size_t size() const { return _cols.size(); }

    size_t getRowCount() const;

    const NamedColumns& cols() const { return _cols; }

    void addColumn(NamedColumn* column);

    NamedColumn* getColumn(ColumnTag tag) const {
        const size_t tagValue = tag.getValue();
        if (tagValue > _headerMap.size()) {
            return nullptr;
        }

        return _headerMap[tagValue];
    }

    template <typename T>
    requires std::is_base_of_v<Column, T>
    const T* getColumn(ColumnTag tag) const {
        const auto* col = getColumn(tag);
        if (!col) {
            return nullptr;
        }

        return dynamic_cast<const T*>(col->getColumn());
    }

    void copyFromLine(const Dataframe* other, size_t startRow, size_t rowCount);
    void copyFrom(const Dataframe* other);

    void dump(std::ostream& out) const;

private:
    NamedColumns _cols;
    std::vector<NamedColumn*> _headerMap;
};

}
