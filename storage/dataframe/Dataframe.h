#pragma once

#include <vector>
#include <unordered_map>
#include <ostream>

#include "ColumnTag.h"
#include "NamedColumn.h"

namespace db {

class Column;

// A basic Dataframe class where columns are indexed by numerical ColumnTag.
// Columns can have optional names as string_views, but only tags can be used
// to uniquely retrieve a column.
// A Dataframe can possibly have several columns with the same name strings.
// Uniqueness is only enforced on ColumnTags.
class Dataframe {
public:
    using NamedColumns = std::vector<NamedColumn*>;
    friend NamedColumn;

    Dataframe();

    Dataframe(Dataframe&& other)
        : _headerMap(std::move(other._headerMap)),
        _cols(std::move(other._cols))
    {
    }

    Dataframe& operator=(Dataframe&& other) {
        _headerMap = std::move(other._headerMap);
        _cols = std::move(other._cols);
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
        const auto it = _headerMap.find(tag);
        if (it == _headerMap.end()) {
            return nullptr;
        }

        return it->second;
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
    std::unordered_map<ColumnTag, NamedColumn*, ColumnTag::Hash> _headerMap;
    NamedColumns _cols;
};

}
