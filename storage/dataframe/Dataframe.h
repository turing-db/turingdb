#pragma once

#include <vector>
#include <unordered_map>

#include "ColumnTag.h"

namespace db {

class NamedColumn;

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

    const NamedColumns& cols() const { return _cols; }

    NamedColumn* getColumn(ColumnTag tag) const {
        const auto it = _headerMap.find(tag);
        if (it == _headerMap.end()) {
            return nullptr;
        }

        return it->second;
    }

private:
    std::unordered_map<ColumnTag, NamedColumn*> _headerMap;
    NamedColumns _cols;

    void addColumn(NamedColumn* column);
};

}
