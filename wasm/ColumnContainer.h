#pragma once

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

namespace wasm {
class Column;

// Owns the decoded columns of one result, with their wire names. Satisfies the
// decoder's column container contract: size(), operator[], addColumn(column, name).
class ColumnContainer {
public:
    ColumnContainer();
    ~ColumnContainer();

    void addColumn(Column* column, std::string_view name);
    Column* operator[](size_t index);

    const std::string& getName(size_t index) const;
    size_t size() const { return _columns.size(); }

    void clear();

    // Drops every column's decoded values but keeps the columns and their names: a
    // response streams several dataframes through the same columns.
    void clearColumnData();

private:
    std::vector<Column*> _columns;
    std::vector<std::string> _names;
};

}
