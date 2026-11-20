#pragma once

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

namespace pybindings {

class PyColumn;

// The columns of one response with their wire names, and the row count of the chunk last
// decoded. The sink owns the columns.
class PyColumnContainer {
public:
    PyColumnContainer();
    ~PyColumnContainer();

    size_t size() const { return _columns.size(); }
    PyColumn* operator[](size_t index) { return _columns[index]; }

    const std::string& getName(size_t index) const { return _names[index]; }

    void addColumn(PyColumn* column, std::string_view name);

    void setRowCount(size_t rowCount) { _rowCount = rowCount; }
    size_t getRowCount() const { return _rowCount; }

private:
    std::vector<PyColumn*> _columns;
    std::vector<std::string> _names;
    size_t _rowCount {0};
};

}
