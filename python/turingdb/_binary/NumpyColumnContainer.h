#pragma once

#include <stddef.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

class NumpyColumn;

// Owns the decoded columns of one response, with their wire names. Satisfies the
// decoder's column container contract: size(), operator[], addColumn(column, name).
class NumpyColumnContainer {
public:
    NumpyColumnContainer();
    ~NumpyColumnContainer();

    size_t size() const { return _columns.size(); }
    NumpyColumn* operator[](size_t index) { return _columns[index].get(); }

    // Takes ownership of the column.
    void addColumn(NumpyColumn* column, std::string_view name);

    void setRowCount(size_t rowCount) { _chunkRowCount = rowCount; }

    // Closes the dataframe just decoded; the next one appends to the same columns.
    void finishChunk();

    // Builds the {"data": {name: ndarray | list}, "dtypes": {name: dtype}} envelope.
    nb::dict toPython();

private:
    std::vector<std::unique_ptr<NumpyColumn>> _columns;
    std::vector<std::string> _names;
    size_t _rowCount {0};
    size_t _chunkRowCount {0};
};

}
