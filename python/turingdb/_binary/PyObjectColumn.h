#pragma once

#include <stddef.h>
#include <span>
#include <vector>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

// The Python objects of one result column, a block of references per chunk. The column
// owns each reference until toArray moves it into the numpy array. The destructor may
// run without the GIL, on a query that failed after some chunks.
class PyObjectColumn {
public:
    PyObjectColumn();
    ~PyObjectColumn();

    std::span<PyObject*> addBlock(size_t rowCount);

    nb::object toArray();

private:
    std::vector<std::vector<PyObject*>> _blocks;
    size_t _rowCount {0};
};

}
