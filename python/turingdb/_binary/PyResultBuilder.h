#pragma once

#include <memory>
#include <vector>

#include <nanobind/nanobind.h>

#include "PyObjectColumn.h"

namespace pybindings {

class PyColumnContainer;
class PyValueStore;

// Builds the {"data", "dtypes"} envelope from the columns of a PySink. A column numpy holds
// as an array of numbers is handed over whole at the end; one Python reads as objects is
// read a chunk at a time into Python objects, moved at the end into one numpy object
// array. A nullable number column is a numpy masked array.
class PyResultBuilder {
public:
    PyResultBuilder(PyColumnContainer* container, PyValueStore* values);
    ~PyResultBuilder();

    // Takes the chunk the columns hold, and the path IDs of the value store, which its
    // GraphPath objects view. The sink drops the chunk's strings, lists and maps once it
    // returns. Runs without the GIL, and takes it only to read a column as objects.
    void appendChunk();

    void build(nb::dict& envelope);

private:
    PyColumnContainer* _container {nullptr};
    PyValueStore* _values {nullptr};
    std::vector<std::unique_ptr<PyObjectColumn>> _objectColumns;

    void allocObjectColumns();
};

}
