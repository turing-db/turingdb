#include "PyObjectColumn.h"

#include <stdint.h>

#include "TuringException.h"

using namespace pybindings;

PyObjectColumn::PyObjectColumn() {
}

PyObjectColumn::~PyObjectColumn() {
    if (_rowCount == 0) {
        return;
    }

    nb::gil_scoped_acquire acquire;

    for (const std::vector<PyObject*>& block : _blocks) {
        for (PyObject* object : block) {
            Py_XDECREF(object);
        }
    }
}

std::span<PyObject*> PyObjectColumn::addBlock(size_t rowCount) {
    std::vector<PyObject*>& block = _blocks.emplace_back(rowCount, nullptr);
    _rowCount += rowCount;

    return block;
}

nb::object PyObjectColumn::toArray() {
    const nb::object numpy = nb::module_::import_("numpy");
    const nb::object array = numpy.attr("empty")(_rowCount, nb::arg("dtype") = "object");

    const nb::object flags = array.attr("flags");
    const bool contiguous = nb::cast<bool>(flags.attr("c_contiguous"));
    const size_t elementSize = nb::cast<size_t>(array.attr("itemsize"));
    if (!contiguous || elementSize != sizeof(PyObject*)) {
        throw TuringException("numpy object array is not a contiguous array of object pointers");
    }

    const nb::object arrayInterface = array.attr("__array_interface__");
    const uintptr_t address = nb::cast<uintptr_t>(arrayInterface["data"][0]);
    PyObject** slots = reinterpret_cast<PyObject**>(address);

    size_t slot = 0;
    for (const std::vector<PyObject*>& block : _blocks) {
        for (PyObject* object : block) {
            Py_XDECREF(slots[slot]);
            slots[slot] = object;
            slot++;
        }
    }

    _blocks.clear();
    _rowCount = 0;

    return array;
}
