#include "PySink.h"

using namespace pybindings;

PySink::PySink(PyValueStore* values)
    : _values(values)
{
}

PySink::~PySink() {
}
